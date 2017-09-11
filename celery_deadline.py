from __future__ import print_function, absolute_import
import re
import sys
import os
import os.path
import getpass
import json
import base64
from collections import deque

from celery.app.amqp import AMQP
import kombu
import kombu.pools
import kombu.transport.virtual.base as base
from kombu.utils.objects import cached_property

import requests

from pymongo import MongoClient
from bson.objectid import ObjectId

# deadline organizational strategies:
# BatchName  Job Name
# {group_id}    {task_name}-{task_id}   batch name by group id if present, then one task per job
# {group_id}    {task_name}             batch name by group id if present, then tasks of same type in one job
# custom        {task_name}             custom batch name
# {app_name}    {task_name}
#               {group}                 tasks in the same group in one job


_frame_regex = None


class PulseRequestError(Exception):
    pass


class JobDeletedError(Exception):
    pass


def configure(app, config_object='celery_deadline_config'):
    """
    Configure a celery app to be executed on Deadline
    """
    app.amqp_cls = __name__ + ':DeadlineAMQP'
    if config_object is not None:
        app.config_from_object(config_object)


# general deadline utils --

def set_extra_info(job_info, key, value):
    i = 0
    while True:
        ikey = 'ExtraInfoKeyValue%d' % i
        if ikey not in job_info:
            job_info[ikey] = '%s=%s' % (key, value)
            return ikey


def _get_frame_regex():
    global _frame_regex
    if _frame_regex is None:
        stepSep = 'x'
        _frame_regex = re.compile('(-?\d+)(?:-(\d+)(?:%s(\d+))?)?' % stepSep)
    return _frame_regex


def parse_frames(frames):
    sequence = []
    for start, stop, step in _get_frame_regex().findall(frames):
        start = int(start)
        if not stop:
            sequence.append(start)
        else:
            stop = int(stop)
            if stop == start:
                sequence.append(start)
                continue

            if not step:
                step = 1
            else:
                step = int(step)

            # Head off attempts to create ridiculously large frame lists
            if (stop - start) / step > 1000000:
                raise SequenceParseOverflow(
                    'Attempted to parse range of '
                    '%d frames from range string %r'
                    % ((stop - start) / step, frames))
            sequence.extend(xrange(start, stop + 1, step))
    return sequence


def submit_job(pulse_url, job_info, plugin_info, aux_files=None, auth=None):
    url = pulse_url + '/api/jobs'

    if auth is None:
        auth = (getpass.getuser(), '')
    elif isinstance(auth, basestring):
        auth = (auth, '')

    data = {
        'JobInfo': job_info,
        'PluginInfo': plugin_info,
        'AuxFiles': aux_files or [],
        'IdOnly': True
    }
    print(data)
    resp = requests.post(url, auth=auth, data=json.dumps(data))

    if not resp.ok:
        print('Request failed with status {0:d}: '
              '{1} {2}').format(resp.status_code, resp.reason, resp.text)
        raise PulseRequestError(resp.status_code, resp.text)

    return resp.json()['_id']


# celery deadline internals --

def _mongo_collection(client):
    return client['celery_deadline']['job_tasks']


class Formatter(object):
    def __init__(self, headers, job_info):
        self.values = {
            'group_id': headers['group'] or '',
            'root_id': headers['root_id'],
            'task_id': headers['id'],
            'task_name': headers['task'],
            'task_args': headers['argsrepr'],
            'task_kwargs': headers['kwargsrepr'],
            'app_name': headers['task'].rsplit('.', 1)[0],
            'plugin': job_info['Plugin'],
        }

        # set defaults
        if self.values['group_id']:
            job_info.setdefault('BatchName', '{plugin}-{group_id}')
        job_info.setdefault('Name', '{plugin}-{task_name}-{task_id}')

    def expand_tokens(self, s):
        return s.format(**self.values)


# override the base virtual channel so that it doesn't use the connection
# object.  we use this for converting to and from Message objects.
class DummyChannel(base.Channel):
    def __init__(self, *args, **kwargs):
        pass


class DeadlineProducer(kombu.Producer):

    def __init__(self, channel, *args, **kwargs):
        self.deadline_pulse_url = kwargs.pop('deadline_pulse_url', None)
        self.deadline_mongo_url = kwargs.pop('deadline_mongo_url', None)
        super(DeadlineProducer, self).__init__(channel, *args, **kwargs)

    @cached_property
    def mongo_client(self):
        return MongoClient(self.deadline_mongo_url)

    def publish(self, body, routing_key=None, delivery_mode=None,
                mandatory=False, immediate=False, priority=0,
                content_type=None, content_encoding=None, serializer=None,
                headers=None, compression=None, exchange=None, retry=False,
                retry_policy=None, declare=None, expiration=None,
                **properties):
        # copied from Producer.publish:
        headers = {} if headers is None else headers
        compression = self.compression if compression is None else compression
        if expiration is not None:
            properties['expiration'] = str(int(expiration * 1000))

        job_info = properties.get('job_info')
        plugin_info = properties.get('plugin_info')

        is_task = 'task' in headers
        is_builtin = is_task and headers['task'].split('.')[0] == 'celery'
        deadline_requested = not is_builtin and \
                             (job_info is not None or plugin_info is not None)
        if deadline_requested and not (self.deadline_pulse_url and self.deadline_mongo_url):
            raise ValueError()
        if is_task and deadline_requested:
            channel = DummyChannel()
            job_info = {} if job_info is None else job_info.copy()
            plugin_info = {} if plugin_info is None else plugin_info.copy()
            group_id = properties.pop('deadline_group_id', None)

            # setup job_info:
            if headers['task'] == __name__ + '.plugin_task':
                # a special celery task that proxies a task for a Deadline job
                plugin, frames, frame, index = body[0]
                if index == 0:
                    # first task, setup to submit to Deadline
                    assert group_id is not None
                    job_info['Plugin'] = plugin
                    job_info['Frames'] = frames
                    # store the group_id, which is used to retrieve the
                    # celery tasks for the job.
                    set_extra_info(job_info, 'celery_id', group_id)
                else:
                    # don't submit to Deadline
                    job_info = None
            else:
                # a celery task to execute on Deadline
                job_info['Plugin'] = 'Celery'
                job_info['Frames'] = '1'

            body, content_type, content_encoding = self._prepare(
                body, serializer, content_type, content_encoding,
                compression, headers)

            message = channel.prepare_message(
                body, priority, content_type,
                content_encoding, headers, properties,
            )
            print("-" * 30)
            print(message)

            if job_info:
                # submit to Deadline!
                job_info['EventOptIns'] = 'CeleryEvents'
                job_id = self._submit_deadline_job(headers, job_info,
                                                   plugin_info)
                if group_id is None:
                    # when no group_id is provided, the job_id is used as a
                    # fallback.  group_id is explicitly provided for plugin_
                    # tasks because of the fact that only the first plugin_task
                    # in a job triggers a Deadline submission, and therefore
                    # only the first task has easy access to the job_id.
                    # providing an explicit group_id to all plugin_task's
                    # avoids the need to devise a system to share the job_id
                    # with subsequent tasks.
                    group_id = job_id
                print("JOBID %s" % job_id)
            print("Inserting data into mongo using %s" % group_id)
            tasks_col = _mongo_collection(self.mongo_client)

            tasks_col.update_one({"_id": ObjectId(group_id)},
                                 {"$push": {"tasks": json.dumps(message)}},
                                 upsert=True)
        else:
            return super(DeadlineProducer, self).publish(
                body, routing_key, delivery_mode, mandatory, immediate, priority,
                content_type, content_encoding, serializer, headers, compression,
                exchange, retry, retry_policy, declare, **properties)

    def _submit_deadline_job(self, headers, job_info, plugin_info):
        # expand values:
        formatter = Formatter(headers, job_info)
        job_info['Name'] = formatter.expand_tokens(job_info['Name'])
        batch_name = job_info.get('BatchName')
        if batch_name:
            job_info['BatchName'] = formatter.expand_tokens(batch_name)
        return submit_job(self.deadline_pulse_url, job_info, plugin_info)


class DeadlineConsumer(kombu.Consumer):
    """
    Consumer that reads a set of passed task messages then shuts down the
    worker.
    """
    def __init__(self, raw_messages, channel, *args, **kwargs):
        self.raw_messages = raw_messages
        super(DeadlineConsumer, self).__init__(channel, *args, **kwargs)

    def consume(self, no_ack=None):
        import kombu.utils
        # override the channel so that we get a simple, consistent decoding
        # scheme. otherwise, the behavior is dependent on the channel type
        self.channel = DummyChannel()
        for raw_message in self.raw_messages:
            print("raw_message")
            print(raw_message)
            # mark as received
            raw_message['properties']['delivery_tag'] = kombu.utils.uuid()
            print("running")
            self._receive_callback(raw_message)
            print("done receive")
        # shut down the worker
        sys.exit(0)

    # def _receive_callback(self, message):
    #     print "MESSAGE", message
    #     super(DeadlineConsumer, self)._receive_callback(message)
    #     print "done receive"
    #     sys.exit(0)

    # def consume(self, no_ack=None):
    #     import kombu.utils
    #     from kombu.message import Message
    #
    #     job_id = os.environ['DEADLINE_JOB_ID']
    #     print "JOBID", job_id
    #     queue = get_queue(job_id)
    #     self._basic_consume(queue.bind(self.channel), no_ack=no_ack, nowait=True)


class DeadlineAMQP(AMQP):
    """
    Patched AMQP class which is used to intercept published celery tasks and
    send them to Deadline (via Mongo), and within a Deadline task to consume
    intercepted tasks.
    """
    def set_messages_and_values(self, messages, values):
        assert len(messages) == len(values)
        self.app.deadline_messages = messages
        self.app.deadline_values = deque(values)

    def get_messages(self):
        messages = getattr(self.app, 'deadline_messages', None)
        if messages is not None:
            return messages
        num_messages = os.environ.get('CELERY_DEADLINE_NUM_MESSAGES')
        if num_messages:
            messages = []
            for i in range(int(num_messages)):
                message_str = os.environ['CELERY_DEADLINE_MESSAGE%d' % i]
                messages.append(json.loads(base64.b64decode(message_str)))
            return messages

    @cached_property
    def mongo_url(self):
        conf = self.app.conf
        mongo_url = conf.get('deadline_mongo_url')
        if mongo_url:
            return mongo_url
        if conf.result_backend and conf.result_backend.startswith('mongodb:'):
            return conf.result_backend
        return

    def Consumer(self, *args, **kwargs):
        messages = self.get_messages()
        if messages is not None:
            print("USING DEADLINE CONSUMER")
            return DeadlineConsumer(messages, *args, **kwargs)
        else:
            print("USING KOMBU CONSUMER")
            return kombu.Consumer(*args, **kwargs)

    def Producer(self, *args, **kwargs):
        kwargs['deadline_pulse_url'] = self.app.conf.get('deadline_pulse_url')
        print("deadline_pulse_url %s" % kwargs['deadline_pulse_url'])
        kwargs['deadline_mongo_url'] = self.mongo_url
        return DeadlineProducer(*args, **kwargs)

    @property
    def producer_pool(self):
        if self._producer_pool is None:
            self._producer_pool = kombu.pools.producers[
                self.app.connection_for_write()]
            self._producer_pool.limit = self.app.pool.limit
            # TODO: submit this patch to celery:
            self._producer_pool.Producer = self.Producer
        return self._producer_pool


# tasks ---

# FIXME: look into global tasks, which get added to all apps automatically
from celery import Celery, group
app = Celery('celery_deadline')
app.amqp_cls = 'celery_deadline:DeadlineAMQP'
app.config_from_object('celery_deadline_config')


# TODO: create a plugin_task that returns registered OutputFilenames as results.
# Examples:
# OutputDirectory0=\\fileserver\Project\Renders\OutputFolder\
# OutputFilename0=o_HDP_010_BG_v01.####.exr
# OutputDirectory1=\\fileserver\Project\Renders\OutputFolder\
# OutputFilename1=o_HDP_010_SPEC_v01####.dpx
# OutputDirectory2=\\fileserver\Project\Renders\OutputFolder\
# OutputFilename2=o_HDP_010_RAW_v01_####.png


@app.task(bind=True)
def plugin_task(self, plugin, frames, frame, index):
    """
    Place-holder task that represents a single task in Deadline plugin-based
    job.

    The task's purpose is to hold information used to submit a job to Deadline
    and to allow results to be retrieved from the celery results backend as
    their corresponding Deadline tasks complete.

    See `celery_deadline.job()`
    """
    # values = getattr(self.app, 'deadline_values', None)
    # if values is not None:
    #     return values.popleft()
    # mode = os.environ.get('CELERY_DEADLINE_MODE')
    # if mode == 'delete':
    #     raise JobDeletedError()
    return frame


@app.task(throws=(JobDeletedError,))
def _on_job_deleted():
    """
    task which is patched in on Deadline to fail incomplete
    tasks when a Deadline job is dumped.
    """
    raise JobDeletedError()


def job(plugin_name, frames, job_info=None, plugin_info=None):
    """
    Create a group of tasks for executing each of the frame packets for
    the given Deadline plugin.

    Parameters
    ----------
    plugin_name : str
    frames : str

    Returns
    -------
    celery.group
    """
    deadline_group_id = ObjectId()
    return group(
        [plugin_task.signature((plugin_name, frames, frame, i),
                               plugin_info=plugin_info,
                               job_info=job_info,
                               deadline_group_id=deadline_group_id)
         for i, frame in enumerate(parse_frames(frames))])

# --

# FIXME: unused:
def _get_deadline_task_id(parent_task_id, frame):
    # FIXME: which approach?
    # frame_id = uuid()
    frame_id = '%s-%06d' % (parent_task_id, frame)
    # frame_id = uuid5(parent_task_id, '%06' % frame)
    return frame_id


def process_task_messages(messages, values):
    from celery.bin.worker import worker
    app.amqp.set_messages_and_values(messages, values)
    args = '-A %s -l debug -P solo --without-gossip --without-mingle ' \
           '--without-heartbeat' % __name__
    w = worker(app)
    #     app=self.app, on_error=self.on_error,
    #     no_color=self.no_color, quiet=self.quiet,
    #     on_usage_error=partial(self.on_usage_error, command=command),
    # ).run_from_argv(self.prog_name, argv[1:], command=argv[0])
    w.run_from_argv('celery', args.split(), command='worker')
    # get celery task_id for current frame
    # frame_id = _get_deadline_task_id(task_id, frame)

