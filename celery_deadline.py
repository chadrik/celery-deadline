import sys
import os
import os.path
import getpass
import json

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
# {group}    {task}-{id}   batch name by group id if present, then one task per job
# {group}    {task}        batch name by group id if present, then tasks of same type in one job
# custom     {task}        custom batch name
# {app}      {task}        
#            {group}       tasks in the same group in one job

# better names: group_id, task_id, task_name, task_args, task_kwargs, app_name

# ExtraInfoKeyValue0=TestID=344


def enable_deadline_support(app, mongodb):
    app.amqp_cls = __name__ + ':DeadlineAMQP'


def get_collection(client):
    return client.celery_deadline.job_tasks


class DummyChannel(base.Channel):
    def __init__(self, *args, **kwargs):
        pass


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
    import pprint
    pprint.pprint(data)
    resp = requests.post(url, auth=auth, data=json.dumps(data))

    if not resp.ok:
        print ('Request failed with status {0:d}: '
               '{1} {2}').format(resp.status_code, resp.reason, resp.text)
        raise PulseRequestError(resp.status_code, resp.text)

    return resp.json()['_id']


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
        # FIXME:
        # detect if deadline was requested based on properties. 
        # maybe this doubles as way of passing pulse URL?
        deadline_requested = True
        if deadline_requested and self.deadline_pulse_url and self.deadline_mongo_url:
            channel = DummyChannel()
            headers = {} if headers is None else headers
            compression = self.compression if compression is None else compression

            if expiration is not None:
                properties['expiration'] = str(int(expiration * 1000))

            body, content_type, content_encoding = self._prepare(
                body, serializer, content_type, content_encoding,
                compression, headers)

            message = channel.prepare_message(
                body, priority, content_type,
                content_encoding, headers, properties,
            )
            job_info = properties.get('job_info', {}).copy()
            job_id = self._submit_deadline_job(job_info, headers)
            print "JOBID", job_id, repr(body)
            print message
            tasks_col = get_collection(self.mongo_client)

            tasks_col.update_one({"_id": ObjectId(job_id)},
                                 {"$push": {"tasks": json.dumps(message)}},
                                 upsert=True)
        else:
            return super(DeadlineProducer, self).publish(
                body, routing_key, delivery_mode, mandatory, immediate, priority, 
                content_type, content_encoding, serializer, headers, compression,
                exchange, retry, retry_policy, declare, **properties)

    def _submit_deadline_job(self, job_info, headers):
        group_id = headers['group']
        # setup JobInfo
        job_info['Plugin'] = 'Celery'
        job_info['Frames'] = '1'
        if group_id:
            job_info.setdefault('BatchName', 'celery-{}'.format(group_id))
        job_info.setdefault('Name', '{task}-{id}'.format(**headers))
        return submit_job(self.deadline_pulse_url, job_info, {})


class DeadlineConsumer(kombu.Consumer):
    """
    Consumer that reads a single task then shuts down the worker
    """
    def __init__(self, job_id, channel, *args, **kwargs):
        self.job_id = job_id
        self.deadline_mongo_url = kwargs.pop('deadline_mongo_url')
        super(DeadlineConsumer, self).__init__(channel, *args, **kwargs)

    @cached_property
    def mongo_client(self):
        return MongoClient(self.deadline_mongo_url)

    def consume(self, no_ack=None):
        import kombu.utils
        tasks_col = get_collection(self.mongo_client)
        # FIXME: get this elsewhere
        curr_task_num = 0
        # note: $slice is [skip, limit/count]
        doc = tasks_col.find_one({'_id': ObjectId(self.job_id)},
                                 {'tasks': {'$slice': [curr_task_num, 1]}})
        raw_message = json.loads(doc['tasks'][0])
        print "raw_message", raw_message
        raw_message['properties']['delivery_tag'] = kombu.utils.uuid()
        # override the channel so that we get a simple, consistent decoding scheme. otherwise,
        # the behavior is dependent on the channel type
        self.channel = DummyChannel()
        # message = Message(payload=raw_message, channel=channel)
        # print message.decode()
        # print message.headers
        # print self.on_message
        # print getattr(self.channel, 'message_to_python', None)
        # run the task:
        print "running"
        self._receive_callback(raw_message)
        print "done receive"
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
    @cached_property
    def mongo_url(self):
        conf = self.app.conf
        mongo_url = conf.get('deadline_mongo_url')
        if mongo_url:
            return mongo_url
        if conf.result_backend and conf.result_backend.startswith('mongodb://'):
            return conf.result_backend
        return

    def Consumer(self, *args, **kwargs):
        job_id = os.environ.get('DEADLINE_JOB_ID')
        if job_id:
            kwargs['deadline_mongo_url'] = self.mongo_url
            return DeadlineConsumer(job_id, *args, **kwargs)
        else:
            return kombu.Consumer(*args, **kwargs)

    def Producer(self, *args, **kwargs):
        kwargs['deadline_pulse_url'] = self.app.conf.get('deadline_pulse_url')
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


