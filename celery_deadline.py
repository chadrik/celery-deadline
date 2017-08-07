import sys
import os
import os.path
import json
import tempfile
import getpass

from celery.app.amqp import AMQP
from celery import bootsteps
import kombu
import kombu.pools
import requests

TASK_FILE = 'celery-task.json'

# deadline organizational strategies:
# BatchName  Job Name
# {group}    {task}-{id}   batch name by group id if present, then one task per job
# {group}    {task}        batch name by group id if present, then tasks of same type in one job
# custom     {task}        custom batch name
# {app}      {task}        
#            {group}       tasks in the same group in one job

# better names: group_id, task_id, task_name, task_args, task_kwargs, app_name

# ExtraInfoKeyValue0=TestID=344

task_exchange = kombu.Exchange('tasks', type='topic')

def enable_deadline_support(app, mongodb):
    app.amqp_cls = __name__ + ':DeadlineAMQP'


def get_queue(job_id):
    return kombu.Queue('deadline-%s' % job_id, exchange=task_exchange,
                       routing_key='deadline.%s.*' % job_id)

def submit_job(pulse_url, job_info, plugin_info, aux_files=None, auth=None):
    import requests
    import urllib

    print pulse_url
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
    # FIXME: this should be configurable at the app level, and/or via apply_async
    pulse_url = 'http://MacBook-Pro-4.local:8082'

    def __init__(self, channel, *args, **kwargs):
        super(DeadlineProducer, self).__init__(channel, *args, **kwargs)
        self.topic_exchange = None

    def publish(self, body, routing_key=None, delivery_mode=None,
                mandatory=False, immediate=False, priority=0,
                content_type=None, content_encoding=None, serializer=None,
                headers=None, compression=None, exchange=None, retry=False,
                retry_policy=None, declare=[], **properties):
        # FIXME:
        # detect if deadline was requested based on properties. 
        # maybe this doubles as way of passing pulse URL?
        deadline_requested = True
        if deadline_requested:
            # probably not compatible with deadline?
            retry = False
            # if self.topic_exchange is None:
            #     self.topic_exchange = task_exchange(self.channel)
            #     self.topic_exchange.declare()
            self.maybe_declare(task_exchange)
            job_info = properties.pop('job_info', {}).copy()
            job_id = self._submit_deadline_job(job_info, headers)
            print "JOBID", job_id, body
            routing_key = 'deadline.%s.0' % job_id
            self.maybe_declare(get_queue(job_id))
            exchange = task_exchange
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
        return submit_job(self.pulse_url, job_info, {})


class DeadlineConsumer(kombu.Consumer):
    """
    Consumer that reads a single task then shuts down the worker
    """

    # def consume(self, no_ack=None):
    #     import kombu.utils
    #     from kombu.message import Message

    #     filepath = os.path.join('.', TASK_FILE)
    #     print "consuming message", filepath
    #     print os.getcwd()
    #     with open(filepath, 'r') as f:
    #         raw_message = json.loads(f.read())

    #     raw_message['properties']['delivery_tag'] = kombu.utils.uuid()
    #     # run the task:
    #     self._receive_callback(raw_message)
    #     print "done receive"
    #     sys.exit(0)

    def _receive_callback(self, message):
        print "MESSAGE", message
        super(DeadlineConsumer, self)._receive_callback(message)
        print "done receive"
        sys.exit(0)

    def consume(self, no_ack=None):
        import kombu.utils
        from kombu.message import Message

        job_id = os.environ['DEADLINE_JOB_ID']
        print "JOBID", job_id
        queue = get_queue(job_id)
        self._basic_consume(queue.bind(self.channel), no_ack=no_ack, nowait=True)


class DeadlineAMQP(AMQP):
    def Consumer(self, *args, **kwargs):
        # FIXME: check for deadline env var
        return DeadlineConsumer(*args, **kwargs)

    Producer = DeadlineProducer

    @property
    def producer_pool(self):
        if self._producer_pool is None:
            self._producer_pool = kombu.pools.producers[
                self.app.connection_for_write()]
            self._producer_pool.limit = self.app.pool.limit
            # TODO: submit this patch to celery:
            self._producer_pool.Producer = self.Producer
        return self._producer_pool


