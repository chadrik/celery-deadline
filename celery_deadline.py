import sys
import os
import os.path
import json

from celery import Celery
from celery.app.amqp import AMQP
from celery import bootsteps
import kombu
import kombu.pools


class DeadlineProducer(kombu.Producer):
    def __init__(self, *args, **kwargs):
        print "DeadlineProducer instatiated"
        super(DeadlineProducer, self).__init__(*args, **kwargs)

    def publish(self, *args, **kwargs):
        # probably not compatible with deadline?
        kwargs['retry'] = False
        super(DeadlineProducer, self).publish(*args, **kwargs)

    def _publish(self, body, priority, content_type, content_encoding,
                 headers, properties, routing_key, mandatory,
                 immediate, exchange, declare):
        message = self.channel.prepare_message(
            body, priority, content_type,
            content_encoding, headers, properties,
        )
        import pprint
        filepath = headers['id'] + '.json'
        print "sending to deadline", filepath
        print message
        with open(filepath, 'w') as f:
            f.write(json.dumps(message))


class DeadlineConsumer(kombu.Consumer):
    """
    Consumer that reads a single task then shuts down the worker
    """
    def __init__(self, *args, **kwargs):
        print "DeadlineConsumer instatiated"
        super(DeadlineConsumer, self).__init__(*args, **kwargs)

    def consume(self, no_ack=None):
        import kombu.utils
        from kombu.message import Message

        task_id = os.environ['TASK_ID']

        filepath = task_id + '.json'
        print "consuming message", filepath
        with open(filepath, 'r') as f:
            raw_message = json.loads(f.read())

        raw_message['properties']['delivery_tag'] = kombu.utils.uuid()
        self._receive_callback(raw_message)
        print "done receive"
        sys.exit(0)


# FIXME: ideally celery would automatically take my Producer class into account with pools:
class Producers(kombu.pools.PoolGroup):

    def create(self, connection, limit):
        pool = kombu.pools.ProducerPool(kombu.pools.connections[connection], limit=limit)
        pool.Producer = DeadlineProducer
        return pool

producers = kombu.pools.register_group(Producers(limit=kombu.pools.use_global_limit))


class DeadlineAMQP(AMQP):
    Consumer = DeadlineConsumer
    Producer = DeadlineProducer

    # FIXME: ideally celery would automatically take my Producer class into account with pools:
    @property
    def producer_pool(self):
        if self._producer_pool is None:
            self._producer_pool = producers[
                self.app.connection_for_write()]
            self._producer_pool.limit = self.app.pool.limit
        return self._producer_pool


app = Celery('tasks', amqp='celery_deadline:DeadlineAMQP')
app.conf.broker_url = 'redis://localhost:6379/0'
app.conf.result_backend = 'redis://localhost:6379/0'

@app.task
def add(x, y):
    print "ADDDING!!!!"
    return x + y

# run with
# celery -A celery_deadline worker -l debug -P solo --without-gossip --without-mingle --without-heartbeat

def test():
    # print add.delay(2, 2)

    from celery import group
    job = group([add.s(2, 2), add.s(4, 4) ])

    result = job.apply_async()
    for x in result.iterate():
        print "result is:", x

