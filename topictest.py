
def topic_test():
    from kombu import Exchange, Connection
    connection = Connection('redis://localhost:6379/0')
    # connection = Connection("amqp://guest:guest@localhost:5672//")
    channel = connection.channel()
    news_exchange = Exchange('news', type='topic')
    bound_exchange = news_exchange(channel)
    bound_exchange.declare()

    # Publish raw string message using low-level exchange API
    bound_exchange.publish(
        'Cure for cancer found!',
        routing_key='news.science',
    )
    bound_exchange.delete()


from kombu import Exchange, Queue, Connection, binding

task_exchange = Exchange("thing", type="topic")
# connection = Connection('redis://localhost:6379/0')

# when first introducing a new queue name, messages don't seem to be produced correctly until the consumer half is run.
# queue = Queue('qq', exchange=task_exchange)
queue = Queue('qq', exchange=task_exchange, routing_key='user.foo.#')
# queue_notify = Queue('whatever', exchange=task_exchange, bindings=[binding(task_exchange, routing_key='user.foo.#')])


def topic_produce():
    from kombu import Exchange, Queue, Connection
    from kombu.common import maybe_declare
    from kombu.pools import producers

    with Connection("amqp://guest:guest@localhost:5672//") as connection:
        producer = connection.Producer(serializer='json')
        maybe_declare(task_exchange, producer.channel)
        payload = "FOO1"
        producer.publish(payload, exchange=task_exchange, routing_key='user.foo.1', declare=[queue])
        payload = "FOO2"
        producer.publish(payload, exchange=task_exchange, routing_key='user.foo.2', declare=[queue])
        payload = "BAR"
        producer.publish(payload, exchange=task_exchange, routing_key='user.bar', declare=[queue])
        print "published"

def topic_consume():
    from kombu.mixins import ConsumerMixin
    from kombu.utils.debug import setup_logging
    connection = Connection("amqp://guest:guest@localhost:5672//")

    class C(ConsumerMixin):
        def __init__(self, connection):
            self.connection = connection
            return
        
        def get_consumers(self, Consumer, channel):
            return [Consumer(queues=[queue], callbacks=[self.on_message])]
        
        def on_message(self, body, message):
            print ("save_db: RECEIVED MSG - body: %r" % (body,))
            print ("save_db: RECEIVED MSG - message: %r" % (message,))
            message.ack()
            return

    setup_logging(loglevel="DEBUG")
    print("run")
    try:
        C(connection).run()
    except KeyboardInterrupt:
        print("bye bye")

if __name__ == '__main__':
    topic_produce()
    topic_consume()
