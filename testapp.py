from celery import Celery

app = Celery('testapp')
# app.conf.broker_url = 'redis://localhost:6379/0'
app.conf.broker_url = "amqp://guest:guest@localhost:5672//"
# app.conf.result_backend = 'redis://localhost:6379/0'
app.conf.result_backend = 'mongodb://macbook-pro-4.local:27090'
app.amqp_cls = 'celery_deadline:DeadlineAMQP'
app.conf.deadline_pulse_url = 'http://MacBook-Pro-4.local:8082'
# app.conf.deadline_mongo_url = 'mongodb://macbook-pro-4.local:27090'

# from celery_deadline import enable_deadline_support
# enable_deadline_support(app, 'mongodb://macbook-pro-4.local')


@app.task
def add(x, y):
    print "ADDDING!!!!"
    return x + y


def test():
    # print add.delay(2, 2)

    from celery import group
    job = group([add.s(2, 2), add.s(4, 4)])

    result = job.apply_async(job_info={})
    print "waiting for results:"
    for x in result.iterate():
        print "result is:", x


if __name__ == '__main__':
    test()

