from __future__ import print_function, absolute_import
from celery import Celery
import celery_deadline

app = Celery('testapp')
celery_deadline.configure(app)

_sum = sum

@app.task
def add(x, y):
    print("ADDDING!!!! %d + %d" % (x, y))
    return x + y


@app.task
def sum(values):
    print("SUMMING!!!! %r" % values)
    return _sum(values)


@app.task
def fail():
    print("FAILING")
    raise TypeError()


def test():
    # print add.delay(2, 2)

    from celery import group, chain

    job_info = {
        'Name': '{task_name}{task_args}',
        'BatchName': 'celery-{root_id}'
    }

    job = group([add.s(2, 2), add.s(4, 4)])
    result = job.apply_async(job_info=job_info)
    print("waiting for results:")
    for x in result.iterate(propagate=False):
        print("result is:" % x)


def test_fail():
    from celery import group
    job = group([add.s(2, 2), fail.s(), add.s(4, 4)])
    # job_info or plugin_info must be passed to trigger submission to deadline
    result = job.apply_async(job_info={})
    print("waiting for results:")
    # failure aborts iteration when propagate=True
    for x in result.iterate():
        print("result is:" % x)


def test_plugin():
    from celery_deadline import job
    result = job('Python', '1-5,40',
                 plugin_info=dict(
                     ScriptFile='/Users/chad/python/untitled.py',
                     Version='2.7')).apply_async()

    print("waiting for results:")
    for x in result.iterate(propagate=False):
        print("result is:" % x)


def test_mixed():
    from celery import chain
    from celery_deadline import job

    task = chain(
        job('Python', '1-2',
            plugin_info=dict(
                ScriptFile='/Users/chad/python/untitled.py',
                Version='2.7')),
        sum.s()  # <-- will go to a celery worker
    )
    result = task.apply_async()
    print("waiting for results:")
    print(result.get())


if __name__ == '__main__':
    test_mixed()
