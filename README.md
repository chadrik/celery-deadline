
# Celery Deadline

Use Deadline as a celery worker, and combine Deadline and celery tasks.

## Why this a good thing

### Return results from Deadline tasks

After configuring one of celery's backends, results returned from tasks are transmitted back to the client that submitted the task, and/or to any additional celery tasks chained it.
This makes it a great alternative to the deprecated Deadline Python plugin type.

### Create networks of tasks whose results feed into each other

Celery has the ability to create graph-like relationships between tasks using its [canvas
primitives](http://docs.celeryproject.org/en/latest/userguide/canvas.html#the-primitives).
Unlike Deadline's dependency system, passing results through a network of celery tasks is a first-class concept.


## How to use it

There are two use cases:  
- submitting celery tasks to Deadline
- wrapping Deadline tasks in celery tasks
  
### Execute celery tasks using Deadline

A normal celery app can easily be configured to execute on Deadline:

```python
from celery import Celery
import celery_deadline

app = Celery('testapp')
celery_deadline.configure(app)

@app.task
def add(x, y):
    return x + y
```

Tasks are then submitted to Deadline using celery, and results are returned as they complete:

```python
from testapp import add
# submit to Deadline
result = add.delay(2, 2)
# wait for the result
print(result.get())
```

## Submit Deadline jobs using celery

You can also bring Deadline jobs into celery (such as the stock MayaCmd, Arnold, and Nuke plugins).
To do so, use `celery_deadline.job()` to create a group of celery tasks that proxy
Deadline tasks and wait for their results:


By default, the value returned by each task is the list of output paths configured for the job.
However, it is possible to return any result you wish.


All the usual celery [canvas primitives](http://docs.celeryproject.org/en/latest/userguide/canvas.html)
are supported, so you can group and chain tasks together, mixing normal celery tasks with those
backed by Deadline.