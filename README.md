
# Celery Deadline

Use Deadline as a celery worker, and intermix Deadline and celery tasks.

[Celery](http://www.celeryproject.org/) is insanely popular in the python programming world, with over 7,500 stars on github and 2,500 forks it’s one of the more popular projects on all of github.  Celery’s strengths lie in its speed -- each task has an incredibly low startup overhead, and many tasks can be run in parallel -- as well as its ability to pass results between tasks.  It is nowhere near as feature-rich as Deadline, nor is it well-suited to the long-running tasks common in VFX and animation, but harnessing the two task queues together for different types of tasks, and allowing results to pass through networks of tasks can be incredibly powerful.

Here's a brief feature comparison:

Feature | Deadline | Celery
--- | --- | ---
**Terminology** | slave, job, task, plugin | worker, group, task, app
**Worker environment** | dynamic per job/task | immutable per worker 
**Task startup time** | non-trivial | instant
**Monitoring** | sophisticated native app | basic web app
**Scheduling** | groups, priorities, criteria, limits | fixed queues
**Task dependencies** | yes | yes
**Tasks can return results** | no | yes

## Why use Celery with Deadline?

### Return results from Deadline tasks

After configuring one of celery's backends, results returned from tasks are transmitted back to the client that submitted the task, and/or to any additional celery tasks chained it.
This makes it a great alternative to the deprecated Deadline Python plugin type.

### Create networks of tasks whose results feed into each other

Celery has the ability to create graph-like relationships between tasks using its [canvas
primitives](http://docs.celeryproject.org/en/latest/userguide/canvas.html#the-primitives).
Unlike Deadline's dependency system, passing results through a network of celery tasks is a first-class concept.


## How to use it

There are two use cases:  
- submit celery tasks to Deadline
- wrap Deadline tasks in celery tasks (to monitor results or network with other celery tasks)
  
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
result = add.delay(2, 2, job_info={})
# wait for the result
print(result.get())
```

### Submit Deadline jobs using celery

You can also submit and wait for Deadline jobs using celery (such as the stock MayaCmd, Arnold, and Nuke plugins).
To do so, use `celery_deadline.job()` to create a group of celery tasks that proxy
Deadline tasks and wait for their results:

```python
from celery_deadline import job
result = job('Python', '1-5,40',
             ScriptFile='/Users/chad/python/untitled.py',
             Version='2.7').apply_async()

for x in result.iterate(propagate=False):
    print("result is: %s" % x)
```

By default, the value returned by each task is the task's frame (eventually will default to list of output paths configured for the job, or custom values).


All the usual celery [canvas primitives](http://docs.celeryproject.org/en/latest/userguide/canvas.html)
are supported, so you can group and chain tasks together, mixing celery tasks executing on Deadline with tasks executed on celery workers.

## Setup

- Copy the contents of the "repo" directory of this project into the "custom" sub-directory of the Deadline repository directory. 
- Start the DeadlineWebService
- Start one or more slaves, and make sure their environment is setup to be able to import the `celery_deadline` module
- Start the Monitor
  - Goto `Tools -> Super User Mode`
  - Goto `Tools -> Configure Repository Options...`
    - Under `House Cleaning`, check `Asynchronous Job Events`
- Start your celery results backend (i.e. redis or rabbitmq)
- Start a celery worker for your app:
  ```
  celery -A testapp worker -n 'w1@%h'
  ```
