from __future__ import absolute_import, print_function

import json
import base64

from Deadline.Scripting import ClientUtils, RepositoryUtils
from MongoDB.Driver import MongoClient
from MongoDB.Driver.Builders import Query, Fields
from MongoDB.Bson import ObjectId, BsonExtensionMethods, BsonString, BsonObjectId


def GetTaskCollection():
    connStr = RepositoryUtils.GetDatabaseConnectionString()
    urls = connStr.strip('()').split(',')
    url = urls[0]
    client = MongoClient('mongodb://' + url)
    db = client.GetServer().GetDatabase('celery_deadline')
    return db.GetCollection('job_tasks')


def GetCeleryTasks(plugin):
    job = plugin.GetJob()
    task = plugin.GetCurrentTask()
    collection = GetTaskCollection()
    groupIdStr = job.GetJobExtraInfoKeyValueWithDefault('celery_id', job.JobId)
    groupId = BsonObjectId(ObjectId.Parse(groupIdStr))
    query = Query.EQ('_id', groupId)
    allFrames = list(job.JobFramesList)
    frames = list(task.TaskFrameList)
    currentFrame = frames[0]
    index = allFrames.index(currentFrame)
    packetSize = len(frames)
    cursor = collection.Find(query).SetFields(Fields.Slice('tasks', index, packetSize)).SetLimit(1)
    doc = list(cursor)[0]
    results = []
    for task in doc.GetValue('tasks'):
        results.append(task.ToString())
    return results


def GetCeleryArguments(plugin):
    tasks = GetCeleryTasks(plugin)
    # FIXME: support packet size > 1?
    task = tasks[0]
    plugin.SetProcessEnvironmentVariable("CELERY_DEADLINE_MESSAGE", base64.b64encode(task))
    # plugin.SetEnvironmentVariable("CELERY_DEADLINE_MESSAGE", base64.b64encode(task))
    app = json.loads(task)['headers']['task'].rsplit('.', 1)[0]
    return '-A %s worker -l debug -P solo --without-gossip --without-mingle --without-heartbeat' % app


def PostRenderTasks(deadlinePlugin):
    def callback():
        args = GetCeleryArguments(deadlinePlugin)
        deadlinePlugin.LogInfo("Running celery callback")
        # deadlinePlugin.RunProcess('celery', args, '', -1)
        deadlinePlugin.StartMonitoredProgram('celery', 'celery', args, '')

    return callback


def __main__(deadlinePlugin):
    job = deadlinePlugin.GetJob()

    deadlinePlugin.LogInfo("args INFO %s" % job.GetJobExtraInfoKeyValue('celery_id'))
    deadlinePlugin.LogInfo("JobName: %s" % job.JobName)
    deadlinePlugin.LogInfo("JobId: %s" % job.JobId)

    deadlinePlugin.PostRenderTasksCallback += PostRenderTasks(deadlinePlugin)
