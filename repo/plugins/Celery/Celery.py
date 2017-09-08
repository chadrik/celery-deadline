from System.IO import *
from System.Text.RegularExpressions import *
from Deadline.Plugins import *

# --
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


def GetCeleryGroupId(job):
    groupIdStr = job.GetJobExtraInfoKeyValueWithDefault('celery_id', job.JobId)
    return BsonObjectId(ObjectId.Parse(groupIdStr))


def GetCeleryTasks(job, frames):
    """
    Get raw celery task messages for the current deadline task.
    """
    collection = GetTaskCollection()
    groupId = GetCeleryGroupId(job)
    query = Query.EQ('_id', groupId)
    allFrames = list(job.JobFramesList)
    currentFrame = frames[0]
    index = allFrames.index(currentFrame)
    packetSize = len(frames)
    cursor = collection.Find(query).SetFields(Fields.Slice('tasks', index, packetSize)).SetLimit(1)
    doc = list(cursor)[0]
    results = []
    for task in doc.GetValue('tasks'):
        results.append(task.ToString())
    return results


def GetCeleryArguments(plugin, tasks, mode='execute'):
    plugin.SetProcessEnvironmentVariable("CELERY_DEADLINE_NUM_MESSAGES", str(len(tasks)))
    apps = []
    for i, task_message in enumerate(tasks):
        task = json.loads(task_message)
        if mode == 'delete':
            # patch the task message so that it fails
            body = json.loads(task['body'])
            body[0] = []
            body[1] = {}
            task['body'] = json.dumps(body)
            task['headers']['task'] = 'celery_deadline._on_job_deleted'
            task_message = json.dumps(task)
        plugin.SetProcessEnvironmentVariable("CELERY_DEADLINE_MESSAGE%d" % i,
                                             base64.b64encode(task_message))
        # plugin.SetEnvironmentVariable("CELERY_DEADLINE_MESSAGE", base64.b64encode(task))
        apps.append(task['headers']['task'].rsplit('.', 1)[0])

    assert len(set(apps)) == 1, "Tasks span more than one celery app"
    app = apps[0]
    return '-A %s worker -l debug -P solo --without-gossip --without-mingle --without-heartbeat' % app


def ExecuteTasks(plugin, tasks, mode='execute'):
    args = GetCeleryArguments(plugin, tasks, mode)
    plugin.SetProcessEnvironmentVariable("CELERY_DEADLINE_MODE", mode)
    plugin.LogInfo("Running celery callback")
    plugin.RunProcess('celery', args, '', -1)
    # not available on DeadlineEventListener:
    # plugin.StartMonitoredProgram('celery', 'celery', args, '')
# --


def GetDeadlinePlugin():
    return CeleryPlugin()


def CleanupDeadlinePlugin(deadlinePlugin):
    deadlinePlugin.Cleanup()


class CeleryPlugin(DeadlinePlugin):
    def __init__(self):
        self.InitializeProcessCallback += self.InitializeProcess
        # self.RenderTasksCallback += self.RenderTasks
        self.RenderExecutableCallback += self.RenderExecutable
        self.RenderArgumentCallback += self.RenderArgument
        # self.PreRenderTasksCallback += self.PreRenderTasks

    def Cleanup(self):
        # for stdoutHandler in self.StdoutHandlers:
        #     del stdoutHandler.HandleCallback

        del self.InitializeProcessCallback
        # del self.RenderTasksCallback
        del self.RenderExecutableCallback
        del self.RenderArgumentCallback
        # del self.PreRenderTasksCallback
    
    def InitializeProcess(self):
        self.SingleFramesOnly = True
        self.UseProcessTree = True
        self.StdoutHandling = False
        self.PluginType = PluginType.Simple

    def RenderExecutable(self):
        return 'celery'

    def RenderArgument(self):
        frames = list(self.GetCurrentTask().TaskFrameList)
        tasks = GetCeleryTasks(self.GetJob(), frames)
        return GetCeleryArguments(self, tasks)
