import json
import base64

from System.IO import *
from System.Text.RegularExpressions import *
from Deadline.Plugins import *

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
        return GetCeleryArguments(self)
