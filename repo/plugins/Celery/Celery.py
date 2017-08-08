import json
from System.IO import *
from System.Text.RegularExpressions import *
from Deadline.Plugins import *

from Deadline.Scripting import ClientUtils, RepositoryUtils
from MongoDB.Driver import MongoClient
from MongoDB.Driver.Builders import Query, Fields
from MongoDB.Bson import ObjectId, BsonExtensionMethods, BsonString, BsonObjectId


def GetDeadlinePlugin():
    return CeleryPlugin()


def CleanupDeadlinePlugin(deadlinePlugin):
    deadlinePlugin.Cleanup()


def GetTaskCollection():
    connStr = RepositoryUtils.GetDatabaseConnectionString()
    urls = connStr.strip('()').split(',')
    url = urls[0]
    client = MongoClient('mongodb://' + url)
    db = client.GetServer().GetDatabase('celery_deadline')
    return db.GetCollection('job_tasks')


class CeleryPlugin(DeadlinePlugin):
    def __init__(self):
        self.InitializeProcessCallback += self.InitializeProcess
        # self.RenderTasksCallback += self.RenderTasks
        self.RenderExecutableCallback += self.RenderExecutable
        self.RenderArgumentCallback += self.RenderArgument
        # self.PreRenderTasksCallback += self.PreRenderTasks

    def Cleanup(self):
        for stdoutHandler in self.StdoutHandlers:
            del stdoutHandler.HandleCallback

        del self.InitializeProcessCallback
        # del self.RenderTasksCallback
        del self.RenderExecutableCallback
        del self.RenderArgumentCallback
        del self.PreRenderTasksCallback
    
    def InitializeProcess(self):
        self.SingleFramesOnly = True
        self.UseProcessTree = True
        self.StdoutHandling = False
        self.PluginType = PluginType.Simple
        self.SetEnvironmentVariable("DEADLINE_JOB_ID", str(self.GetJob().JobId))

    def GetCeleryTasks(self):
        collection = GetTaskCollection()
        id = BsonObjectId(ObjectId.Parse(self.GetJob().JobId))
        query = Query.EQ('_id', id)
        # FIXME: get current frame
        currentFrame = 0
        # FIXME: support packet size > 1?
        packetSize = 1
        cursor = collection.Find(query).SetFields(Fields.Slice('tasks', currentFrame, packetSize)).SetLimit(1)
        doc = list(cursor)[0]
        results = []
        for task in doc.GetValue('tasks'):
            results.append(task.ToString())
        return results

    def RenderExecutable(self):
        return 'celery'

    def RenderArgument(self):
        tasks = self.GetCeleryTasks()
        # FIXME: support packet size > 1?
        task = tasks[0]
        app = json.loads(task)['headers']['task'].rsplit('.', 1)[0]
        arguments = '-A %s worker -l debug -P solo --without-gossip --without-mingle --without-heartbeat' % app
        return arguments
