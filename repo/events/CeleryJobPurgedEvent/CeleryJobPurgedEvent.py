from Deadline.Events import DeadlineEventListener
from Deadline.Scripting import ClientUtils, RepositoryUtils
from MongoDB.Driver import MongoClient
from MongoDB.Driver.Builders import Query
from MongoDB.Bson import ObjectId, BsonObjectId


def GetDeadlineEventListener():
    return CeleryJobPurgedEvent()


def CleanupDeadlineEventListener(deadlinePlugin):
    deadlinePlugin.Cleanup()


def GetTaskCollection():
    connStr = RepositoryUtils.GetDatabaseConnectionString()
    urls = connStr.strip('()').split(',')
    url = urls[0]
    client = MongoClient('mongodb://' + url)
    db = client.GetServer().GetDatabase('celery_deadline')
    return db.GetCollection('job_tasks')


class CeleryJobPurgedEvent(DeadlineEventListener):
    def __init__(self):
        self.OnJobPurgedCallback += self.OnJobPurged

    def Cleanup(self):
        del self.OnJobPurgedCallback

    def OnJobPurged(self, job):
        collection = GetTaskCollection()
        id = BsonObjectId(ObjectId.Parse(job.JobId))
        query = Query.EQ('_id', id)
        collection.deleteOne(query)
