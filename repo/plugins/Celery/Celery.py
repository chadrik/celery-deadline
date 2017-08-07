import os
import re

from System.IO import *
from System.Text.RegularExpressions import *

from Deadline.Scripting import *
from Deadline.Plugins import *


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

    def RenderExecutable(self):
        return 'celery'

    def RenderArgument(self):
        app = 'testapp'
        arguments = '-A %s worker -l debug -P solo --without-gossip --without-mingle --without-heartbeat' % app
        return arguments
