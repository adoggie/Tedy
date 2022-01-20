#coding:utf-8

import sys,os,os.path,traceback,time,datetime
import json
import toml
import pymongo
import signal
import sys
import fire
import cmd

from elabs.fundamental.utils.useful import singleton,input_params
from elabs.app.core.controller import Controller
from elabs.app.core.behavior import Behavior
from elabs.app.core.logger import initLogger
from elabs.app.core import logger


from elabs.tedy.logger import INFO,DEBUG,WARN,ERROR,init as init_log
from elabs.tedy.message_client import MessageClient
from elabs.tedy.command import *

PWD = os.path.dirname(os.path.abspath(__file__))



class TedyServer(Behavior,cmd.Cmd):
  prompt = 'TedyNode > '
  intro = 'welcome to elabs..'
  def __init__(self):
    Behavior.__init__(self)
    cmd.Cmd.__init__(self )

    self.running = False
    # self.configs = {} #  with cone-project.toml
    self.nosql = None
    self.msgclient = None

  def init(self,**kvs):
    initLogger('DEBUG', os.path.join(PWD), 'tedynode.log',stdout=True)

    Behavior.init(self, **kvs)
    self.nosql = pymongo.MongoClient(**self.cfgs['mongodb'])
    MessageClient().init(**self.cfgs).open().addUser(self)
    return self

  def open(self):
    return self

  def close(self):
    self.running = False

  def do_exit(self,*args):
    Controller().close()
    print('bye bye!')
    return True

  def do_show(self,line):
    args = input_params(line,['pos'])
    if args:
     pass

  def onMessage(self,message:CommandBase):

    if type(message) == ProjectRun:
      self.project_run(message)
    elif type(message) ==  ProjectDeploy:
      self.project_deploy(message.prj_id)
    elif type(message) == DataDeploy:
      self.data_deploy(message.prj_id)
    elif type(message) == ProjectStop:
      self.project_stop(message.prj_id)

  def project_stop(self,prj_id):
    logger.info(f"on project_stop : {prj_id}")

    db = self.nosql['Task']
    coll = db[prj_id]
    rs = coll.find({'node': self.cfgs['name']})
    for r in rs:
      task_id = str(r['_id'])

      try:
        cmd = "ps -eaf | grep tedyworker | grep -v grep | awk '{print $2}'"
        pid = os.popen(cmd).read()
        cmd = f"pkill -f -9 {pid}"
        os.system(cmd)
      except:
        pass
      cmd = f"pkill -f -9 {task_id}"
      os.system(cmd)

  def data_deploy(self,prj_id):
    """项目数据部署或同步请求"""
    python = self.cfgs['python']
    cmd = f"flock -xn /tmp/data-deploy-{prj_id}.lock -c 'cd {PWD}/..; python -m TedyNode.tedyworker data_deploy {prj_id}' & "
    DEBUG(cmd)
    os.system(cmd)


  def project_deploy(self,prj_id):
    python = self.cfgs['python']
    cmd = f"flock -xn /tmp/project-deploy-{prj_id}.lock -c 'cd {PWD}/..; {python} -m TedyNode.tedyworker project_deploy {prj_id}' &  "
    DEBUG(cmd)
    os.system(cmd)
    time.sleep(1)

  def project_run(self,m:ProjectRun):
    logger.info(f"on project_run : {m.prj_id}")
    db = self.nosql['Task']
    if m.prj_id not in db.list_collection_names():
      WARN('project not existed!', m.prj_id)
      return

    coll = db[m.prj_id]
    rs = coll.find({'node':self.cfgs['name'] })
    for r in rs:
      self.task_run( m.prj_id, str(r['_id']))

  def task_run(self,prj_id,task_id):
    """调度批次任务加载worker进程"""
    python = self.cfgs['python']
    cmd = f"flock -xn /tmp/tedy_task_{task_id}.lock -c 'cd {PWD}; {python} tedyworker.py run {prj_id} {task_id}' & "
    DEBUG(cmd)
    logger.info(f"worker launched : {cmd}")
    os.system(cmd)

def signal_handler(signal,frame):
  Controller().close()
  print('bye bye!')
  sys.exit(0)

FN = os.path.join(PWD, 'tedy-settings.toml')

def run(id = '',fn='',noprompt=False):
  global FN
  if fn:
    FN = fn
  if FN[0]!='/':
    FN = os.path.join(PWD,FN)

  params = toml.load(open(FN))
  # params = json.loads(open(FN).read())
  if id:
    params['service_id'] = id

  Controller().init(**params).addBehavior("market",TedyServer()).open()
  if noprompt:
    signal.signal(signal.SIGINT, signal_handler)
    print("")
    print("~~ Press Ctrl+C to kill .. ~~")
    while True:
      time.sleep(1)
  else:
    TedyServer().cmdloop()

if __name__ == '__main__':
  fire.Fire()
