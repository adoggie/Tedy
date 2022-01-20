#coding:utf-8

import sys,os,os.path,traceback,time,datetime
import json
import toml
import fire
import pymongo
from bson.objectid import ObjectId
import zmq
import os


from elabs.tedy.model import Project
from elabs.fundamental.utils.importutils import import_function
from elabs.tedy.context import Context
from elabs.tedy.logger import INFO,DEBUG,WARN,ERROR,init as init_log
from elabs.tedy import logger
from elabs.tedy.command import *
# from elabs.tedy import model
from elabs.fundamental.utils.useful import make_dir
from elabs.utils.zmqex import init_keepalive
from elabs.app.core.logger import initLogger

PWD = os.path.dirname(os.path.abspath(__file__))
file = os.path.join(PWD,'tedy-settings.toml')
settings = toml.load(file)

initLogger('DEBUG', os.path.join(PWD), 'tedyworker.log',stdout=True)

def init_database(name=''):
  conn = pymongo.MongoClient(**settings['mongodb'])
  if not name:
    name = 'ConTedy'
  return conn[name]

# database = init_database()
# model.set_database(database)

init_log()
#---------------------------------------

def run(prj_id,task_id):
  """ 运行指定的task
  """
  zctx = zmq.Context()
  sock = zctx.socket(zmq.PUB)
  addr = settings['system_broker_addr_p']
  sock.connect(addr)
  time.sleep(0.1)
  os.environ['NUMEXPR_MAX_THREADS'] = str(settings['max_threads'])
  print('max_threads:',str(settings['max_threads']))

  db = init_database('Task')
  INFO(f"{prj_id} task:{task_id}")
  coll = db[prj_id]
  r = coll.find_one({'_id':ObjectId(task_id)})
  if not r:
    ERROR(f' task not found! {task_id}')
    return

  path = os.path.join(settings['repo_dir'],prj_id)
  sys.path.append(path)
  path = os.path.join(settings['repo_dir'], prj_id, 'cone-project.toml')
  configs = toml.load(open(path))

  imp_entry = configs['handlers'].get('main_entry')  # 加载运行触发入口

  if not imp_entry:
    return
  fx = import_function(imp_entry)
  #
  data = {'start':datetime.datetime.now(),'end':None}
  coll.update_one({'_id': ObjectId(task_id)},{'$set':data},upsert=True)

  ctx = Context()
  ctx.project = prj_id
  ctx.node = settings['name']
  ctx.data_dir = settings['data_dir']
  ctx.repo_dir = settings['repo_dir']
  ctx.node_dir = settings['node_dir']
  ctx.task_list = r['task_list']
  ctx.cfgs = configs
  ctx.ncores = settings['par_ncore']


  m = WorkerStart()
  m.prj_id = prj_id
  m.node = ctx.node
  m.task_id = task_id
  sock.send(m.marshall().encode())

  fx(ctx)
  data = { 'end': datetime.datetime.now()}
  coll.update_one({'_id': ObjectId(task_id) }, {'$set':data}, upsert=True)

  # mx 发送消息
  m = WorkerStop()
  m.prj_id = prj_id
  m.node = ctx.node
  m.task_id = task_id
  sock.send(m.marshall().encode())
    


def project_deploy(prj_id):
  """执行部署项目代码之后执行事件"""

  INFO('>> project_deploy:{}'.format(prj_id))

  path = os.path.join(settings['repo_dir'],prj_id)
  sys.path.append(path)

  path = os.path.join(settings['repo_dir'], prj_id, 'cone-project.toml')

  configs = toml.load(open(path))
  imp_entry = configs['handlers'].get('project_deploy')
  if not imp_entry:
    return

  INFO('import function:',imp_entry)
  fx = import_function(imp_entry)
  #
  path = os.path.join(settings['repo_dir'],prj_id) #当前node的data_dir
  if not os.path.exists(path):
    try:
      make_dir(path)
    except: pass

  ctx = Context()
  ctx.project = prj_id
  ctx.node = settings['name']
  ctx.data_dir = settings['data_dir']
  ctx.repo_dir = settings['repo_dir']
  ctx.node_dir = settings['node_dir']
  fx(ctx)


def data_deploy(prj_id):
  """项目数据更新或部署"""
  INFO('>> data_deploy:{}'.format(prj_id))

  path = os.path.join(settings['data_dir'], prj_id)
  if not os.path.exists(path):
    make_dir(path)

  path = os.path.join(settings['repo_dir'], prj_id)
  sys.path.append(path)
  path = os.path.join(settings['repo_dir'], prj_id, 'cone-project.toml')
  configs = toml.load(open(path))
  imp_entry = configs['handlers'].get('data_deploy')  # 加载运行触发入口
  if not imp_entry:
    return
  fx = import_function(imp_entry)
  #
  ctx = Context()
  ctx.project = prj_id
  ctx.node = settings['name']
  ctx.data_dir = settings['data_dir']
  ctx.repo_dir = settings['repo_dir']
  ctx.node_dir = settings['node_dir']

  fx(ctx)


def project_clean(prj_id):
  #清除项目数据
  pass


# def result_reducer(prj_id):
#   """  data reducer
#   """
#   INFO('result_reducer:{}'.format( prj_id))
#
#   prj = Project.get(prj_id = prj_id)
#   if not prj:
#     return
#
#   finished = False
#   path = os.path.join(settings['repo_dir'], prj_id)
#   sys.path.append(path)
#   path = os.path.join(settings['repo_dir'], prj_id, 'cone-project.toml')
#   prj.configs = toml.load(open(path))
#   imp_entry = prj.configs.get('task_result', {}).get('reducer')  # 加载运行触发入口
#   if not imp_entry:
#     return
#   fx = import_function(imp_entry)
#
#   reducer = False
#   # task_result = []
#   while True:
#     time.sleep(1)
#     rs = model.Task.find2( dict(prj_id=prj_id) ,sort=dict(task_id=1))
#     running = 0
#     task_result = []
#     for task in rs:
#       task_result.append(task)
#       if task.status !='finish':
#         running += 1
#     if running == 0:
#       reducer = True
#       break
#
#   if reducer:
#     ctx = Context()
#     ctx.project = prj
#     ctx.logger = logger
#     ctx.data = task_result
#     fx(ctx)  # main.result_reducer(ctx)
#

if __name__ == '__main__':
  fire.Fire()

"""
/home/dcp/anaconda3/bin/python -m TedyNode.tedyworker project_deploy RPan
"""