#coding:utf-8

import sys,os,os.path,traceback,time,datetime
import copy
import json
import toml
import fire
import pymongo
import zmq
from elabs.tedy import logger
from elabs.tedy import model

# from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
# from tinyrpc.transports.http import HttpPostClientTransport
# from tinyrpc import RPCClient

from tedy import tedy_settings

from elabs.fundamental.utils.importutils import import_function
from elabs.tedy.context import Context
from elabs.tedy.logger import *
from elabs.tedy.command import *

PWD = os.path.dirname(os.path.abspath(__file__))

def init_database(name=''):
  settings = tedy_settings()
  conn = pymongo.MongoClient(**settings['mongodb'])
  if not name:
    name = 'ConTedy'
  return conn[name]

logger.init()
settings = tedy_settings()

zctx = zmq.Context()
sock = zctx.socket(zmq.PUB)
addr = settings['default']['system_broker_addr_p']
sock.connect(addr)
time.sleep(0.1)

def init(prj_id):
  repo_dir = settings['default']['repo_dir']
  prj_dir = os.path.join(repo_dir, prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!", prj_dir)
    return
  task_split(prj_id)
  INFO('Project Init Okay!')

def task_split(prj_id):
  """任务分割，一个worker对应多个任务"""
  settings = tedy_settings()
  repo_dir = settings['default']['repo_dir']

  prj_dir = os.path.join(repo_dir, prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!", prj_dir)
    return

  path = prj_dir
  sys.path.append(prj_dir)
  
  path = os.path.join(path,'cone-project.toml')
  configs = toml.load(open(path))
  imp_entry = configs.get('handlers').get('task_split')  # 加载运行触发入口

  if not imp_entry:
    return
  fx = import_function(imp_entry)
  #

  task_list = fx()  # main.task_split(ctx) 项目任务切割

  db = init_database('Task')
  coll = db[prj_id]
  coll.delete_many({})

  all_workers = 0
  for name in configs['default']['nodes']:
    node = settings.get('nodes')[name]
    all_workers+=node['workers']
  task_num = len(task_list)

  # num = task_num // all_workers
  # if task_num > num * all_workers:
  #   num+=1
  node_workers = []
  for name in configs['default']['nodes']:
    node = settings.get('nodes')[name]
    node_workers.append([name,node['workers'],[]])

  while task_list:
    for nw in node_workers:
      for w in range(nw[1]):  # workers
        if not task_list:
          continue
        t = task_list.pop(0)  # task input args
        args = nw[2]
        args.append(t)

  for nw in node_workers:
    task_num = len(nw[2])
    workers = nw[1]

    tasks_per_worker = task_num//workers
    if task_num % workers:
      tasks_per_worker = (task_num+workers)//workers

    worker = 0
    while nw[2]:
      tasks = nw[2][:tasks_per_worker]
      data=dict(node= nw[0],
                worker = worker,
                task_list= tasks,
                start = None ,
                end = None
                )
      coll.insert_one(data)
      nw[2] = nw[2][tasks_per_worker:]
      worker+=1
    # 一个node存放多个N个task
  INFO('Project Task Split Okay!')

def deploy( prj_id ):
  """部署指定项目到集群
    打包拷贝项目文件到计算节点
  """
  settings = tedy_settings()
  repo_dir = settings['default']['repo_dir']
  
  prj_dir = os.path.join(repo_dir,prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!",prj_dir)
    return

  cfg_file = os.path.join(prj_dir,'cone-project.toml')
  cfgs = toml.load( open(cfg_file))
  
  for name in cfgs['default']['nodes']:
    node = settings['nodes'].get(name)

    cmd = f"sshpass -p {node['ssh_passwd']} rsync -ave 'ssh -p {node['ssh_port']} -o StrictHostKeyChecking=no ' {prj_dir} {node['ssh_user']}@{node['ip']}:{node['repo_dir']}/ "

    # cmd = "rsync -av -e 'ssh -p {port} -o StrictHostKeyChecking=no ' {prj_dir} {user}@{ip}:{repo}".\
    #   format(prj_dir=prj_dir,user= node['user'],port=node['port'],ip=node['ip'],repo = node['repo_dir'])
    print(cmd )
    os.system(cmd)

    elabs_dir = os.path.join(PWD, '../elabs')
    cmd = f"sshpass -p {node['ssh_passwd']} rsync -ave 'ssh -p {node['ssh_port']} -o StrictHostKeyChecking=no ' {elabs_dir} {node['ssh_user']}@{node['ip']}:{node['repo_dir']}/{prj_id}/ "
    print(cmd)
    os.system(cmd)

    m = ProjectDeploy()
    m.dest_service = "node"
    m.dest_id = "node"
    m.from_service = "admin"
    m.from_id = "admin"
    m.prj_id = prj_id
    sock.send(m.marshall().encode())


def data_deploy(prj_id, par=False):
  """拷贝项目的数据到集群目录
   par - 是否并行
  """
  settings = tedy_settings()
  
  # data_dir = settings['default']['data_dir']
  repo_dir = settings['default']['repo_dir']
  prj_dir = os.path.join(repo_dir, prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!", prj_dir)
    return
  
  cfg_file = os.path.join(prj_dir, 'cone-project.toml')
  # DEBUG(cfg_file)
  cfgs = toml.load(open(cfg_file))

  data_dir = cfgs['default']['data_dir']
  for name in cfgs['default']['nodes']:
    node = settings['nodes'].get(name)

    #拷贝本地数据到计算节点
    cmd = f"sshpass -p {node['ssh_passwd']} rsync -ave 'ssh -p {node['ssh_port']} -o StrictHostKeyChecking=no ' {data_dir} {node['ssh_user']}@{node['ip']}:{node['data_dir']}/ "
    print(cmd)
    os.system(cmd)

    # notify node
    m = DataDeploy()
    m.dest_service = "node"
    m.dest_id = "node"
    m.from_service = "admin"
    m.from_id = "admin"
    m.prj_id = prj_id
    sock.send(m.marshall().encode())

def run(prj_id , wait='wait'):
  settings = tedy_settings()
  db = init_database('Task')
  if prj_id not in db.list_collection_names():
    logger.ERROR("Project not Found!",prj_id)
    return

  repo_dir = settings['default']['repo_dir']
  prj_dir = os.path.join(repo_dir, prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!", prj_dir)
    return

  # coll = model.Task.collection()
  # coll.update_many(dict(prj_id=prj_id),{'$set':{'status':'idle'}})
  # logger.DEBUG('Updating Task Status ..','Okay')
  #
  # repo_dir = settings['default']['repo_dir']
  # prj_dir = os.path.join(repo_dir, prj_id)
  # if not os.path.exists(prj_dir):
  #   logger.error("Project not existed!", prj_dir)
  #   return
  
  cfg_file = os.path.join(prj_dir, 'cone-project.toml')
  # DEBUG(cfg_file)
  cfgs = toml.load(open(cfg_file))
  
  for node in cfgs['default']['nodes']:
    # _nod = settings['nodes'].get(node)
    # node.update(**_nod)
    
    # notify node
    m = ProjectRun()
    m.dest_service = "node"
    m.dest_id = node
    m.from_service = "admin"
    m.from_id = "admin"
    m.prj_id = prj_id
    bytes = m.marshall().encode()
    sock.send(bytes)
    print(bytes)

  if wait == 'nowait':
    return

  # 轮训db中的项目task运行状态
  nodes =0
  workers = 0
  finished = 0
  start = ''
  end = ''
  while True:
    time.sleep(1)
    n,w,f,start,end = status(prj_id,silent=True)
    if n != nodes or w !=workers or f!=finished:
      print(f"Nodes:{n}  Workers:{w}  Finished:{f}")
    if workers == finished:
      break

  print("Project Finished!")
  print(f"Project:{prj_id} Nodes:{nodes} Workers:{workers} start:{start}  end:{end}")


def stop(prj_id):
  settings = tedy_settings()

  repo_dir = settings['default']['repo_dir']
  prj_dir = os.path.join(repo_dir, prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!", prj_dir)
    return

  cfg_file = os.path.join(prj_dir, 'cone-project.toml')
  cfgs = toml.load(open(cfg_file))

  for node in cfgs['default']['nodes']:
    # notify node
    m = ProjectStop()
    m.dest_service = "node"
    m.dest_id = node
    m.from_service = "admin"
    m.from_id = "admin"
    m.prj_id = prj_id
    bytes = m.marshall().encode()
    sock.send(bytes)
    print(bytes)


def list_prj():
  """ list all projects in registry."""
  db = init_database('Task')
  names = db.list_collection_names()
  for name in names:
    print(f"Project: {name}")


def status(prj_id ,silent=False ):
  """查询运行状态
    nodes,workers,finished，start,end
    节点数，workers数，完成workers数，开始，结束时间
  """
  settings = tedy_settings()

  repo_dir = settings['default']['repo_dir']
  prj_dir = os.path.join(repo_dir, prj_id)
  if not os.path.exists(prj_dir):
    logger.error("Project not existed!", prj_dir)
    return

  cfg_file = os.path.join(prj_dir, 'cone-project.toml')
  cfgs = toml.load(open(cfg_file))

  db = init_database('Task')
  coll = db[prj_id]
  rs = coll.find({})
  nodes = {}
  worker_finished = 0
  rs = list(rs)
  start_times = []
  end_times = []
  max_worker_time_cost = 0
  for r in rs:
    if r['start']:
      start_times.append(r['start'])
    if r['end']:
      end_times.append(r['end'])
    if r['start'] and r['end']:
      cost = (r['end'] - r['start']).total_seconds()
      max_worker_time_cost = max(cost,max_worker_time_cost)

    fin = 1 if r['end'] else 0
    if r['node'] not in nodes:
      nodes[r['node']] = dict(workers=1,task_list=len(r['task_list']))

    else:
      node = nodes[r['node']]
      node['workers']+=1
      node['task_list']+= len(r['task_list'])
    worker_finished += fin

  if not silent:
    print("-" * 20)
    print(f"project:{prj_id}")
    print(f"nodes:{len(nodes)} , workers:{len(rs)} , finished: {worker_finished}")
    # print(f"workers:{len(rs)} , Finished: {worker_finished}")

  start_times.sort()
  end_times.sort()
  start =''

  if start_times:
    start = start_times[0]
  # print(f"start: {str(start).split('.')[0]}")
  end = datetime.datetime.now()
  if end_times:
    end = end_times[-1]

  print(f"start: {str(start).split('.')[0]} , end: {str(end).split('.')[0]}")
  task_time_elapsed = round( (end - start).total_seconds() / 60,2)
  print(f"total task time: { task_time_elapsed } mins")
  print(f"max worker time: { round(max_worker_time_cost / 60 , 2) } mins")
  print("-"*20)
  return len(nodes),len(rs),worker_finished,start,end




if __name__ == '__main__':
  fire.Fire()

  