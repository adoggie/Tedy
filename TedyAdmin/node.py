#coding:utf-8
import sys,os,os.path,traceback,time,datetime
import copy
import json
import toml
import fire
import pymongo

from tedy import tedy_settings
PWD = os.path.dirname(os.path.abspath(__file__))

def init_database():
  settings = tedy_settings()
  conn = pymongo.MongoClient(**settings['mongodb'])
  return conn['tedy']


def install(node=''):
  """init node and install TedyNode
    1.拷贝node 代码到远程服务器
    2.注册node的worker信息到nosql
  """
  settings = tedy_settings()
  nodes = settings['nodes'].keys()
  if node:
    nodes=[node]
  
  prj_dir = os.path.join(PWD,'../TedyNode')
  db = init_database()
  coll = db['Node']
  coll.delete_many({})

  for name in nodes:
    prj_dir = os.path.join(PWD, '../TedyNode')
    node = settings['nodes'][name]
    node['name'] = name
    node['message_topic'] = [f"tedy1.0,node:{name}"]
    settings_file = os.path.join(prj_dir,'tedy-settings.toml')
    data = node
    data['mongodb'] = settings['mongodb']
    fp = open(settings_file,'w')
    toml.dump(data,fp)
    fp.close()

    cmd = f"sshpass -p {node['ssh_passwd'] } rsync -ave 'ssh -p {node['ssh_port']} -o StrictHostKeyChecking=no ' {prj_dir} {node['ssh_user']}@{node['ip']}:{node['node_dir']}/ "

    # cmd = "rsync -av -e 'ssh -p {port} -o StrictHostKeyChecking=no ' {prj_dir} {user}@{ip}:{repo}". \
    #   format(prj_dir=prj_dir, user=node['user'], port=node['port'], ip=node['ip'], repo=node['node_dir'])
    print(cmd)
    os.system(cmd)
    """
    sshpass -p tedy123 rsync -ave 'ssh -p 2022 -o StrictHostKeyChecking=no' /Users/scott/Desktop/projects/Tedy/TedyAdmin/../TedyNode tedy@127.0.0.1:/home/tedy/node/
    """
    prj_dir = os.path.join(PWD, '../elabs')
    cmd = f"sshpass -p {node['ssh_passwd']} rsync -ave 'ssh -p {node['ssh_port']} -o StrictHostKeyChecking=no ' {prj_dir} {node['ssh_user']}@{node['ip']}:{node['node_dir']}/TedyNode/ "

    # cmd = "rsync -av -e 'ssh -p {port} -o StrictHostKeyChecking=no ' {prj_dir} {user}@{ip}:{repo}". \
    #   format(prj_dir=prj_dir, user=node['user'], port=node['port'], ip=node['ip'], repo=node['node_dir'])
    print(cmd)
    os.system(cmd)

    coll.insert_one(node)


def start(node=''):
  """
  """
  settings = tedy_settings()
  nodes = settings['nodes'].keys()
  if node:
    nodes = [node]

  for name in nodes:
    node = settings['nodes'][name]
    node['name'] = name
    node_start = f"flock -xn /tmp/tedynode.lock -c  'cd {node['node_dir']}/TedyNode; {node['python']} ./tedynode.py run --noprompt=true' &"
    cmd = f"sshpass -p {node['ssh_passwd']} ssh  -p {node['ssh_port']} -o StrictHostKeyChecking=no  {node['ssh_user']}@{node['ip']} {node_start} "

    print(cmd)
    # os.system(cmd)

"""
*/1 * * * * flock -xn /tmp/tedynode.lock -c  'cd /home/tedy/node/TedyNode; /home/tedy/anaconda3/bin/python ./tedynode.py run --noprompt=true'

flock -xn /tmp/tedynode.lock -c  'cd /home/tedy/node/TedyNode;  /home/tedy/anaconda3/bin/python ./tedynode.py run --noprompt=true'
"""

def stop(node=''):
  """
  """
  settings = tedy_settings()
  nodes = settings['nodes'].keys()
  if node:
    nodes = [node]

  for name in nodes:
    node = settings['nodes'][name]
    node['name'] = name
    node_cmd = f"pkill -f -9 tedynode.py "
    cmd = f"sshpass -p {node['ssh_passwd']} ssh  -p {node['ssh_port']} -o StrictHostKeyChecking=no  {node['ssh_user']}@{node['ip']} {node_cmd} "

    print(cmd)
    os.system(cmd)

def stop_workers(node=''):
  """
  停止 计算节点上所有的工作 进程 worker
  """
  settings = tedy_settings()
  nodes = settings['nodes'].keys()
  if node:
    nodes = [node]

  for name in nodes:
    node = settings['nodes'][name]
    node['name'] = name
    node_cmd = f"pkill -f -9 tedyworker.py "
    cmd = f"sshpass -p {node['ssh_passwd']} ssh  -p {node['ssh_port']} -o StrictHostKeyChecking=no  {node['ssh_user']}@{node['ip']} {node_cmd} "

    print(cmd)
    os.system(cmd)


__all__ = ('install',)

if __name__ == '__main__':
  fire.Fire()