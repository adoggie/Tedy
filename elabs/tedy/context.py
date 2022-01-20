#coding:utf-8

from elabs.tedy.node import Node
from elabs.tedy.model import Project

class Context(object):
  """调度上下文"""
  def __init__(self):
    self.cfgs = {} #  with cone-project.toml
    self.node = ''
    self.project = ''
    self.data_dir = ''
    self.repo_dir = ''
    self.node_dir = ''
    self.task_list = []
    self.ncores = 1





if __name__ == '__main__':
  print(Context().node)