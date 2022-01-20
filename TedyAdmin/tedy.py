#coding:utf-8


import sys,os,os.path,traceback,time,datetime
import copy
import json
import toml
import fire

PWD = os.path.dirname(os.path.abspath(__file__))

def tedy_settings():
  file = os.path.join(PWD, 'tedy-settings.toml')
  data = toml.load( open(file))

  for name,node in data['nodes'].items():
    d = copy.deepcopy(data['node_template'])
    d.update(**node)
    node.update(**d)
  del data['node_template']
  return data


if __name__ == '__main__':
  print(tedy_settings())