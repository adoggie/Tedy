#coding:utf-8

import time
from elabs.tedy.context import Context
from elabs.tedy.logger import INFO,DEBUG,WARN,ERROR,init as init_log
import smartQ

init_log()
# remote
def project_deploy(ctx:Context):
  """项目代码部署触发"""
  INFO("project deployed!")

# remote
def data_deploy(ctx:Context) :
  """数据部署触发"""
  INFO(">> data_deploy ..")


# local
def task_split() -> list:
  """任务切割"""
  task_list = []
  mins_bin = [1, 5, 15]
  period_bin = [1, 3, 5, 10, 20, 30]
  beta_bin = [0.25]
  for mins in mins_bin:
    for period in period_bin:
      for beta in beta_bin:
        task_list.append((mins,period,beta,20))
  return task_list

# remote
def computating( ctx:Context) :
  INFO(">> computing...")
  print("par n_cores:",ctx.ncores)

  for n,args in enumerate(ctx.task_list):
    print(f"step: {n}/{len(ctx.task_list)} ",args)
    args[-1] = ctx.ncores
    smartQ.SmartQ( *args)

if __name__ == '__main__':
  pass

