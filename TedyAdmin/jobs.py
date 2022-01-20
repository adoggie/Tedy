#coding:utf-8
import sys,os,os.path,traceback,time,datetime
import copy
import json
import toml
import fire
import pymongo
from elabs.tedy import logger
from tedy import tedy_settings
from elabs.app.core.logger import initLogger
import project as tedy_project

PWD = os.path.dirname(os.path.abspath(__file__))

initLogger('DEBUG', os.path.join(PWD), 'jobs.log',stdout=True)

# logger.init()
def init_database(name='Tedy'):
  settings = tedy_settings()
  conn = pymongo.MongoClient(**settings['mongodb'])
  return conn[name]


def start(name='job1'):
  """启动工作任务job
  一次加载执行所有 计算项目
  """
  file = os.path.join(PWD, 'tedy-jobs.toml')
  jobdata = toml.load(open(file))
  if name not in jobdata:
    logger.error("Job name invalide!",name)
    return
  job = jobdata[name]

  db = init_database('JOBS')
  now = datetime.datetime.now()
  now = str(now).split('.')[0].replace(' ', '_').replace(':', '').replace('-', '')
  cname = f"{name}_{now}"
  coll = db[cname]

  logger.info(f"-- Job: {name}  begin.. ---")
  for prj_id in job['task']:

    settings = tedy_settings()
    repo_dir = settings['default']['repo_dir']
    prj_dir = os.path.join(repo_dir, prj_id)
    if not os.path.exists(prj_dir):
      logger.error("Project not existed!", prj_dir)
      return

    cfg_file = os.path.join(prj_dir, 'cone-project.toml')
    cfgs = toml.load(open(cfg_file))
    exec_time_limit = cfgs.get('exec_time_limit',0) * 60  # seconds
    if not exec_time_limit: #
      exec_time_limit =   1e+10
    exec_time_end = datetime.datetime.now().timestamp() + exec_time_limit

    tedy_project.stop(prj_id)
    time.sleep(2)
    tedy_project.run(prj_id,'nowait')
    logger.info(" Project running start.." , prj_id)

    job_start = datetime.datetime.now()
    while True:
      nodes, workers, finished, start, end = tedy_project.status(prj_id, silent=True)

      data = dict( prj_id = prj_id, start = job_start, end=None ,result = '',
                   nodes = nodes,workers = workers , finished = finished
                   )
      if workers == finished:
        data['end'] = datetime.datetime.now()
        data['result'] = 'succ'
        logger.info("Project exec succ .",prj_id)

      # 此project 超时运行最大时间限制
      if datetime.datetime.now().timestamp() > exec_time_end:
        data['result'] = 'timeout'
        data['end'] = datetime.datetime.now()
        logger.error("Project running timeout. " , prj_id)

      coll.update_one({'prj_id':prj_id} ,{'$set':data} ,upsert=True )

      if workers == finished:
        break
      time.sleep(2)
  logger.info(f"-- Job: {name}  end. --")



if __name__ == '__main__':
  fire.Fire()