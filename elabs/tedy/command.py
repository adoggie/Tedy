#coding:utf-8


"""

ver,dest_service:dest_id,from_service:from_id,msg_type

1.0,market:001,manager:m01,status_query,plain,text
1.0,market:001,manager:m01,status_query,base64:json,

"""

import datetime,json,base64
import os
import time
import traceback

class CommandBase(object):
  Type = ''
  def __init__(self):
    self.ver = 'tedy1.0'
    self.dest_service = 'manager' # 中心管理服务
    self.dest_id = 'manager'
    self.from_service = ''
    self.from_id = ''
    self.timestamp = int(datetime.datetime.now().timestamp()*1000)
    self.msg_type = self.Type
    self.encode = 'plain'

  def body(self):
    return ''

  def marshall(self):
    signature = 'nosig'
    text = f"{self.ver},{self.dest_service}:{self.dest_id},{self.msg_type}," \
           f"{self.from_service}:{self.from_id},{self.timestamp}," \
           f"{signature},{self.encode},{self.body()}"
    return text


class ProjectDeploy(CommandBase):
  """项目部署"""
  Type = 'project_deploy'
  def __init__(self):
    CommandBase.__init__(self)
    self.prj_id = ''
    self.encode = 'base64:json'

  def body(self):
    data = dict(prj_id = self.prj_id,)
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

  @classmethod
  def parse(cls,data):
    m = cls()
    m.prj_id = data.get('prj_id')
    return m

class DataDeploy(ProjectDeploy):
  """数据部署"""
  Type = 'data_deploy'
  def __init__(self):
    ProjectDeploy.__init__(self)

class ProjectRun(CommandBase):
  """"""
  Type = 'project_run'
  def __init__(self):
    CommandBase.__init__(self)
    self.prj_id = ''

    self.encode = 'base64:json'

  def body(self):
    data = dict(prj_id = self.prj_id)
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

  @classmethod
  def parse(cls,data):
    m = cls()
    m.prj_id = data.get('prj_id')
    return m

class ProjectStop(CommandBase):
  """"""
  Type = 'project_stop'
  def __init__(self):
    CommandBase.__init__(self)
    self.prj_id = ''

    self.encode = 'base64:json'

  def body(self):
    data = dict(prj_id = self.prj_id)
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

  @classmethod
  def parse(cls,data):
    m = cls()
    m.prj_id = data.get('prj_id')
    return m

class WorkerStart(CommandBase):
  """"""
  Type = 'worker_start'
  def __init__(self):
    CommandBase.__init__(self)
    self.task_id = ''   # 任务编号
    self.prj_id = ''    # 项目编号
    self.node = ''      # 节点编号
    self.from_service ='worker'
    self.from_id = 'worker'

    self.encode = 'base64:json'

  def body(self):
    data = dict(prj_id = self.prj_id,task_id = self.task_id, node = self.node)
    text = base64.b64encode(json.dumps(data).encode()).decode()
    return text

  @classmethod
  def parse(cls,data):
    m = cls()
    m.prj_id = data.get('prj_id')
    m.task_id = data.get('task_id')
    m.node = data.get('node')
    return m

class WorkerStop(CommandBase):
  """"""
  Type = 'worker_stop'
  def __init__(self):
    CommandBase.__init__(self)

MessageDefinedList = [
  ProjectDeploy,
  DataDeploy,
  ProjectRun,
  ProjectStop,
]

def parseMessage(text):
  """解析消息报文"""
  if isinstance(text,bytes):
    text = text.decode()
  fs = text.split(',')
  if len(fs) < 8:
    return None
  ver ,dest,msg_type,from_,timestamp,signature,encode,body,*others = fs
  if ver !='tedy1.0':
    print("Error: msg ver error! ",text)
    return None
  m = None
  for md in MessageDefinedList:
    m = None
    try:
      if md.Type == msg_type:
        encs= encode.split(':')
        for enc in encs:
          if enc == 'base64':
            body = base64.b64decode(body)
          elif enc == 'json':
            body = json.loads(body)

        m = md.parse(body)
        m.ver = ver
        m.dest_service, m.dest_id = dest.split(':')
        m.msg_type = msg_type
        m.from_service, m.from_id = from_.split(':')
        m.timestamp = int(float(timestamp))
        m.signature = signature
    except:
      traceback.print_exc()
      m = None
    if m:
      break
  return m

def test_serde():
  print()
  text = PositionSignal.rand_one().marshall()
  print(text)
  m = parseMessage(text)
  text2 = m.marshall()
  print(text2)
  assert (text == text2)

  text = ServiceStatusRequest.rand_one().marshall()
  print(text)
  m = parseMessage(text)
  text2 = m.marshall()
  print(text2)
  assert (text == text2)

  text = ServiceStatusReport.rand_one().marshall()
  print(text)
  m = parseMessage(text)
  text2 = m.marshall()
  print(text2)
  assert (text == text2)

  text = ServiceKeepAlive.rand_one().marshall()
  print(text)
  m = parseMessage(text)
  text2 = m.marshall()
  print(text2)
  assert (text == text2)

  text = ServiceLogText.rand_one().marshall()
  print(text)
  m = parseMessage(text)
  text2 = m.marshall()
  print(text2)
  assert (text == text2)
