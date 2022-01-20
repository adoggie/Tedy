#coding:utf-8

'''
pip install pymssql pymysql
https://pypi.org/project/pymssql/
'''

"""
RegistryClient 
1. 注册服务到服务中心 
2. 接收管理平台发送的控制指令并操作 
3. 上报服务运行状态 

- http 连接 registry server
- 连接mx，从管理系统接收控制消息，发送消息到监控平台 
"""

import fire
import json
import time
import datetime
import os
import traceback,base64,threading
import zmq
from elabs.fundamental.utils.sign_and_aes import sign_check_and_get_data,sign_data

from elabs.fundamental.utils.useful import singleton
# from elabs.fundamental.utils.timeutils import  localtime2utc
from elabs.tedy.command import *
from elabs.utils.zmqex import init_keepalive

@singleton
class MessageClient(object):
  """"""
  def __init__(self):
    self.cfgs = {}
    self.users = []
    self.broker = None
    self.ctx = zmq.Context()
    self.sock_s = self.ctx.socket(zmq.SUB)
    init_keepalive(self.sock_s)
    self.sock_p = self.ctx.socket(zmq.SUB)
    init_keepalive(self.sock_p)

    self.running = False

  def init(self, **cfgs):
    '''
     market_topic :
     market_broker_addr :
    '''
    self.cfgs.update(**cfgs)
    topic = self.cfgs.get('message_topic', '')
    if isinstance(topic, (tuple, list)):
      for tp in topic:
        tp = tp.encode()
        self.sock_s.setsockopt(zmq.SUBSCRIBE, tp)
    else:
      topic = topic.encode()
      self.sock_s.setsockopt(zmq.SUBSCRIBE, topic)  # 订阅所有
    addr = self.cfgs.get('system_broker_addr_s')
    self.sock_s.connect(addr)

    addr = self.cfgs.get('system_broker_addr_p')
    self.sock_p.connect(addr)
    return self

  def addUser(self, user):
    self.users.append(user)
    return self

  def _recv_thread(self):
    self.running = True
    poller = zmq.Poller()
    poller.register(self.sock_s, zmq.POLLIN)
    while self.running:
      text = ''
      try:
        events = dict(poller.poll(1000))
        if self.sock_s in events:
          text = self.sock_s.recv_string()
          self.parse(text)
      except:
        traceback.print_exc()
        print(text)
        time.sleep(.1)

  def parse(self, text):
    message = parseMessage(text)
    for user in self.users:
      user.onMessage(message)


  def open(self):
    self.thread = threading.Thread(target=self._recv_thread)
    self.thread.daemon = True
    self.thread.start()
    return self

  def close(self):
    self.running = False
    self.sock_s.close()
    self.sock_p.close()

  def join(self):
    self.thread.join()

