
## TedyAdmin

### 1.node

安装TedyNode到计算节点
> python node.py install 

### crontab
> */1 * * * * flock -xn /tmp/tedynode.lock -c  'cd /home/tedy/node/TedyNode; /home/tedy/anaconda3/bin/python ./tedynode.py run --noprompt=true'

ansible f4cs -m cron -a " state=present user=tedy minute=*/1 job="flock -xn /tmp/tedynode.lock -c  'cd /home/tedy/node/TedyNode; /home/tedy/anaconda3/bin/python ./tedynode.py run --noprompt=true' "

- name: tedynode cron
  cron:
    name : "node-cron"
    state : present 
    user: tedy 
    minute : */1
    job : "flock -xn /tmp/tedynode.lock -c  'cd /home/tedy/node/TedyNode; /home/tedy/anaconda3/bin/python ./tedynode.py run --noprompt=true' "
