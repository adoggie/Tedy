
###############################
#TedyAdmin
###############################
vim TedyAdmin/tedy-settings.toml
    'default/repo_dir'
    '[nodes]'

python -m TedyAdmin.node install <nodename>
python -m TedyAdmin.project init <DemoPhoenix>
python -m TedyAdmin.project deploy <DemoPhoenix>
python -m TedyAdmin.project list
python -m TedyAdmin.project run <DemoPhoenix>

###############################
#TedyNode
###############################
install Anaconda3
pip3 install -r ./requirements.txt

vim TedyNode/tedy-settings.toml
    - name , python, NoSQL(host,port)

python3 -m TedyNode.tedyrpc

 sshpass -p 'password' rsync -ave ssh /src/ user@hostname:/dst/
