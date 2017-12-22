import os
for i in range(0,1):
    cmd="ssh 1155086998@proj"+str(i+5)+" & /data/opt/tmp/1155086998/pms/build/./Testlr "+str(i)
    os.system(cmd)