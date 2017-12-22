import os
for i in range(0,2):
    cmd="ssh 1155086998@proj"+str(10-i)+" & /data/opt/tmp/1155086998/pms/build/./TestSVM "+str(10-i)+" "+str(i)
    os.system(cmd)