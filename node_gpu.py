from detect import *
import time
import os
import fcntl
import json
import pickle
import sys

period = 5

if __name__ == '__main__':
    #get hostname
    node_name = os.popen('hostname').readline().strip()
    home = os.path.expanduser('~')
    #filename = '/home/yanshp/gpustat/{}_gpu_stat.pickle'.format(node_name)
    filename = os.path.join(home, 'gpustat/{}_gpu_stat.json'.format(node_name))
    while True:
        lock_file = filename + '.lock'
        while os.path.exists(lock_file):
            time.sleep(0.1)
        print("host_name:%s open file" % node_name)
        try:
            with open(filename,'w') as f:
                open(lock_file, 'w+').close()
                # import pdb;pdb.set_trace()
                contents = query_gpu()
                timestamp = time.time()
                result ={
                    'timestamp': timestamp,
                    'gpus_stats': contents,
                }
                #import pdb;pdb.set_trace()
                json.dump(result, f)
                print("update successfully")
        except  KeyboardInterrupt as e:
            os.remove(lock_file)
            print("exiting .......")
            sys.exit(0)
        except IOError as e:
            pass
        finally:
            time.sleep(0.5)
            os.remove(lock_file)
            time.sleep(period)

