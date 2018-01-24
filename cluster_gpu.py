import os
import pickle
import time
from pprint import pprint
import fcntl
import json
import pandas as pd

MaxTimeSpacing = 300
period = 3
#home = os.path.expanduser('~')
#base_dir = os.path.join(home, 'gpustat')
base_dir = '/home/PublicDataset/gpuStats'
suffix = '.json'#'.pickle'
node_stats = {}
output_file = '/home/PublicDataset/gpuStats/gpu_stats.txt'


def read_fr_file(file_path, lock_file):
    while True:
        try:
            with open(file_path,'r') as f:
    #           import pdb;pdb.set_trace()
                open(lock_file,'w+').close()
                info = json.load(f)
                return info
        except Exception as e:
                # import pdb;pdb.set_trace()
            print(e)
        finally:
            os.remove(lock_file)

def scan():
    file_items = os.walk(base_dir, followlinks=True)
    #import pdb;pdb.set_trace()
    nodes = {}
    for root,_,files  in file_items:
        for f in files:
            if not f.endswith(suffix):
                continue
            node_i = f.split('_')[0]
            lock_file = os.path.join(root,f+'.lock')
            while os.path.exists(lock_file):
                time.sleep(0.2)
                print("lock: ", lock_file)
            
            file_path = os.path.join(root,f)
            info = read_fr_file(file_path, lock_file)
            timestamp = info.get('timestamp', 0)
            cur_timestamp = time.time()
            if abs(cur_timestamp-timestamp)>MaxTimeSpacing:
                node_stats['node_i'] = None
                print("%s break" % node_i)
            else:
                node_stats[node_i] = info['gpus_stats']
                print("node: ", node_i)
                for gpu_info in info['gpus_stats']:
                    pprint(gpu_info)

def print2file():
        global node_stats
        open(output_file,'w+').close() #clear the file
        node_stats_lst =  sorted(node_stats.items(), key=lambda t:t[0])
        for node_i, gpus_status in node_stats_lst:
            #import pdb;pdb.set_trace()
            with open(output_file, 'a') as f:
                if not gpus_status:
                    f.write(node_i + "is broken\n")
                    continue
                else:
                    f.write(node_i+'\n')
                data = []
                for gpu_stat in gpus_status:
#                    import pdb;pdb.set_trace()
                    index = gpu_stat['index']
                    del gpu_stat['index']
                    del gpu_stat['power.draw']
                    del gpu_stat['power.limit']
                    data.append(pd.DataFrame(gpu_stat, index=[index]))
                data = pd.concat(data)
                #import pdb;pdb.set_trace()
                data.to_csv(f,index_label='index', sep='\t')

if __name__ == '__main__':
    while True:
        print("working...")
        scan()
        print2file()
        time.sleep(period)
