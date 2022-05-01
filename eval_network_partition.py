import redis
from deepdiff import DeepDiff
import time
import os
import ast
import sys
import subprocess
from config import get_redis, IPS

rds=get_redis(False)

def reset_redis():
    for red in rds.conns:
        red.flushall()

def convert_list_dict(data):
    wc = {}
    for data_item in data:
        wc.update({data_item[0]: data_item[1]})
    return wc

def get_top_words(network_heal=False):
    if network_heal:
        return rds.get_top_words(10, True)
    else:
        return rds.get_top_words(10, False)

def check_correctness(output_file, debug=False, network_heal=False):
    opath = sys.argv[1] # Path of the output directory
    output_file = open(os.path.join(opath, output_file), "r")
    correct_answer = ast.literal_eval(str.strip(output_file.read()))
    result = get_top_words(network_heal)
    if result is None:
        return 0
    result = convert_list_dict(result)
    diff = DeepDiff(result, correct_answer)
    if debug:
        print(correct_answer)
        print(result)
        print(diff)

    return (len(correct_answer)-len(diff.get('values_changed',{}))-len(diff.get('dictionary_item_added',{})))/len(correct_answer)

output_file= sys.argv[2] # The output file of the testcase


# ======== Network Partition Configuration ==========
# The current node is isolated from other nodes
# Update the current_ip variable with the IP.
current_ip = IPS[1]
faulty_node_ips = list(set(IPS).difference(current_ip))

# Firewall Commands
isolate_command = \
  f'''sudo iptables -I INPUT 1 -s {faulty_node_ips[0]} -p tcp --dport 6379 -j DROP; 
  sudo iptables -I OUTPUT 1 -d {faulty_node_ips[0]} -p tcp --dport 6379 -j DROP'''

heal_command = \
  f'''sudo iptables -D INPUT -s {faulty_node_ips[0]} -p tcp --dport 6379 -j DROP; 
  sudo iptables -D OUTPUT -d {faulty_node_ips[0]} -p tcp --dport 6379 -j DROP'''

timeout=150
elapsed_time = 0
correctness = 0
stream = 0

#reset_redis()
input("Start Execution of the client script!!!!")

subprocess.run([isolate_command], shell=True, text=True, input='guess123\n')
input("Do you want to heal the network partition?")

subprocess.run([heal_command], shell=True, text=True, input='guess123\n')

start_time = time.time()
while (elapsed_time < timeout):
    correctness = max(correctness, check_correctness(output_file, network_heal=True))
    elapsed_time = time.time() - start_time
    if(correctness == 1.0):
        break
    time.sleep(1)
 
if(elapsed_time >= timeout):
    check_correctness(output_file, debug=True, network_heal=True)
else:
    check_correctness(output_file, network_heal=True)
 
print({"elapsed_time": elapsed_time, "correctness": correctness})
