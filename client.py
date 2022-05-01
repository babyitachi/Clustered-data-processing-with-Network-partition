import os
import sys
from tasks import map
from celery import chord, group
from config import get_redis
from time import sleep
from config import IPS
import subprocess

if __name__ != '__main__':
  sys.exit(1)

# Invoke this file as `python3 client.py True` to run the consistent version.
CONSISTENT=False
print(f"Running with CONSISTENT = {CONSISTENT}")

if (len(sys.argv) < 2):
    print("Use the command: python3 client.py <data_dir>")

DIR=sys.argv[1]

# ======== Network Partition Configuration ==========
# The current node is isolated from other nodes
# Update the current_ip variable with the IP.
current_ip = {'10.17.10.17'}
faulty_node_ips = list(set(IPS).difference(current_ip))

# Firewall Commands
isolate_command = \
  f'''sudo iptables -I INPUT 1 -s {faulty_node_ips[0]} -p tcp --dport 6379 -j DROP; 
  sudo iptables -I OUTPUT 1 -d {faulty_node_ips[0]} -p tcp --dport 6379 -j DROP;'''

heal_command = \
  f'''sudo iptables -D INPUT -s {faulty_node_ips[0]} -p tcp --dport 6379 -j DROP; 
  sudo iptables -D OUTPUT -d {faulty_node_ips[0]} -p tcp --dport 6379 -j DROP;'''

abs_files=[os.path.join(pth, f) for pth, _, files in os.walk(DIR) for f in files]

abs_files=abs_files[:500]
job = group(map.s(files) for files in abs_files)
print(job)

results=job.apply_async()

sleep(2)

subprocess.run([isolate_command], shell=True, text=True, input='123pass\n')  # Updated with password
print("The network partition is in place")

results.get()

rds = get_redis(CONSISTENT)
wc = rds.get_top_words(10)
print(wc)

subprocess.run([heal_command], shell=True, text=True, input='123pass\n') # Updated with password
print("The network partition is healed")

wc = rds.get_top_words(10, True)
print(wc)
