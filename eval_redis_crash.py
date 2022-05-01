import redis
from deepdiff import DeepDiff
import time
import os
import ast
import sys
from pexpect import pxssh
from config import get_redis, IPS

rds=get_redis(False)

def reset_redis():
    for red in rds.conns:
        red.flushall()

def get_top_words(crash_heal=False):
    if crash_heal:
        return rds.get_top_words(10, True)
    else:
        return rds.get_top_words(10, False)

def convert_list_dict(data):
    wc = {}
    for data_item in data:
        wc.update({data_item[0]: data_item[1]})
    return wc

def check_correctness(output_file, debug=False, crash_heal=False):
    opath = sys.argv[1] # Path of the output directory
    output_file = open(os.path.join(opath, output_file), "r")
    correct_answer = ast.literal_eval(str.strip(output_file.read()))
    result = get_top_words(crash_heal)
    if result is None:
        return 0
    result = convert_list_dict(result)
    diff = DeepDiff(result, correct_answer)
    if debug:
        print(correct_answer)
        print(result)
        print(diff)

    return (len(correct_answer)-len(diff.get('values_changed',{}))-len(diff.get('dictionary_item_added',{})))/len(correct_answer)

def crash_redis(ip: str) -> None:
    s = pxssh.pxssh()
    s.login(ip, 'student', '123pass\n') #Update the password

    s.sendline(f'redis-cli -h {ip} SHUTDOWN')
    s.prompt()
    print(s.before)
    print("Redis shutdown")

def start_redis(ip: str) -> None:
    s = pxssh.pxssh()
    s.login(ip, 'student', '123pass\n') #Update the password

    s.sendline('redis-server ~/labs/2021SIY7558_Abhishrut/redis.conf')
    s.prompt()
    print(s.before)
    print("Redis restarted")


output_file= sys.argv[2] # The output file of the testcase
timeout=150
elapsed_time = 0
correctness = 0
stream = 0
start_time = time.time()

#reset_redis()

input("Start Execution of the client script!!!!")

crash_redis(IPS[1])

input("Do you want to reset the redis instance?")

start_redis(IPS[1])

while (elapsed_time < timeout):
    correctness = max(correctness, check_correctness(output_file, crash_heal=True))
    elapsed_time = time.time() - start_time
    if(correctness == 1.0):
        break
    time.sleep(1)
    

#if(elapsed_time >= timeout):
#    check_correctness(output_file, debug=True, crash_heal=True)
#else:
#    check_correctness(output_file, crash_heal=True)
 
print({"elapsed_time": elapsed_time, "correctness": correctness})
