from time import sleep
from celery import Celery,group
from config import IPS, get_redis

broker = f'pyamqp://test:test@{IPS[0]}'
app = Celery('tasks', backend='rpc', broker=broker)

@app.task(acks_late=True, ignore_results=True, bind=True, max_retries=1,CELERY_TASK_REJECT_ON_WORKER_LOST=True)
def map(self, filename):
    count={}
    with open(filename, mode='r', newline='\r') as f:
        for text in f:
            if text == '\n':
                continue
            ## remove first 3 fields and last two fields
            ## tweet itself can have commas
            sp = text.split(',')[4:-2]
            tweet = " ".join(sp)
            for word in tweet.split(" "):
                if word not in count:
                    count[word]=1
                else:
                    count[word]=count[word]+1

    rds = get_redis(False)
    rds.pushNewSet(filename,count)
        