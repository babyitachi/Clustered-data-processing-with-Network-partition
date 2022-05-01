from abc import ABC
# Import part
import redis
from time import sleep
from typing import Tuple,List

# from config import IPS
class Lab3Redis(ABC):
  """
  __init__ accepts a list of IP addresses on which redis is deployed.
  Number of IPs is typically 3.
  """

  def __init__(self, ips: List[str]):
    self.conns = [redis.Redis(host=ip, decode_responses=True, socket_timeout=5) for ip in ips]
    self.num_instances = len(ips)
    # print('self.num_instances from constructor availredis ',self.num_instances)

  def get_top_words(self, n: int,
                    repair: bool = False) -> List[Tuple[str, int]]:
    words=[]
    for i in range(self.num_instances):
      words.append(self.conns[i].zrevrange(0,0,n,True))
    return words

class ConsistentRedis(Lab3Redis):

  def get_top_words(self, n: int,
                    repair: bool = False) -> List[Tuple[str, int]]:
    pass

class AvailableRedis(Lab3Redis):
  """
  This method is necessary for evaluation
  """
  def __init__(self,ips):
    self.con=Lab3Redis(ips)
    self.rd = self.con.conns
    self.count=self.con.num_instances

  def getAllKeys(self):
    try:
      allkeys=[]
      for i in range(self.count):
        allkeys.append(self.getAllKeysByRedis(self.rd[i]))
      return allkeys
    except:
      return []

  def getAllKeysByRedis(self,red):
    try:
      if red.ping():
        keys=[]
        keys=red.keys()
        if keys:
          keys.sort()
        return keys
      return []
    except:
      return []

  def getTopKeyByRedis(self,red,max=True):
    try:
      if red.ping():
        keys=[]
        keys=red.keys()
        if keys:
          keys.sort(reverse=max)
        return keys[0] if len(keys)>0 else -1
      return -1
    except:
      return -1

  def pushNewSetByRedis(self,instance,filename,wordCounts:dict):
    try:
      if self.rd[instance].exists(filename)==0:
        p=self.rd[instance].pipeline()
        p.watch(filename)
        p.multi()
        p.zadd(filename,wordCounts)
        p.execute()
        return 1
    except Exception as e:
      return 0

  def pushNewSet(self,filename,wordCounts:dict):
    try:
      ret=[]
      for i in range(self.count):
        ret.append(self.pushNewSetByRedis(i,filename,wordCounts))
      if sum(ret)>=2:
        return True
      else:
        self.pushNewSet(filename,wordCounts)
    except Exception as e:
      self.pushNewSet(filename,wordCounts)

  def isAllRedisUp(self):
    try:
      t=[self.rd[i].ping() for i in range(self.count)]
      return all(t)
    except Exception as e:
      return False

  def mergeSetsPipeline(self,instance,key,intersection,maxTry):
    try:
      if maxTry!=0:
        p=self.rd[instance].pipeline()
        for z in intersection:
          p.watch(z)
        p.multi()
        p.zunionstore(key,intersection,aggregate='SUM')
        for z in intersection:
          if z!=key:
            p.delete(z)
        p.execute()
        return 1 
      return 0
    except:
      self.mergeSetsPipeline(instance,key,intersection,maxTry-1)
      return 0

  def mergeSets(self):
    try:
      allKeys=self.getAllKeys()
      setList=[]
      for keys in allKeys:
        setList.append(set(keys))
      
      inter=set.intersection(*setList)
      
      if len(inter)>1:
        inter=list(inter)
        key=inter[0]
        ret=[]
        if self.isAllRedisUp():
          for i in range(self.count):
            ret.append(self.mergeSetsPipeline(i,key,inter,3))
          if sum(ret)==self.count:
            return
          else:
            self.mergeSets()

    except Exception as e:
      self.mergeSets()
      pass

  def whichRedisIsDown(self,keysList):
    mink=min(keysList)
    index=keysList.index(mink)
    if len(set(keysList)) == 1:
      return -1
    return index

  def mergeRedisAfterHeal(self):
    try:
      allKeys=self.getAllKeys()
      
      setList=[]
      keysLen=[]
      for keys in allKeys:
        setList.append(set(keys))
        keysLen.append(len(keys))
      inter=set.union(*setList).difference(set.intersection(*setList))
      
      downIndex=self.whichRedisIsDown(keysLen)
      if downIndex==-1:
        return
      if len(inter)>0:
        inter=list(inter)
        if self.isAllRedisUp():
          p=self.rd[downIndex].pipeline()
          for z in inter:
            p.watch(z)
          p.multi()
          for z in inter:
            l=dict(self.rd[(downIndex+1)%self.count].zrange(z,0,-1,withscores=True))
            k=dict(self.rd[(downIndex+2)%self.count].zrange(z,0,-1,withscores=True))
            if l:
              p.zadd(z,l)
            else:
              p.zadd(z,k)
          p.execute()
      self.mergeRedisAfterHeal()
      self.mergeSets()

    except Exception as e:
      self.mergeRedisAfterHeal()
      pass

  def get_top_words(self, n: int,
                    repair: bool = False) -> List[Tuple[str, int]]:
    if repair:
      self.mergeRedisAfterHeal()

    words=[]
    allKeys=self.getAllKeys()
    setList=[]
    keysLen=[]

    for keys in allKeys:
      setList.append(set(keys))
      keysLen.append(len(keys))
    downIndex=self.whichRedisIsDown(keysLen)
    upindex=(downIndex+1)%self.count
    topkey=self.getTopKeyByRedis(self.rd[upindex])
    words=self.rd[(downIndex+1)%self.count].zrevrange(topkey,0,n,True)
    return words