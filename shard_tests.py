from flask import request, jsonify
import json
import os
import sys
import requests

def getShardID():
    print('GET: http://localhost:8082/shard/my_id')
    res = requests.get( 'http://localhost:8082/shard/my_id')
    d = res.json()
    print( 'shard', d )

def getShardIDs():
    print('GET: http://localhost:8082/shard/my_ids')
    res = requests.get( 'http://localhost:8082/shard/my_ids')
    d = res.json()
    print( 'shards', d )

def getShardIPs(SID):
    print('GET: http://localhost:8082/shard/members/%s'%SID)
    res = requests.get( 'http://localhost:8082/shard/members/%s'%SID)
    d = res.json()
    print( 'shard members', d )

def getShardKeyCount(SID):
    print('GET: http://localhost:8082/shard/count/%s'%SID)
    res = requests.get( 'http://localhost:8082/shard/count/%s'%SID)
    d = res.json()
    print( 'shard key count', d )

def changeShardNumber(shards):
    print ('PUT: http://localhost:8082/shard/changeShardNumber, data = {shards: %s}'%shards)
    res = requests.put('http://localhost:8082/shard/changeShardNumber', data={'num':shards})
    d = res.json()
    print ( 'changedShardNumber', d )

getShardID()
getShardIDs()
SID = 0
getShardIPs(SID)
getShardKeyCount(SID)

shards = 2 
changeShardNumber(shards)
getShardID()
getShardIDs()
getShardIPs(SID)
getShardKeyCount(SID)
# need more tests for multiple shards
