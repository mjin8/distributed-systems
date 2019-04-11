import os
import sys
import math

# takes key string and returns hash key
def hash_key(key):
    hkey = 0
    for i in key:
        hkey += ord(i)
    return hkey
# takes key, value, and number of shards in system
# returns shard ID for key, value
def assign_sid(key, _shards):
    hkey = hash_key(key)
    sid = hkey % _shards
    return sid
# takes shard number, and shards dictionary {sid: [ip1, ip2...]}
def select_ip(sid, shards):
    return shards[sid][0]

def insert(key, value, kvs_dict, shards, _shards):
    sid = assign_sid(key, _shards)
    ip = select_ip(sid, shards)
    kvs_dict[ip][key] = value
    return
