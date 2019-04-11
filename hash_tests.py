import os
import sys
import kvs_hash as kvsh

key = "hello"
value = "world"
kvs_dict = {}
ips = ['111.111.111', '222.222.222', '333.333.333', '444.444.444','555.555.555','666.666.666']
ips = ['111.111', '222.222']
for ip in ips:
    kvs_dict[ip] = {}

_shards = 1
shards = {}

for s in range(_shards):
    shards[s] = []

s = 0
for ip in ips:
    if s == _shards: s = 0
    shards[s] += [ip]
    s += 1

hkey = kvsh.hash_key(key)
sid = kvsh.assign_sid(key, _shards)
ip = kvsh.select_ip(sid, shards)
# def insert(key, value, kvs_dict, shards, _shards):
kvsh.insert(key, value, kvs_dict, shards, _shards)

print ('hkey:', hkey)
print ('sid', sid)
print ('kvs_dict', kvs_dict.items())
print ('shards', shards.items())
print ('ip', ip)
