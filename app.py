from flask import Flask, make_response, request, jsonify
import json
import os
import socket
import requests
import math
import kvs_hash as kvsh

app = Flask(__name__)

VIEW = os.getenv('VIEW')
IP_PORT = os.getenv('IP_PORT')
S = os.getenv('S') # number of shards
S = int(S)
addresses = {} # addresses in VIEW
shards = {} # {sid1: [ip1, ip2], sid2: [ip3, ip4]...}

# initialize addresses
for IP in VIEW.split(','):
    if (VIEW):
        addresses[IP] = VIEW.split(',')
        addresses[IP] = sorted(addresses[IP])

class vector_clock(object):
    def __init__(self, vector=None):
        if isinstance(vector, list):
            self.vector = {key: val for key, val in enumerate(vector)}
        elif vector is None:
            self.vector = {}
        else:
            self.vector = dict(vector)
    
    def get_clock(self):
        return dict(self.vector)

    def increment(self, key_idx):
        # increment vector clock at index 
        # resize vector clock if length not sufficient
        new_clock = self.__class__(self.vector)
        new_clock.vector[key_idx] = new_clock.vector.get(key_idx, 0) + 1
        return new_clock
    
    def merge(self, clock):
        merged_clock = {}
        c1, c2 = self.vector, clock.vector
        for key in set(c1.keys()).union(c2.keys()):
            # using get(key,0) handles case where there's no value for key
            # returns 0 if no key
            merged_clock[key] = max(c1.get(key,0), c2.get(key,0))
        #merged_clock[key_idx] = merged_clock.get(key_idx,0) + 1
        return self.__class__(merged_clock)

    # checks if self is after clock
    def _isafter(self,clock):
        # clock must have values for each slot  at least equal or greater than self
        # vector clocks cannot be equal
        if self.vector == clock.vector: return False
        if set(clock.vector.keys()).difference(self.vector.keys()):return False
        
        clock_zeros = True
        for key, val in clock.vector.items():
            if val != 0: clock_zeros = False
        self_zeros = True
        for key, val in self.vector.items():
            if val != 0: self_zeros = False
            if clock.vector.get(key,0) > val: return False
        if clock_zeros and self_zeros: return False
        return True
    
    # given list of clocks, check that self is after each clock in list
    # returns a list of vector clocks that are after the vector clock of the main process
    def isafter(self, clocks):
        ls = []
        for c in clocks:
            if self._isafter(c):
                ls.append(c)
        return ls

def max_vector(items):
    result = items[0]
    m = vector_clock({0:0})
    for i in items:
        v = i[0]
        c = vector_clock(v)
        if c._isafter(m):
            m = c
            result = i
    return result

# setup shards and key-value dictionaries
def setup(n, sids, kvs_dict, shards, addresses):
    kvset = {}
    for s in shards.keys():
        for ip in shards[s]:
            kvset = dict(list(kvset.items()) + list(kvs_dict[ip].items()))
            kvs_dict[ip] = {}
    #return make_response(jsonify({"kvset":json.dumps(kvset)}), 200)
    # check that there are enough nodes left for shards in case of delete node
    if (len(addresses) / n) < 2:
        old = n
        n = math.floor(len(addresses) / 2)
        for i in range (n-1, old): # range is [a, b) but index begins at 0 so lower range is n-1
            del shards[i]
    # adjust shards and the nodes in the shards
    for s in range(n):
        shards[s] = []
        sids += [str(s)]
    s = 0
    for IP in addresses:
        if s == n: s = 0
        shards[s] += [IP]
        s += 1
    # rehash keys
    for key, value in kvset.items():
        sid = kvsh.assign_sid(key, n)
        ip = shards[sid][0]
        kvs_dict[ip][key] = value
    return

v = {} # dictionary of items in vector clock
vclocks = {} # dictionary of vector clocks of each process
vclock = vector_clock({}) # a vector clock

for i in VIEW.split(','):
    for j in VIEW.split(','):
        v[j] = 0
    vclock = vector_clock(v)
    vclocks[i] = vclock

kvs_dict = {}
for ip in VIEW.split(','):
    kvs_dict[ip] = {}
    
setup(S, [], kvs_dict, shards, addresses)

@app.route("/keyValue-store/<key>", methods = ['GET', 'PUT', 'DELETE'])
def kvs(key):
    
    sid = kvsh.assign_sid(key, S)
    # if there is no sid assigned, then there must have been an error
    ips = list(set(shards[sid]) - set([IP_PORT]))
    # there must always be 2 or more nodes per shard
    # must perform request from different ip because can't request from self
    # once it returns a response, the loop should exist anyway so no redundancy
    # if not then just use the first item in ips
    for ip in ips:
        if request.method == 'GET':
            resp = requests.get('http://' + ip + '/keyValue-store/shard/%s'%(key), data = {'shard_id':sid})
            d = resp.json()
            payload = d['payload']
            if resp.status_code == 200:
                val = d['val']
                sid = d['owner']
                resp = make_response(jsonify({'owner':sid,'result': 'Success', 'val': val, 'payload': payload}), 200)
            elif resp.status_code == 404: # if 404 then not valid response
                resp = make_response(jsonify({'result': 'Error', 'error':'Key does not exist', 'payload': payload}), 404)
            # if response is 200 then return that response...so I have to make a response every time...can't just forward it
            resp.headers['Content-Type'] = 'application/json'
            return resp

        if request.method == 'PUT':
            val = request.values.get('val')
            payload = request.values.get('payload')
            resp = requests.put('http://' + ip + '/keyValue-store/shard/%s'%(key), data = {'val':val, 'payload':payload, 'shard_id':sid})
            d = resp.json()
            payload = d['payload']
            if resp.status_code == 200:
                resp =  make_response(jsonify({'replaced': 0, 'msg':'Added successfully', 'payload': payload}), 200)
            elif resp.status_code == 201:
                resp = make_response(jsonify({'replaced': 1, 'msg':'Updated successfully', 'payload': payload}), 201)
            elif resp.status_code == 400:
                resp = make_response(jsonify({'result':'Error', 'msg':'Payload out of date', 'payload': payload}), 400)
            resp.headers['Content-Type'] = 'application/json'
            return resp

        if request.method == 'DELETE':
            resp = requests.delete('http://' + ip + '/keyValue-store/shard/%s'%(key), data = {'shard_id':sid})
            d = resp.json()
            payload = d['payload']
            if resp.status_code == 200:
                resp = make_response(jsonify({'result':'Success', 'msg':'Key deleted','payload': payload}),200)
            elif resp.status_code == 404:
                resp = make_response(jsonify({'result':'Error', 'msg':'Key does not exist','payload': payload}),404)
            else:
                resp = make_response(jsonify({'result':'what', 'msg':'delete doesnt work', 'payload':json.dumps({})}),200)
            resp.headers['Content-Type'] = 'application/json'
            return resp

    # code should not reach here. else there is not enough nodes in the shard 

@app.route("/keyValue-store/shard/<key>", methods = ['GET','PUT', 'DELETE'])
def shard_kvs(key):
    sid = request.values.get('shard_id')
    if len(key) > 200:
        resp = make_response(jsonify({'result': 'Failure', 'msg':'Key not valid'}), 200)
        return resp
    
    if request.method == 'GET':
        
        if key in kvs_dict[IP_PORT]:
            vclocks[IP_PORT] = vclocks[IP_PORT].increment(IP_PORT)
            resp = make_response(jsonify({'owner':sid,'IP_PORT':IP_PORT,'result': 'Success', 'val': kvs_dict[IP_PORT][key], 'payload':json.dumps(vclocks[IP_PORT].get_clock())}), 200)
        else: # broadcast to others the current ip does not have the message
            resp = make_response(jsonify({'result': 'Error', 'error':'Key does not exist', 'payload':json.dumps(vclocks[IP_PORT].get_clock())}), 404)
        resp.headers['Content-Type'] = 'application/json'
        data = {} 
        for IP in shards[int(sid)]:
            if IP != IP_PORT:
                url = 'http://' + IP + '/gossip/' + key
                r = requests.get(url)
                d = r.json()
                if d['result'] == 'Success':
                    payload = json.loads(d['payload'])
                    data[IP] = (payload,d['val'],IP)
        #compare vector clocks and then take the max vector clock
        #then write value to all
        # then return value
        if data:
            max_item = max_vector([i for i in data.values()]) # does the max_vector work
            max_vc = max_item[0]
            max_val = max_item[1]
            max_ip = max_item[2]
            
            # if my vector clock is after the max vector clock from other processes
            # tehen set max items to my data
            # else update my data
            if vclocks[IP_PORT]._isafter(vector_clock(max_vc)):
                max_vc = vclocks[IP_PORT].get_clock()
                max_val = kvs_dict[IP_PORT][key]
                max_ip = IP_PORT
            else:    
                vclocks[IP_PORT] = vector_clock(max_vc)
                kvs_dict[IP_PORT] = max_val
            
            # propagate to other nodes
            for IP in shards[int(sid)]:
                if IP != max_ip and IP != IP_PORT:
                    url = 'http://' + IP + '/gossip/' + key
                    requests.put(url, data = {'payload':json.dumps(max_vc), 'val':max_val})
                resp = make_response(jsonify({'owner':sid,'IP_PORT':IP_PORT,'result': 'Success', 'val': max_val, 'payload':json.dumps(max_vc)}), 200)
        return resp

    '''
    If incoming request has key that exists in dictionary, update if the vector clock is greater otherwise block
    If incoming request does not have key that exists in dictionary, write
    Propagate write to everyone else in shard
    '''
    if request.method == 'PUT': 
        # check that value is less than 1 MB 
        # check if vector clock is greater than own vector clock
        # if not then return error message
        sid = request.values.get('shard_id')
        d = request.values.get('payload')
        v = json.loads(d)
        val = request.values.get('val')
        if vclocks[IP_PORT]._isafter(vector_clock(v)):
            if not v:
                vclocks[IP_PORT] = vclocks[IP_PORT].merge(vector_clock(v))
                vclocks[IP_PORT] = vclocks[IP_PORT].increment(IP_PORT)
                kvs_dict[IP_PORT][key] = val
                resp = make_response(jsonify({'replaced': 0, 'msg':'Added successfully', 'payload':json.dumps(vclocks[IP_PORT].get_clock())}), 200)
            else:
                resp = make_response(jsonify({'result':'Error', 'msg':'Payload out of date','payload': json.dumps(vclocks[IP_PORT].get_clock())}), 400)
            resp.headers['Content-Type'] = 'application/json'
            return resp
        
        if key in kvs_dict[IP_PORT]:
            x = request.values.get('msg')
            # check that their vector clock is after our vector clock
            vc_vector = request.values.get('payload')
            d = json.loads(vc_vector) 
            vc = vector_clock(d)
            if vclocks[IP_PORT]._isafter(vc):
                resp = make_response(jsonify({'result':'Error', 'msg':'Payload out of date', 'payload': json.dumps(vclocks[IP_PORT].get_clock())}), 400)
            else:
                vclocks[IP_PORT] = vclocks[IP_PORT].merge(vc)
                vclocks[IP_PORT] = vclocks[IP_PORT].increment(IP_PORT)
                kvs_dict[IP_PORT][key] = val
                resp = make_response(jsonify({'replaced': 1, 'msg':'Updated successfully', 'payload':json.dumps(vclocks[IP_PORT].get_clock())}), 201)
        else:
            kvs_dict[IP_PORT][key] = val
            vclocks[IP_PORT] = vclocks[IP_PORT].merge(vector_clock(v))
            vclocks[IP_PORT] = vclocks[IP_PORT].increment(IP_PORT)
            resp = make_response(jsonify({'replaced': 0, 'msg':'Added successfully', 'payload':json.dumps(vclocks[IP_PORT].get_clock())}), 200)
            resp.headers['Content-Type'] = 'application/json'
        
        for IP in shards[int(sid)]:
            if IP != IP_PORT:
                url = 'http://' + IP + '/gossip/' + key
                r = requests.put(url, data = {'payload':json.dumps(vclocks[IP_PORT].get_clock()),'val':kvs_dict[IP_PORT][key]})
        return resp

    if request.method == 'DELETE':
        if key in kvs_dict[IP_PORT]:
            #data_dict[key].pop()
            del kvs_dict[IP_PORT][key]
            for IP in shards[int(sid)]:
                if IP != IP_PORT:
                    requests.delete('http://' + IP + '/keyValue-store/shard/' + key, data = {'shard_id':sid})
            resp = make_response(jsonify({'result':'Success', 'msg':'Key deleted','payload':json.dumps(vclocks[IP_PORT].get_clock())}),200)
        else:
            resp = make_response(jsonify({'result':'Error', 'msg':'Key does not exist','payload':json.dumps(vclocks[IP_PORT].get_clock())}),404)
        resp.headers['Content-Type'] = 'application/json'
        return resp


@app.route("/gossip/<key>", methods = ['GET','PUT'])
def kvs_gossip(key):
    
    # if vector clock is greater then merge and keep new clock
    if len(key) > 200:
        resp = make_response(jsonify({'result': 'Error', 'msg':'Key not valid'}), 200)
        return resp

    if request.method == 'GET':
        #kvs_dict[IP_PORT][key] = 'hi'
        if key in kvs_dict[IP_PORT]:
            resp = make_response(jsonify({'IP_PORT':IP_PORT,'result': 'Success', 'val': kvs_dict[IP_PORT][key], 'payload':json.dumps(vclocks[IP_PORT].get_clock())}), 200)
        else: # broadcast to others the current ip does not have the message
            resp = make_response(jsonify({'result': 'Error', 'error':'Key does not exist', 'payload':json.dumps(vclocks[IP_PORT].get_clock())}), 404)
        resp.headers['Content-Type'] = 'application/json'
        return resp

    if request.method == 'PUT': 
        # check that value is less than 1 MB 
        # check if our own vector clock is more updated than the incoming one
        # if so then block
        d = request.values.get('payload')
        v = json.loads(d)
        #return make_response(jsonify({'v':v, 'd':d}),600)
        if vclocks[IP_PORT]._isafter(vector_clock(v)):
            resp = make_response(jsonify({'result':'Error', 'msg':'Payload out of date', 'payload': json.dumps(vclocks[IP_PORT].get_clock())}), 400)
            resp.headers['Content-Type'] = 'application/json'
            return resp
        # merge clocks
        vclocks[IP_PORT] = vclocks[IP_PORT].merge(vector_clock(v))
        kvs_dict[IP_PORT][key] = request.values.get('val')
        if key in kvs_dict:
            resp = make_response(jsonify({'replaced': 1, 'val':kvs_dict[IP_PORT][key],'msg':'Updated successfully', 'payload':json.dumps(vclocks[IP_PORT].get_clock())}), 201)
        else:
            resp = make_response(jsonify({'replaced': 0, 'val':kvs_dict[IP_PORT][key],'msg':'Added successfully', 'payload':json.dumps(vclocks[IP_PORT].get_clock())}), 200)
        resp.headers['Content-Type'] = 'application/json'
        return resp

@app.route("/keyValue-store/search/<key>", methods = ['GET'])
def search(key):
    sid = kvsh.assign_sid(key, S)
    ips = list(set(shards[sid]) - set([IP_PORT]))
    for ip in ips:
        if request.method == 'GET':
            resp = requests.get('http://' + ip + '/keyValue-store/search/shard/' + key)
            d = resp.json()
            payload = d['payload']
            if resp.status_code == 200:
                if d['result'] == 'Error':
                    resp = make_response(jsonify({'payload': payload,'result': 'Error', 'msg':'Key not valid'}), 200)
                elif d['result'] == 'Success':
                    b = d['isExists']
                    resp = make_response(jsonify({'payload':payload,'result':'Success', 'isExists':b}),200)
                resp.headers['Content-Type'] = 'application/json'
                return resp

@app.route("/keyValue-store/search/shard/<key>", methods = ['GET'])
def shard_search(key):
    
    if len(key) > 200:
        resp = make_response(jsonify({'payload':json.dumps(vclocks[IP_PORT].get_clock()),'result': 'Error', 'msg':'Key not valid'}), 200)
        return resp
    if key in kvs_dict[IP_PORT]:
        resp = make_response(jsonify({'payload':json.dumps(vclocks[IP_PORT].get_clock()),'result':'Success', 'isExists':True}),200)
    else: 
        b = False
        for IP in addresses[IP_PORT]:
            if IP != IP_PORT:
                url = 'http://' + IP + '/gossip/' + key
                r = requests.get(url)
                d = r.json()
                if d['result'] == 'Success':
                    b = True
        resp = make_response(jsonify({'payload':json.dumps(vclocks[IP_PORT].get_clock()),'result':'Success', 'isExists':b}),200)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route("/view", methods=['GET', 'PUT', 'DELETE'])
def view():
    # global numNodes
    global VIEW
    global addresses

    if request.method == 'GET':
        resp = make_response(jsonify({'view': ','.join(addresses[IP_PORT])}), 200)
        resp.headers['Content-Type'] = 'application/json'
        return resp
    
    if request.method == 'PUT':
        addressToUpdate = request.values.get('ip_port')
        if addressToUpdate in addresses[IP_PORT]:
            resp = make_response(jsonify({
                'result': 'Error', 
                'msg':addressToUpdate + ' is already in view'
                }), 404)
        else: # why str(addressToUpdate) when you use 'if addressToUpdate in addresses[IP_PORT]' for comparison
            addresses[IP_PORT].append(addressToUpdate)
            # update view for all nodes
            addresses[addressToUpdate] = addresses[IP_PORT]
            kvs_dict[addressToUpdate] = kvs_dict[IP_PORT] # create new kvs dictionary for new IP
            vclocks[addressToUpdate] = vclocks[IP_PORT] # create new vector clock dictionary for new IP
            for IP in addresses:
                if IP != IP_PORT:
                    # dont need another route bc if the view already exists then it will be routed to the if statement above and terminate
                    requests.put('http://' + IP + '/view', data = {'ip_port':addressToUpdate})
            # rehash everything
            setup(len(shards.keys()), [], kvs_dict, shards, addresses)
            resp = make_response(jsonify({
                'result': 'Success',
                'msg': 'Successfully added '+ addressToUpdate +' to view'
                }), 200)
        return resp

    if request.method == 'DELETE':
        delete_ip = str(request.values.get('ip_port'))
        if delete_ip in addresses[IP_PORT]:
            # remove from list
            for s in shards.keys():
                if delete_ip in shards[s]:
                    shards[s].remove(delete_ip)
            addresses[IP_PORT].remove(delete_ip)
            del addresses[delete_ip]
            del kvs_dict[delete_ip]
            del vclocks[delete_ip]
            for IP in addresses[IP_PORT]:
                if IP != IP_PORT:
                    requests.delete('http://' + IP + '/view', data = {'ip_port':delete_ip})
            #setup(len(shards.keys()), [], kvs_dict, shards, addresses)
            ### so this works here but not when I put it in a function...?
            kvset = {}
            for s in shards.keys():
                for ip in shards[s]:
                    kvset = dict(list(kvset.items()) + list(kvs_dict[ip].items()))
                    kvs_dict[ip] = {}
            #return make_response(jsonify({"kvset":json.dumps(kvset)}), 200)
            # check that there are enough nodes left for shards in case of delete node
            n = len(shards.keys())
            if (len(addresses) / n) < 2:
                old = n
                if len(addresses) < 2: # if there's one address then only 1 shard is needed
                    if n > 1:
                        for i in range (1, old):
                            del shards[i]
                            n -= 1
                else:
                    n = math.floor(len(addresses) / 2)
                    for i in range (n-1, old): # range is [a, b) but index begins at 0 so lower range is n-1
                        del shards[i]
            # adjust shards and the nodes in the shards
            sids = []
            for s in range(n):
                shards[s] = []
                sids += [str(s)]
            s = 0
            for IP in addresses:
                if s == n: s = 0
                shards[s] += [IP]
                s += 1
            # rehash keys
            for key, value in kvset.items():
                sid = kvsh.assign_sid(key, n)
                ip = shards[sid][0]
                kvs_dict[ip][key] = value

            ###
            # return response message
            resp = make_response(jsonify({'result' : 'Success','msg' : 'Successfully removed ' + delete_ip + ' from view'}), 200)
        else:
            resp = make_response(jsonify({'result' : 'Error','msg' : delete_ip + ' is not in current view'}), 404)
        return resp

@app.route("/shard/my_id", methods=['GET'])
def shard_id():
    sid = None
    for s in shards.keys():
        if IP_PORT in shards[s]:    
            sid = s
            break
    resp = make_response(jsonify({'id': sid}), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route("/shard/all_ids", methods=['GET'])
def shard_ids():
    sids = []
    for s in shards.keys():
        sids.append(str(s))
    resp = make_response(jsonify({'result':'Success','shard_ids': (',').join(sids)}), 200)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route("/shard/members/<shard_id>", methods=['GET'])
def shard_ips(shard_id):
    if int(shard_id) in shards.keys():
        sips = [] # shard ips
        for ip in shards[int(shard_id)]:
            sips += [ip]
        resp = make_response(jsonify({'result':'Success','members': (',').join(sips)}), 200)
    else:
        resp = make_response(jsonify({'result':'Error', 'msg':'No shard with id %s'%shard_id}),404)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route("/shard/count/<shard_id>", methods=['GET'])
def shard_count(shard_id):
    kvset = [] # key-value set
    for ip in shards[int(shard_id)]: # join all the dictionaries in the shard
        kvset = list(set(kvset) | set(kvs_dict[ip].keys()))
    count = len(kvset)
    resp = make_response(jsonify({'result':'Success','Count':count}),200)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route("/shard/changeShardNumber", methods=['PUT'])
def csn():
    n = request.values.get('num')
    n = int(n)
    nodes = len(addresses)
    # not accounting for n = 0
    if nodes < n:
        resp = make_response(jsonify({'result':'Error','msg':'Not enough nodes for %s shards'%n}),400)
    elif (nodes / n) <  2:
        resp = make_response(jsonify({'result':'Error', 'msg':'Not enough nodes. %s shards result in nonfault tolerant shard'%n}),400)
    else:
        # maybe make reconfiguration / set up a function
        sids = []
        setup(n, sids, kvs_dict, shards, addresses)
        resp = make_response(jsonify({'result':'Success', 'shard_ids': (',').join(sids)}),200)
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.errorhandler(405)
def not_supported(error=None):
    message = {}
    resp = Response(status=405)
    resp.headers['Content-Type'] = 'application/json'
    return resp

if __name__ == "__main__":
    app.run(
        host='0.0.0.0', 
        port=8080, 
        # debug= True
        )
