from flask import request, jsonify
import json
import os
import sys
import requests
def test_a_put_nonexistent_key():
    #payload = json.dumps({'192.168.0.2:8080':1,'192.168.0.3:8080':1})
    payload = json.dumps({})
    res = requests.put('http://localhost:8082'  + '/keyValue-store/subject', data = {'payload':json.dumps(payload),'val': 'Distributed System'})
    d = res.json()
    print ("res", res, "res.json()", res.json())
    payload = d['payload']
    res = requests.put('http://localhost:8083'  + '/keyValue-store/subject2', data = {'payload':json.dumps(payload),'val': 'Distributed System'})
    d = res.json()
    print ("res", res, "res.json()", res.json())

def test_b_put_existent_key():
    payload = json.dumps({'192.168.0.2:8080':2,'192.168.0.3:8080':2})
    res = requests.put('http://localhost:8082'  + '/keyValue-store/subject', data = {'payload':payload,'val': 'Distributed Systems 2'})
    d = res.json()
    print ("res", res, "res.json()", res.json())

def test_c_gossip():
    payload = json.dumps({'192.168.0.2:8080':3,'192.168.0.3:8080':3})
    res = requests.put('http://localhost:8082'  + '/gossip/subject', data = {'payload':payload,'val': 'Distributed Systems 3'})
    d = res.json()
    print ("res", res, "res.json()", res.json())
# also check what happens when you put subject onto another node. it should return replaced
def storeKeyValue(payload):
    res = requests.put( 'http://localhost:8082/keyValue-store/subject', data={'val':'value1', 'payload': payload}, timeout=5 )
    d = res.json()
    print ("new key", res, "res.json()", res.json())
    payload = d['payload']
    res = requests.put( 'http://localhost:8083/keyValue-store/subject1', data={'val':'value1', 'payload': payload}, timeout=5 )
    d = res.json()
    print ("new key", res, "res.json()", res.json())
    payload = d['payload']
    res = requests.put( 'http://localhost:8082/keyValue-store/subject', data={'val':'value2', 'payload': payload}, timeout=5 )
    d = res.json()
    print ("update key", res, "res.json()", res.json())
    return d['payload']
    
    return
def checkKey(payload):
    #payload = json.dumps({'192.168.0.2:8080':1,'192.168.0.3:8080':0})
    res = requests.get( 'http://localhost:8082/keyValue-store/search/subject', data={'payload': payload} )
    d = res.json()
    print ("search key that does not exist",res,"res.json()",d)
    payload = d['payload']
    res = requests.get( 'http://localhost:8082/keyValue-store/search/subject', data={'payload': payload} )
    d = res.json()
    print ("search key that exists",res,"res.json()",d)
    payload = d['payload']
    res = requests.get( 'http://localhost:8082/keyValue-store/search/doesnotexist', data={'payload': payload} )
    d = res.json()
    print ("search key that does not exists",res,"res.json()",d)
    return d['payload']

def getKeyValue(payload):
    #payload = {'192.168.0.2:8080':5,'192.168.0.3:8080':5}
    res = requests.get( 'http://localhost:8082/keyValue-store/subject', data={'payload': payload} )
    d = res.json()
    print ("get key value that exists", res, "res.json()", res.json())
    payload = d['payload']
    res = requests.get( 'http://localhost:8083/keyValue-store/subject', data={'payload': payload} )
    d = res.json()
    print ("get value from not primary", res, "res.json()", d)
    return d['payload']

def addNode(ipPort, newAddress):
    print('PUT: http://localhost:8082/view')
    return requests.put( 'http://localhost:8082/view', data={'ip_port':newAddress} )
  
def removeNode(ipPort, oldAddress):
    print('DELETE: http://localhost:8082/view')
    return requests.delete( 'http://localhost:8082/view', data={'ip_port':oldAddress} )

def viewNetwork(ipPort):
    print('GET: http://localhost:8082/view')
    res = requests.get( 'http://localhost:8082/view')
    d = res.json()
    print( 'view', d )

#test_a_put_nonexistent_key()
#test_b_put_existent_key()
#test_c_gossip()
payload = json.dumps({})
payload = checkKey(payload)
payload = storeKeyValue(payload)
payload = checkKey(payload)
getKeyValue(payload)
#ipPort = "localhost:8082"
#newAddress = "192.168.0.2:8080"
#oldAddress = "192.168.0.2:8080"
#addNode(ipPort, newAddress)
#addNode(ipPort, newAddress)
#viewNetwork(ipPort)
#removeNode(ipPort, oldAddress)
#viewNetwork(ipPort)
