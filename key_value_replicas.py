import flask
from flask import request, jsonify, redirect, Response
import json
import urllib.parse
import re 
import os
import requests
import socket
import urllib.request
import ast

# Replica's vector clock position incremented every k-v put/delete, and updated by startup or receiving broadcast

# To Do:
# If a request has a newer vector clock, replica restarts by copying another replica. not best solution
# Should maybe deny requests that have an old vector clock?
# Some code would be cleaner split into functions

app = flask.Flask(__name__)
#app.config["DEBUG"] = True

view = os.getenv('VIEW')
replica_address = os.getenv('SOCKET_ADDRESS')
shard_count = os.getenv('SHARD_COUNT')

view_list = view.split(",")

key_value_store = {}
vector_clock = {}


for v in view_list:
    vector_clock[v] = 0

### look at bottom for more global variables

# Called right before app.run and if vector_clock is outdated
def startup():
    global key_value_store
    global vector_clock
    global view_list
    global replica_address

    other_view_list = [viewip for viewip in view_list if viewip != replica_address]
    #broadcast(other_view_list, {'socket-address': replica_address}, '/key-value-store-view/', "put")
    #broadcast(other_view_list, None, '/key-value-store-copy/', "get")
    for v in other_view_list:
        try:
            response = requests.put('http://' + v + '/key-value-store-view/', 
                        json={'socket-address': replica_address},
                        timeout=1.0)
        except:
            continue
    for v in other_view_list:
        try:
            response = requests.get('http://' + v + '/key-value-store-copy/',
                        timeout=1.0)
            jr = response.json()
            key_value_store = json.loads(jr['key-value-store'])
            vector_clock = json.loads(jr['causal-metadata'])
            break
        except:
            continue


### function for getting shard_ids
def shard_count_to_list(shard_count):
    shard_list = []
    for i in range(1, int(shard_count) + 1):
        shard_list.append(i)
    return shard_list

### function for creating/ resharding shards
def process_shard(shard_ids, view_list, shard_dict):
    nodes_per_shard = len(view_list)//len(shard_ids)
    
    nodes_left = len(view_list)
    nodes_taken = 0

    for i in shard_ids:
        shard_dict[i] = [view_list[x + nodes_taken] for x in range(nodes_per_shard)]
        nodes_taken += nodes_per_shard 
        if (nodes_left - nodes_taken < nodes_per_shard and nodes_left - nodes_taken != 0) or (nodes_left - nodes_taken < 2 and nodes_left - nodes_taken != 0):
            shard_dict[i].append(view_list[-1])

    print(shard_dict)
    return shard_dict

### used in add member endpoint for the new member to get a shard dict
@app.route('/get_shard_dict', methods = ['GET', 'PUT', 'DELETE'])
def get_shard_dict():
    global shard_dict
    return shard_dict

### for replica to get shard ids in the store (initialized global variable that takes care of this)
@app.route('/key-value-store-shard/shard-ids', methods = ['GET', 'PUT', 'DELETE'])
def shard_ids_endpoint():
    global shard_ids
    if request.method == 'GET':
        return {"message":"Shard IDs retrieved successfully","shard-ids": shard_ids}

### find the shard a node is part of
@app.route('/key-value-store-shard/node-shard-id', methods = ['GET', 'PUT', 'DELETE'])
def node_shard_id():
    global replica_address
    global shard_dict
    if request.method == 'GET':
        for i in shard_dict:
            if replica_address in shard_dict[i]:
                return {"message":"Shard ID of the node retrieved successfully","shard-id": i}

### find the members of a shard given the shard id
@app.route('/key-value-store-shard/shard-id-members/<shard_id>', methods = ['GET', 'PUT', 'DELETE'])
def shard_members_endpoint(shard_id = None):
    global shard_dict
    if request.method == 'GET':
        return {"message":"Members of shard ID retrieved successfully", "shard-id-members": shard_dict[int(shard_id)]}

### used for key count
@app.route('/keys-for-shard', methods = ['GET'])
def num_keys_for_shard():
    global key_value_store
    if request.method == 'GET':
        print(key_value_store)
        return {"num-keys":str(len(key_value_store))}

### Still need to implement, but we must implement it after the put works
### function for getting key count in the shard
@app.route('/key-value-store-shard/shard-id-key-count/<shard_id>', methods = ['GET', 'PUT', 'DELETE'])
def shard_count_endpoint(shard_id = None):
    global shard_dict
    global key_value_store
    if request.method == 'GET':
        #shard = shard_dict[int(shard_id)]
        #data = broadcast(shard, [], '/keys-for-shard', "get_keys")
        #data = [int(i) for i in data]
        #print(key_value_store)
        #print(data)
        #key_count = sum(data)
        return {"message":"Key count of shard ID retrieved successfully","shard-id-key-count": str(len(key_value_store))}


def get_all_keys(data):
    data = set(data)
    total_kvs = {}
    for i in data:
        dic = json.loads(i)
        for j in dic:
            total_kvs[j] = dic[j]
    return total_kvs

@app.route('/delete-all', methods = ['GET', 'PUT', 'DELETE'])
def delete_whole_kvs():
    global key_value_store
    if request.method == 'GET':
        key_value_store = {}
        return "deleted key_value_store"

### we have to possibly reshard
### this function reshards for the existing nodes 
### we get the new shard value from the client
@app.route('/key-value-store-shard/reshard', methods = ['GET', 'PUT', 'DELETE'])
def reshard_endpoint():
    global shard_count
    global view_list
    global shard_dict
    global shard_ids
    global replica_address
    global vector_clock
    if request.method == 'PUT':
        r = json.loads(request.data) 
        shard_count_from_request = r['shard-count']
        if shard_count_from_request != '':
            if len(view_list) - int(shard_count_from_request) < 2 :
                return {"message":"Not enough nodes to provide fault-tolerance with the given shard count!"}, 400
            
            shard_count = int(shard_count_from_request)
            shard_ids = shard_count_to_list(shard_count)
            shard_dict = process_shard(shard_ids, view_list, shard_dict)
            tmp_view_list = [replica for replica in view_list if replica != replica_address]
            broadcast(tmp_view_list,{'shard-count': '', 'new-shard-count':shard_count}, '/key-value-store-shard/reshard', "put")
        else:
            shard_count_from_broadcast = int(r['new-shard-count'])
            shard_count = shard_count_from_broadcast
            shard_ids = shard_count_to_list(shard_count)
            shard_dict = process_shard(shard_ids, view_list, shard_dict)
        
        tmp_view_list = [replica for replica in view_list if replica != replica_address]
        data = broadcast(tmp_view_list, [], '/key-value-store-copy/', "get_kvs")
        total_kvs = get_all_keys(data)
        
        tmp_view_list = list(view_list)
        # deletes all keys in the kvs by setting the kvs to {}
        broadcast(tmp_view_list, None, "/delete-all", "delete_all")

        for key in total_kvs:
            tmp_view_list = list(view_list)
            broadcast(tmp_view_list, {'value': str(total_kvs[key]), "causal-metadata": json.dumps(vector_clock)}, '/key-value-store/' + str(key), "put")

        return {"message":"Resharding done successfully"}

### function used for broadcasting, works well
### recursion based
### ignore getv2 for now
### given a view list without the current replica address,
### first try to access that address, and then remove it from the view list
### after that break out of the loop and return with the shortened view list
def broadcast(tmp_view_list, params, url, method):
    for replica in tmp_view_list:
        try:
            if method == 'put':
                requests.put('http://' + replica +  url,json=params)
                tmp_view_list.remove(replica)
                break
            elif method == 'get':
                response = requests.get('http://' + replica +  url)
                return response
                tmp_view_list.remove(replica)
                break
            elif method == 'delete':
                requests.delete('http://' + replica +  url, json=params)
                tmp_view_list.remove(replica)
                break
            elif method == 'delete_all':
                requests.get('http://' + replica +  url)
                tmp_view_list.remove(replica)
                break
            elif method == 'get_metadata':
                response = requests.get('http://' + replica +  url)
                r_json = response.json()
                params.append(r_json['causal-metadata'])
                tmp_view_list.remove(replica)
                break
            elif method == 'get_kvs':
                response = requests.get('http://' + replica +  url)
                r_json = response.json()
                params.append(r_json['key-value-store'])
                tmp_view_list.remove(replica)
                break
            elif method == 'get_keys':
                response = requests.get('http://' + replica +  url)
                r_json = response.json()
                params.append(r_json['num-keys'])
                tmp_view_list.remove(replica)
                break

        except:
            tmp_view_list.remove(replica)
            break

    if tmp_view_list != []:
        return broadcast(tmp_view_list, params,url, method)
    elif method == "get_metadata" and tmp_view_list == []:
        return params
    elif method == "get_keys" and tmp_view_list == []:
        return params
    elif method == "get_kvs" and tmp_view_list == []:
        return params
    else:
        return
            
### when we add a new member/node,
### for the new node, it gets the shard_dict from anoher node
### take a look at broadcast function
@app.route('/key-value-store-shard/add-member/<shard_id>', methods = ['GET', 'PUT', 'DELETE'])
def add_member_endpoint(shard_id):
    global shard_dict
    global view_list
    global replica_address
    global shard_ids
    global view
    if request.method == 'PUT':
        if request.data != b'':
            r = json.loads(request.data) 
            new_replica_client = r['socket-address']
            if new_replica_client != '':
                shard_dict[int(shard_id)].append(new_replica_client)
                tmp_view_list = [replica for replica in view_list if replica != replica_address]
                broadcast(tmp_view_list,{'socket-address': '', 'node-added':new_replica_client}, '/key-value-store-shard/add-member/' + shard_id, "put")
                
            else:
                if shard_dict != {}:
                    new_replica_broadcast = r['node-added']
                    shard_dict[int(shard_id)].append(new_replica_broadcast)
                    
                else:
                    tmp_view_list = [replica for replica in view_list if replica != replica_address]
                    response = broadcast(tmp_view_list, None, '/get_shard_dict', "get")
                    shard_dict = response.json()
                    shard_ids = [int(i) for i in shard_dict]
                    shard_count = str(len(shard_ids))
                    shard_dict = {int(i):shard_dict[i] for i in shard_dict}
                    
            replica_added = new_replica_client
        return {"added member:": replica_added}
    

@app.route('/key-value-store-view/', methods=['GET', 'PUT', 'DELETE'])
def view_endpoint():
    global view
    global view_list
    if request.method == 'GET':
        # Check if all in view_list are up, fix list, then return view
        other_view_list = [v for v in view_list if v != replica_address]
        for v in other_view_list:
            # dummy request
            try:
                response = requests.delete('http://' + v + '/key-value-store-view/', 
                            json={'socket-address': ""},
                            timeout=1.0)
            except:
                # Timed out, replica should be removed from all lists
                view_list.remove(v)
                view = ','.join(view_list)
                del vector_clock[v]
                del_other_view_list = other_view_list.copy()
                del_other_view_list.remove(v)
                for v_del in del_other_view_list:
                    try:
                        response = requests.delete('http://' + v_del + '/key-value-store-view/', 
                                json={'socket-address': v},
                                timeout=1.0)
                    except:
                        continue
        return json.dumps({"message": "View retrieved successfully", "view": view}), 200
    elif request.method == 'PUT':
        ### put first THEN broadcast ###
        # update changes in view variable
        # broadcast changes to other replicas in view
        # idea:
        # 1) accept put request
        # 2) rebroadcast to all your peers (same group you’d send a kv-put to) who will also add that node to their view
        r = json.loads(request.data)  # eg {'socket-address':'10.10.0.4:8085'}
        adr_to_add = f"{r['socket-address']}"  # eg 10.10.0.4:8085

        # simple version assuming broadcast logic is outside
        if adr_to_add not in view_list:  # if this instance is one of the put req. from the loop so goal is to add it to this view
            view = view + f",{adr_to_add}"
            view_list.append(adr_to_add)
            vector_clock[adr_to_add] = 0
            return json.dumps({"message": "Replica added successfully to the view"}), 201
        else:
            return json.dumps({"error": "Socket address already exists in the view", "message": "Error in PUT"}), 404


    elif request.method == 'DELETE':
        ### broadcast first THEN del ###
        # 1) check if node is up (if it responds in given time)
        # 2) if node found to be unreachable, DELETE broadcast sent to all nodes in view(including itself)
        # 3) remove dead node from view
        # mechanism for a node who notices it is removed from other’s view to ask to rejoin the view?

        r = json.loads(request.data)  # eg {'socket-address':'10.10.0.4:8085'}
        adr_to_del = f"{r['socket-address']}"  # eg 10.10.0.4:8085

        # simple version asssuming broadcast logic is outside
        if adr_to_del in view_list:
            view_list.remove(adr_to_del)
            #view.replace(adr_to_del, '')
            view = ','.join(view_list)
            del vector_clock[adr_to_del]
            return json.dumps({"message": "Replica deleted successfully from the view"})
        else:
            return json.dumps({"error": "Socket address does not exist in the view", "message": "Error in DELETE"})

    
def hash_function(key, num_shards):
    hash_value = 0
    for i in key:
        hash_value += ord(i)
    return hash_value%num_shards + 1

def get_max_vc(total, data, vc):
    counter = 0
    for i in data:
        clocks = re.findall(r': \d*', vc)
        clocks = [i.replace(': ','') for i in clocks] 
        for x in clocks:
            counter += int(x)
        if counter > total:
            vc = i
            total = counter
        counter = 0
    return vc        
    

@app.route('/key-value-store/<key>', methods = ['GET', 'PUT', 'DELETE'])
def key_value_endpoint(key):
    global view
    global view_list
    global vector_clock
    global key_value_store
    global shard_ids
    global shard_dict
    global replica_address

    if request.method == 'GET':
        shard_id = hash_function(key, len(shard_ids))
        if replica_address in shard_dict[shard_id]:
            d = {'message':'Retrieved successfuly', 'causal-metadata':json.dumps(vector_clock), 'value':key_value_store[key]}
            return json.dumps(d), 200
        elif replica_address not in shard_dict[shard_id]:
            response = broadcast(shard_dict[shard_id], None, '/key-value-store-copy/', "get")
            r = response.json()
            kvs = json.loads(r['key-value-store'])
            d = {'message':'Retrieved successfuly', 'causal-metadata':json.dumps(vector_clock), 'value':kvs[key]}
            return json.dumps(d), 200
        else:
            d = {'error':'Key does not exist', 'message':'Error in GET', 'causal-metadata':json.dumps(vector_clock)}
            return json.dumps(d), 404

    elif request.method == 'PUT':
        causal_metadata = ""
        value = ""
        replica_prev = ""
        if request.data != b'':
            requested_data = json.loads(request.data)
            causal_metadata = requested_data['causal-metadata']
            value = requested_data['value']

        if causal_metadata != '':
            client_vc = json.loads(causal_metadata)
            for v in vector_clock:
                if v in client_vc and client_vc[v] > vector_clock[v]:
                    tmp_view_list = [replica for replica in view_list if replica != replica_address]
                    ls = []
                    data = broadcast(tmp_view_list, [], '/key-value-store-copy/', "get_metadata")
                    vector_clock = get_max_vc(0, data, data[0])
                    vector_clock = json.loads(vector_clock)
                    break

        # At this point, store guaranteed to change
        vector_clock[replica_address] += 1
        tmp_view_list = [replica for replica in view_list if replica != replica_address]

        d = {}
        code = 404
        if key not in key_value_store:
            d = {"message":"Added successfully"}
            code = 201
        else:
            d = {"message":"Updated successfully"}
            code = 200


        shard_id = hash_function(key, len(shard_ids))
        if replica_address in shard_dict[shard_id]:
            key_value_store[key] = value
            shard_view_list = [i for i in shard_dict[shard_id] if i != replica_address]
            broadcast(shard_view_list, {'value': value, "causal-metadata": json.dumps(vector_clock), "shard-id": shard_id}, '/key-value-store-internal/' + key, "put")
        else:
            shard_view_list = [i for i in shard_dict[shard_id]]
            broadcast(shard_view_list, {'value': value, "causal-metadata": json.dumps(vector_clock), "shard-id": shard_id}, '/key-value-store-internal/' + key, "put")
            
        d['causal-metadata'] = json.dumps(vector_clock)
        d["shard-id"] = shard_id
        return json.dumps(d), code
        
    elif request.method == 'DELETE':
        causal_metadata = ""
        if request.data != b'':
            requested_data = json.loads(request.data)
            causal_metadata = requested_data['causal-metadata']

        # If our vector clock is old, update by getting values from other replicas
        # Right now just pulls all values from first replica in list
        if causal_metadata != '':
            client_vc = json.loads(causal_metadata)
            for v in vector_clock:
                if v in client_vc and client_vc[v] > vector_clock[v]:
                    tmp_view_list = [replica for replica in view_list if replica != replica_address]
                    ls = []
                    data = broadcast(tmp_view_list, [], '/key-value-store-copy/', "get_metadata")
                    vector_clock = get_max_vc(0, data, data[0])
                    vector_clock = json.loads(vector_clock)
                    break
        
        # At this point, store guaranteed to change
        vector_clock[replica_address] += 1

        shard_id = hash_function(key, len(shard_ids))
        if replica_address in shard_dict[shard_id]:
            try:    
                del key_value_store[key]
            except:
                d = {"error":"Key does not exist", "message":"Error in DELETE", 'causal_metadata':json.dumps(vector_clock)}
                return json.dumps(d), 404

            shard_view_list = [i for i in shard_dict[shard_id] if i != replica_address]
            broadcast(shard_view_list, {"causal-metadata": json.dumps(vector_clock), "shard-id": shard_id}, '/key-value-store-internal/' + key, "delete")
        else:
            shard_view_list = [i for i in shard_dict[shard_id]]
            broadcast(shard_view_list, {"causal-metadata": json.dumps(vector_clock), "shard-id": shard_id}, '/key-value-store-internal/' + key, "delete")
        
        # Broadcasting (if any) is finished
        return json.dumps({"message":"Deleted successfully", "causal-metadata": json.dumps(vector_clock), "shard-id": str(shard_id)}), 200
        

# Endpoint just for internal communication, updates vector clocks without incrementing our own position
@app.route('/key-value-store-internal/<key>', methods=['GET', 'PUT', 'DELETE'])
def internal_endpoint(key):
    global view
    global view_list
    global vector_clock
    global key_value_store
    if request.method == 'GET':
        if key in key_value_store:
            d = {'message':'Retrieved successfuly', 'causal-metadata':json.dumps(vector_clock), 'value':key_value_store[key]}
            return json.dumps(d), 200
        else:
            d = {'error':'Key does not exist', 'message':'Error in GET', 'causal-metadata':json.dumps(vector_clock)}
            return json.dumps(d), 404

    elif request.method == 'PUT':
        causal_metadata = ""
        value = ""
        if request.data != b'':
            requested_data = json.loads(request.data)
            causal_metadata = requested_data['causal-metadata']
            value = requested_data['value']
            shard_id = requested_data['shard-id']
    
        broadcaster_vc = json.loads(causal_metadata)
        for v in broadcaster_vc:
            if v in vector_clock:
                vector_clock[v] = max(broadcaster_vc[v], vector_clock[v])

        d = {}
        code = 404
        if key not in key_value_store:
            d = {"message":"Added successfully"}
            code = 201
        else:
            d = {"message":"Updated successfully"}
            code = 200

        key_value_store[key] = value
    
        # At this point, broadcasting (if any) is finished and request is completely done
        d['causal-metadata'] = json.dumps(vector_clock)
        d['shard-id'] = shard_id
        return json.dumps(d), code

    elif request.method == 'DELETE':
        causal_metadata = ""
        if request.data != b'':
            requested_data = json.loads(request.data)
            causal_metadata = requested_data['causal-metadata']
            shard_id = requested_data['shard-id']

        if key not in key_value_store:
            d = {"error":"Key does not exist", "message":"Error in DELETE", 'causal_metadata':json.dumps(vector_clock)}
            return json.dumps(d), 404


        broadcaster_vc = json.loads(causal_metadata)
        for v in broadcaster_vc:
            if v in vector_clock:
                vector_clock[v] = max(broadcaster_vc[v], vector_clock[v])

        try:    
            del key_value_store[key]
        except:
            d = {"error":"Key does not exist", "message":"Error in DELETE", 'causal_metadata':json.dumps(vector_clock)}
            return json.dumps(d), 404
        # Broadcasting (if any) is finished
        return json.dumps({"message":"Deleted successfully", "causal-metadata": json.dumps(vector_clock), "shard-id": shard_id}), 200

@app.route('/key-value-store-copy/', methods=['GET'])
def copy_endpoint():
    if request.method == 'GET':
        d = {"key-value-store":json.dumps(key_value_store), "causal-metadata":json.dumps(vector_clock)}
        return json.dumps(d), 200


startup()

### these are for creating a global view of the shards and the shard_ids
if shard_count is not None:
    shard_ids = shard_count_to_list(int(shard_count))
    shard_dict = process_shard(shard_ids, view_list, {})
else:
    ### we need this case in case a new member is added.
    ### new members have no shard_count, so we can't process yet, 
    ### we will deal with this in the resharding endpoint
    shard_dict = {}
    shard_ids = []

app.run(host='0.0.0.0', port = 8085)
