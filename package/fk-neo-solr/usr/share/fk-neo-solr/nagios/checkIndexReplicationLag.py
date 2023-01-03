import urllib
import json
import socket
import sys


# nagios return codes
UNKNOWN = -1
OK = 0
WARNING = 1
CRITICAL = 2


def get_index_replication_url(input_replica):
  return input_replica["base_url"] + "/" + input_replica["core"] + "/replication?command=details&wt=json"


def get_index_generation(input_replica):
  index_replication_url = input_replica["base_url"] + "/" + input_replica["core"] + \
                          "/replication?command=details&wt=json"
  replica_response = urllib.urlopen(index_replication_url)
  replica_response_data = json.loads(replica_response.read())
  return replica_response_data["details"]["generation"]


host_ip = socket.gethostbyname(socket.gethostname())

url = "http://localhost:8983/solr/admin/collections?action=clusterstatus&wt=json"

try:
  response = urllib.urlopen(url)
except:
  print 'solr not running on localhost'
  sys.exit(OK)

data = json.loads(response.read())

collections = data["cluster"]["collections"]

for collection in collections.values():
  shards = collection["shards"]

  for shard in shards.values():

    replicas = shard["replicas"]

    leader = None
    for replica in replicas.values():
      leader_key = "leader"
      if leader_key in replica.keys():
        if replica[leader_key]:
          leader = replica

    for replica in replicas.values():
      if replica["node_name"].startswith(host_ip):
        if replica["state"] == "active" and (replica["type"] == "PULL" or replica["type"] == "TLOG"):
          replica_index_replication_url = get_index_replication_url(replica)

          leader_generation = get_index_generation(leader)
          replica_generation = get_index_generation(replica)

          print str(leader_generation) + "," + str(replica_generation) + " "

          if replica_generation < leader_generation - 4:
            print "lagging! leader_generation:" + str(leader_generation) + ", replica_generation:" + \
                  str(replica_generation)
            sys.exit(CRITICAL)
        elif replica["state"] != "active":
          print "replica not active"
        else:
          print "attention! I am NRI (sorry! NRT) replica"

print "OK"
