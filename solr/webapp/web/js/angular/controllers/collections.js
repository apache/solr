/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

solrAdminApp.controller('CollectionsController',
    function($scope, $routeParams, $location, $timeout, Collections, CollectionsV2, AliasesV2, ShardsV2, ReplicasV2, ConfigSetsV2, ClusterV2, Constants){
      $scope.resetMenu("collections", Constants.IS_ROOT_PAGE);

      $scope.refresh = function() {

          $scope.rootUrl = Constants.ROOT_URL + "#/~collections/" + $routeParams.collection;

          ClusterV2.listClusterNodes(function(error, data, response) {
            $timeout(function() {
              if (error) return;
              $scope.availableNodeSet = data.nodes;
            });
          });

          Collections.status(function (data) {
              $scope.collections = [];
              for (var name in data.cluster.collections) {
                  if (name.startsWith("._designer_")) {
                      continue;
                  }
                  var collection = data.cluster.collections[name];
                  collection.name = name;
                  collection.type = 'collection';
                  var shards = collection.shards;
                  collection.shards = [];
                  for (var shardName in shards) {
                      var shard = shards[shardName];
                      shard.name = shardName;
                      shard.collection = collection.name;
                      var replicas = shard.replicas;
                      shard.replicas = [];
                      for (var replicaName in replicas) {
                          var replica = replicas[replicaName];
                          replica.name = replicaName;
                          replica.collection = collection.name;
                          replica.shard = shard.name;
                          shard.replicas.push(replica);
                      }
                      collection.shards.push(shard);
                  }
                  $scope.collections.push(collection);
                  if ($routeParams.collection == name) {
                      $scope.collection = collection;
                  }
              }
              // Fetch aliases using getAliases to get properties
              AliasesV2.getAliases(function (error, adata, response) {
                  $timeout(function() {
                      if (error) return;
                      // TODO: Population of aliases array duplicated in app.js
                      $scope.aliases = [];
                      for (var key in adata.aliases) {
                          props = {};
                          if (key in adata.properties) {
                              props = adata.properties[key];
                          }
                          var alias = {name: key, collections: adata.aliases[key], type: 'alias', properties: props};
                          $scope.aliases.push(alias);
                          if ($routeParams.collection == 'alias_' + key) {
                              $scope.collection = alias;
                          }
                      }
                      // Decide what is selected in list
                      if ($routeParams.collection && !$scope.collection) {
                          alert("No collection or alias called " + $routeParams.collection);
                          $location.path("/~collections");
                      }
                  });
              });

              $scope.liveNodes = data.cluster.liveNodes;
          });
          ConfigSetsV2.listConfigSet(function(error, data, response) {
              $timeout(function() {
                  if (error) return;
                  $scope.configs = [];
                  var items = data.configSets;
                  for (var i in items) {
                      $scope.configs.push({name: items[i]});
                  }
              });
          });
      };

      $scope.hideAll = function() {
          $scope.showRename = false;
          $scope.showAdd = false;
          $scope.showDelete = false;
          $scope.showSwap = false;
          $scope.showCreateAlias = false;
          $scope.showDeleteAlias = false;
      };

      $scope.showAddCollection = function() {
        $scope.hideAll();
        $scope.showAdd = true;
        $scope.newCollection = {
          name: "",
          routerName: "compositeId",
          numShards: 1,
          configName: "",
          nrtReplicas: 0,
          tlogReplicas: 0,
          pullReplicas: 0,
          replicationFactor: 1
        };
      };

      $scope.toggleCreateAlias = function() {
        $scope.hideAll();
        $scope.showCreateAlias = true;
      }

      $scope.toggleDeleteAlias = function() {
        $scope.hideAll();
        $scope.showDeleteAlias = true;
      }

      $scope.cancelCreateAlias = $scope.cancelDeleteAlias = function() {
        $scope.hideAll();
      }

      $scope.createAlias = function() {
        var collections = [];
        for (var i in $scope.aliasCollections) {
          collections.push($scope.aliasCollections[i].name);
        }
        AliasesV2.createAlias({createAliasRequestBody: {name: $scope.aliasToCreate, collections: collections}}, function(error, data, response) {
          $timeout(function() {
            if (error) return;
            $scope.cancelCreateAlias();
            $scope.resetMenu("collections", Constants.IS_ROOT_PAGE);
            $location.path("/~collections/alias_" + $scope.aliasToCreate);
          });
        });
      }
      $scope.deleteAlias = function() {
        AliasesV2.deleteAlias($scope.collection.name, {}, function(error, data, response) {
          $timeout(function() {
            if (error) return;
            $scope.hideAll();
            $scope.resetMenu("collections", Constants.IS_ROOT_PAGE);
            $location.path("/~collections/");
          });
        });

      };
      $scope.addCollection = function() {
        if (!$scope.newCollection.name) {
          $scope.addMessage = "Please provide a core name";
        } else if (false) { //@todo detect whether core exists
          $scope.AddMessage = "A core with that name already exists";
        } else if ( $scope.newCollection.pullReplicas > 0 && ($scope.newCollection.nrtReplicas + $scope.newCollection.tlogReplicas == 0))
        {
          $scope.addMessage = "A collection can't be made up of just PULL replicas";
        } else {
            var coll = $scope.newCollection;
            var createCollectionParams = {
              name: coll.name,
              router: {name: coll.routerName},
              numShards: coll.numShards,
              config: coll.configName
            };
            if (coll.shards) createCollectionParams.shardNames = coll.shards.split(",").map(function(s) { return s.trim(); });
            if (coll.routerField) createCollectionParams.router.field = coll.routerField;
            if (coll.createNodeSet) createCollectionParams.nodeSet = coll.createNodeSet;
            if ($scope.replicaTypesChosen()) {
              createCollectionParams.nrtReplicas = coll.nrtReplicas;
              createCollectionParams.tlogReplicas = coll.tlogReplicas;
              createCollectionParams.pullReplicas = coll.pullReplicas;
            } else {
              createCollectionParams.replicationFactor = coll.replicationFactor;
            }
            CollectionsV2.createCollection({createCollectionRequestBody: createCollectionParams}, function(error, data, response) {
              $timeout(function() {
                if (error) return;
                $scope.cancelAddCollection();
                $scope.resetMenu("collections", Constants.IS_ROOT_PAGE);
                $location.path("/~collections/" + $scope.newCollection.name);
              });
            });
        }
      };

      $scope.cancelAddCollection = function() {
        delete $scope.addMessage;
        $scope.showAdd = false;
      };

      $scope.showDeleteCollection = function() {
          $scope.hideAll();
          if ($scope.collection) {
              $scope.showDelete = true;
          } else {
              alert("No collection selected.");
          }
      };

      $scope.deleteCollection = function() {
        if ($scope.collection.name == $scope.collectionDeleteConfirm) {
            CollectionsV2.deleteCollection($scope.collection.name, {}, function (error, data, response) {
                $timeout(function() {
                    if (error) return;
                    $location.path("/~collections");
                });
            });
        } else {
            $scope.deleteMessage = "Collection names do not match.";
        }
      };

      $scope.reloadCollection = function() {
        if (!$scope.collection) {
            alert("No collection selected.");
            return;
        }
        CollectionsV2.reloadCollection($scope.collection.name, function(error, data,response) {
           if (error) {
               $scope.reloadFailure = true;
               $timeout(function() {$scope.reloadFailure=false}, 1000);
               $location.path("/~collections");
           } else {
               $scope.reloadSuccess = true;
               $timeout(function() {$scope.reloadSuccess=false}, 1000);
           }
        });
      };

      $scope.toggleAddReplica = function(shard) {
          $scope.hideAll();
          shard.showAdd = !shard.showAdd;
          delete $scope.addReplicaMessage;

          ClusterV2.listClusterNodes(function(error, data, response) {
            $timeout(function() {
              if (error) return;
              $scope.nodes = data.nodes;
            });
          });
      };

      $scope.toggleRemoveReplica = function(replica) {
          $scope.hideAll();
          replica.showRemove = !replica.showRemove;
      };

      $scope.toggleRemoveShard = function(shard) {
          $scope.hideAll();
          shard.showRemove = !shard.showRemove;
      };

      $scope.deleteShard = function(shard) {
          ShardsV2.deleteShard(shard.collection, shard.name, {}, function(error, data, response) {
            if (error) return;
            shard.deleted = true;
            $timeout(function() {
              $scope.refresh();
            }, 2000);
          });
        }

      $scope.deleteReplica = function(replica) {
        ReplicasV2.deleteReplicaByName(replica.collection, replica.shard, replica.name, {}, function(error, data, response) {
          if (error) return;
          replica.deleted = true;
          $timeout(function() {
            $scope.refresh();
          }, 2000);
        });
      }
      $scope.addReplica = function(shard) {
        var createReplicaParams = {
          type: shard.replicaType
        }
        if (shard.replicaNodeName && shard.replicaNodeName != "") {
          createReplicaParams.node = shard.replicaNodeName;
        }
        ReplicasV2.createReplica(shard.collection, shard.name, {createReplicaRequestBody: createReplicaParams}, function(error, data, response) {
          if (error) return;
          shard.replicaAdded = true;
          $timeout(function () {
            shard.replicaAdded = false;
            shard.showAdd = false;
            $scope.refresh();
          }, 2000);
        });
      };

      $scope.toggleShard = function(shard) {
          shard.show = !shard.show;
      }

      $scope.toggleReplica = function(replica) {
          replica.show = !replica.show;
      }

      $scope.replicaTypesChosen = function () {
          if ($scope.newCollection) {
            return ( $scope.newCollection.nrtReplicas + $scope.newCollection.tlogReplicas + $scope.newCollection.pullReplicas > 0 );
          }
      }

      $scope.refresh();
    }
);
