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

var solrAdminServices = angular.module('solrAdminServices', ['ngResource']);

solrAdminServices.factory('Metrics',
  ['$resource', 'PrometheusParser', function($resource, PrometheusParser) {
    return $resource('admin/metrics', {"wt":"prometheus", "node": "@node", "_":Date.now()}, {
      get: {
        method: 'GET',
        transformResponse: function(data) {
          // Parse the merged Prometheus text response
          try {
            return {metrics: PrometheusParser.parse(data)};
          } catch (e) {
            return {metrics: {}, error: e.message};
          }
        }
      },
      "raw": {
        method: 'GET',
        params: {wt: 'prometheus', core: '@core'},
        transformResponse: function(data) {
          return {data: data};
        }
      }
    });
  }])
.factory('ApiErrorHandler',
    ['$rootScope', '$location', '$timeout', function($rootScope, $location, $timeout) {
      // v2 API calls go through the generated OpenAPI client, which uses superagent directly and
      // so never passes through Angular's $http -- meaning httpInterceptor in app.js (the global
      // error banner, 401 redirect, 403 handling) never fires for them. This mirrors
      // httpInterceptor's responseError handling for v2's superagent responses, so a failure looks
      // the same to the user regardless of which API generation served it. Call from any v2
      // callback's error branch: `if (error) { ApiErrorHandler.handle(response); return; }`
      function handle(response) {
        if (!response) {
          return;
        }
        // superagent callbacks fire outside Angular's digest cycle, so changes here are invisible
        // until the next digest -- $timeout both defers and triggers one.
        $timeout(function() {
          if (response.status === 401) {
            var headers = response.headers || {};
            sessionStorage.setItem("auth.wwwAuthHeader", headers['www-authenticate']);
            sessionStorage.setItem("auth.authDataHeader", headers['x-solr-authdata']);
            sessionStorage.setItem("auth.statusText", response.statusText);
            sessionStorage.setItem("http401", "true");
            sessionStorage.removeItem("auth.scheme");
            sessionStorage.removeItem("auth.realm");
            sessionStorage.removeItem("auth.username");
            sessionStorage.removeItem("auth.header");
            sessionStorage.removeItem("auth.state");
            if ($location.path().includes('/login')) {
              if (!sessionStorage.getItem("auth.location")) {
                sessionStorage.setItem("auth.location", "/");
              }
            } else {
              sessionStorage.setItem("auth.location", $location.path());
              $location.path('/login');
            }
          } else if (response.status === 403) {
            $rootScope.showAuthzFailures = true;
          } else {
            var url = (response.req && response.req.url) || (response.status + ' ' + $location.url());
            var body = response.body || {};
            var msg = (body.error && body.error.msg) || response.statusText || "Unknown error";
            // MainController normally sets this up first, but don't assume that ordering here.
            $rootScope.exceptions = $rootScope.exceptions || {};
            $rootScope.exceptions[url] = {msg: msg};
          }
        });
      }
      return {handle: handle};
    }])
.factory('CollectionsV2',
    function() {
      solrApi.ApiClient.instance.basePath = '/api';
      delete solrApi.ApiClient.instance.defaultHeaders["User-Agent"];
      return new solrApi.CollectionsApi();
    })
.factory('CoresV2',
    function() {
      solrApi.ApiClient.instance.basePath = '/api';
      delete solrApi.ApiClient.instance.defaultHeaders["User-Agent"];
      return new solrApi.CoresApi();
    })
.factory('LoggingV2',
    function() {
      solrApi.ApiClient.instance.basePath = '/api';
      delete solrApi.ApiClient.instance.defaultHeaders["User-Agent"];
      return new solrApi.LoggingApi();
    })
.factory('SystemV2',
    function() {
      solrApi.ApiClient.instance.basePath = '/api';
      delete solrApi.ApiClient.instance.defaultHeaders["User-Agent"];
      return new solrApi.SystemApi();
    })
.factory('AliasesV2',
    function() {
      solrApi.ApiClient.instance.basePath = '/api';
      delete solrApi.ApiClient.instance.defaultHeaders["User-Agent"];
      return new solrApi.AliasesApi();
    })
.factory('ShardsV2',
    function() {
      solrApi.ApiClient.instance.basePath = '/api';
      delete solrApi.ApiClient.instance.defaultHeaders["User-Agent"];
      return new solrApi.ShardsApi();
    })
.factory('ReplicasV2',
    function() {
      solrApi.ApiClient.instance.basePath = '/api';
      delete solrApi.ApiClient.instance.defaultHeaders["User-Agent"];
      return new solrApi.ReplicasApi();
    })
.factory('ConfigSetsV2',
    function() {
      solrApi.ApiClient.instance.basePath = '/api';
      delete solrApi.ApiClient.instance.defaultHeaders["User-Agent"];
      return new solrApi.ConfigsetsApi();
    })
.factory('ClusterV2',
    function() {
      solrApi.ApiClient.instance.basePath = '/api';
      delete solrApi.ApiClient.instance.defaultHeaders["User-Agent"];
      return new solrApi.ClusterApi();
    })
.factory('SegmentsV2',
    function() {
      solrApi.ApiClient.instance.basePath = '/api';
      delete solrApi.ApiClient.instance.defaultHeaders["User-Agent"];
      return new solrApi.SegmentsApi();
    })
.factory('SchemaDesignerV2',
    function() {
      solrApi.ApiClient.instance.basePath = '/api';
      delete solrApi.ApiClient.instance.defaultHeaders["User-Agent"];
      return new solrApi.SchemaDesignerApi();
    })
.factory('Collections',
  ['$resource', function ($resource) {
    // v2 ClusterAPI (/api/cluster) delegates straight through to the same v1 CollectionsHandler
    // that v1's CLUSTERSTATUS action used, so the response shape is byte-identical -- no
    // generated solrApi client class exists for it (old-style @EndPoint API, predates the
    // OpenAPI-based v2 framework), so this stays a plain $resource, like Threads/ParamSet.
    return $resource('/api/cluster', {'wt':'json', '_':Date.now()}, {
      "status": {}
    });
  }])
.factory('ConfigSetFiles',
 ['$http', function ($http) {
    // Fetches a single file from a configset via V2 /api/configsets/{name}/files/{path}.
    // Each path segment is encoded separately so subdirectory paths like "lang/stopwords.txt"
    // preserve their slashes (encoding them as %2F gets rejected by Jetty).
    // transformResponse is overridden to skip JSON parsing since files are raw text.
    return {
      get: function (params, successFn, errorFn) {
        var url = "/api/configsets/" + encodeURIComponent(params.configSet) +
                  "/files/" + params.filePath.split("/").map(encodeURIComponent).join("/");
        $http.get(url, {transformResponse: [function (data) { return data; }]}).then(
          function (response) { if (successFn) successFn({content: response.data}); },
          function (response) { if (errorFn) errorFn(response); }
        );
      }
    };
 }])
.factory('Logging',
  ['$resource', function($resource) {
    // This v1 factory only covers "setLevel", which needs the "nodes=all" broadcast-to-every-node
    // behavior that the v2 NodeLoggingApis endpoint doesn't support yet (see SOLR-16738). Retire
    // this factory once setLevel moves to LoggingV2.
    return $resource('admin/info/logging', {'wt':'json', '_':Date.now()}, {
      "setLevel": {params: {nodes:'all'}}
      });
  }])
.factory('Zookeeper',
  ['$resource', function($resource) {
    return $resource('admin/zookeeper', {wt:'json', _:Date.now()}, {
      "simple": {},
      "liveNodes": {params: {path: '/live_nodes'}},
      "clusterState": {params: {detail: "true", path: "/clusterstate.json"}},
      "detail": {params: {detail: "true", path: "@path"}}
    });
  }])
.factory('ZookeeperStatus',
  ['$resource', function($resource) {
    return $resource('admin/zookeeper/status', {wt:'json', _:Date.now()}, {
      "monitor": {}
    });
  }])
.factory('Properties',
  ['$resource', function($resource) {
    return $resource('admin/info/properties', {'wt':'json', '_':Date.now()});
  }])
.factory('Threads',
  ['$resource', function($resource) {
    // v2 NodeThreadsAPI (/api/node/threads) still just delegates straight through to the same v1
    // ThreadDumpHandler, so the response shape is byte-identical -- no generated solrApi client
    // class exists for it (it predates the OpenAPI-based v2 API framework), so this stays a plain
    // $resource, like Security and (partially) SchemaDesigner.
    return $resource('/api/node/threads', {'wt':'json', '_':Date.now()});
  }])
.factory('Replication',
  ['$resource', function($resource) {
    return $resource(':core/replication', {'wt':'json', core: "@core", '_':Date.now()}, {
      "details": {params: {command: "details"}},
      "command": {params: {}}
    });
  }])
.factory('CoreInfo',
  ['$resource', function($resource) {
    return $resource(':core/admin/info', {wt:'json', core: "@core", _:Date.now()});
  }])
.factory('Update',
  ['$resource', function($resource) {
    return $resource(':core/:handler', {core: '@core', wt:'json', _:Date.now(), handler:'update'}, {
      "commit": {params: {commit: "true"}},
      "post": {headers: {'Content-type': 'application/json'}, method: "POST", params: {handler: '@handler'}},
      "postJson": {headers: {'Content-type': 'application/json'}, method: "POST", params: {handler: '@handler'}},
      "postXml": {headers: {'Content-type': 'text/xml'}, method: "POST", params: {handler: '@handler'}},
      "postCsv": {headers: {'Content-type': 'application/csv'}, method: "POST", params: {handler: '@handler'}}
    });
  }])
.factory('ParamSet',
  ['$resource', function($resource) {
    // v2 GetConfigAPI/ModifyParamSetAPI (/api/(cores|collections)/:core/config/params) still
    // delegate straight through to the same v1 SolrConfigHandler, so the response shape is
    // byte-identical -- no generated solrApi client class exists for it (old-style @EndPoint API,
    // predates the OpenAPI-based v2 framework), so this stays a plain $resource, like Threads.
    // NB: unlike v1's flexible routing, the v2 API requires knowing up front whether ":core" is a
    // collection name (SolrCloud) or an actual core name (standalone/user-managed) --
    // /api/collections/... 500s in standalone mode (it tries to resolve aliases, which needs ZK),
    // and there's no such thing as a "collection" there anyway. Callers must pass "indexType" as
    // "collections" or "cores" (see paramsets.js, driven by $scope.isCloudEnabled).
    return $resource('/api/:indexType/:core/config/params/:name', {core: '@core', indexType: '@indexType', wt:'json', _:Date.now()}, {
      "submit": {headers: {'Content-type': 'application/json'}, method: "POST"},
      "get": {headers: {'Content-type': 'application/json'}, method: "GET"}
    });
  }])
.service('FileUpload', function ($http) {
    this.upload = function(params, file, success, error){
        var url = "" + params.core + "/" + params.handler + "?";
        raw = params.raw;
        delete params.core;
        delete params.handler;
        delete params.raw;
        url += $.param(params);
        if (raw && raw.length>0) {
            if (raw[0] != "&") raw = "&" + raw;
            url += raw;
        }
        var fd = new FormData();
        fd.append('file', file);
        $http.post(url, fd, {
            transformRequest: angular.identity,
            headers: {'Content-Type': undefined}
        }).then(function(response) {
            success(response.data);
        }, function(response) {
            error(response.data);
        });
    }
})
.filter('splitByComma', function() {
  return function(input) {
    return input === undefined ? input : input.split(',');
  }
})
.factory('Luke',
  ['$resource', function($resource) {
    return $resource(':core/admin/luke', {core: '@core', wt:'json', _:Date.now()}, {
      "index":  {params: {numTerms: 0, show: 'index'}},
      "raw": {params: {numTerms: 0}},
      "schema": {params: {show:'schema'}},
      "field": {},
      "fields": {params: {show:'schema'}, interceptor: {
          response: function(response) {
              var fieldsAndTypes = [];
              for (var field in response.data.schema.fields) {
                fieldsAndTypes.push({group: "Fields", label: field, value: "fieldname=" + field});
              }
              for (var type in response.data.schema.types) {
                fieldsAndTypes.push({group: "Types", label: type, value: "fieldtype=" + type});
              }
              return fieldsAndTypes;
          }
      }}
    });
  }])
.factory('Analysis',
  ['$resource', function($resource) {
    return $resource(':core/analysis/field', {core: '@core', wt:'json', _:Date.now()}, {
      "field": {params: {"analysis.showmatch": true}}
    });
  }])
.factory('Ping',
  ['$resource', function($resource) {
    return $resource(':core/admin/ping', {wt:'json', core: '@core', ts:Date.now(), _:Date.now()}, {
     "ping": {},
     "enable": {params:{action:"enable"}, headers: {doNotIntercept: "true"}},
     "disable": {params:{action:"disable"}, headers: {doNotIntercept: "true"}},
     "status": {params:{action:"status"}, headers: {doNotIntercept: "true"}
    }});
  }])
.factory('Files',
  ['$resource', function($resource) {
    return $resource(':core/admin/file', {'wt':'json', core: '@core', '_':Date.now()}, {
      "list": {},
      "get": {method: "GET", interceptor: {
          response: function(config) {return config;}
      }, transformResponse: function(data) {
          return data;
      }}
    });
  }])
.factory('Query',
    ['$resource', function($resource) {
       var resource = $resource(':core/:handler', {core: '@core', handler: '@handler', '_':Date.now()}, {
           "query": {
             method: "GET",
             transformResponse: function (data) {
               return {data: data}
             },
             headers: {doNotIntercept: "true"}
           }
       });
       resource.url = function(params) {
           var qs = [];
           for (key in params) {
               if (key != "core" && key != "handler") {
                   for (var i in params[key]) {
                       qs.push(key + "=" + encodeURIComponent(params[key][i]));
                   }
               }
           }
           return "" + params.core + "/" + params.handler + "?" + qs.sort().join("&");
       }
       return resource;
}])
.factory('Schema',
   ['$resource', function($resource) {
     return $resource(':core/schema', {wt: 'json', core: '@core', _:Date.now()}, {
       get: {method: "GET"},
       check: {method: "GET", headers: {doNotIntercept: "true"}},
       post: {method: "POST"}
     });
}])
.factory('Config',
   ['$resource', function($resource) {
     return $resource(':core/config', {wt: 'json', core: '@core', _:Date.now()}, {
       get: {method: "GET"}
     })
}])
.factory('SchemaDesigner',
   ['$resource', function($resource) {
     // Schema Designer's analyze (sample-doc upload/paste, dynamic content-type) and query
     // (arbitrary forwarded Solr query params) endpoints read their request bodies/params in ways
     // the OpenAPI-generated SchemaDesignerApi client can't express: analyze() always sends a null
     // body (the server deliberately reads the raw content stream, dispatched by Content-Type,
     // rather than a formal parameter) and query() takes no query params at all (the server
     // forwards arbitrary SolrParams straight through). Both stay on this plain $resource, like
     // Threads/Collections/ParamSet. Every other Schema Designer endpoint uses SchemaDesignerV2.
     return $resource('/api/schema-designer/:configSet/:path', {wt: 'json', path: '@path', configSet: '@configSet', filePath: '@filePath', _:Date.now()}, {
       get: {method: "GET"},
       post: {method: "POST", timeout: 90000},
       postXml: {headers: {'Content-type': 'text/xml'}, method: "POST", timeout: 90000},
       postCsv: {headers: {'Content-type': 'application/csv'}, method: "POST", timeout: 90000},
       upload: {method: "POST", transformRequest: angular.identity, headers: {'Content-Type': undefined}, timeout: 90000}
     })
}])
.factory('Security',
    ['$resource', function($resource) {
          return $resource('/api/cluster/security/:path', {wt: 'json', path: '@path', _:Date.now()}, {
            get: {method: "GET"}, post: {method: "POST", timeout: 90000}
        })
}])
.factory('AuthenticationService',
    ['base64', '$resource', function (base64, $resource) {
      var service = {};

      service.getOAuthTokens = function (url, data) {
        var serializedData = serialize(data);
        var resource = $resource(url, {}, {
          getToken: {
            method: 'POST',
            timeout: 10000,
            headers: {
              'Content-Type': 'application/x-www-form-urlencoded',
              'X-Requested-With': undefined // Set this header to undefined to prevent preflight requests
            },
            transformResponse: function (data) {
              return angular.fromJson(data);
            }
          }
        });
        return resource.getToken({}, serializedData).$promise;
      };

      var codeChallengeMethod = "S256";
      service.getCodeChallengeMethod = function getCodeChallengeMethod() {
        return codeChallengeMethod;
      }

      service.generateCodeVerifier = function generateCodeVerifier() {
        var codeVerifier = '';
        var possibleChars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~';
        for (var i = 0; i < 96; i++) {
          codeVerifier += possibleChars.charAt(Math.floor(Math.random() * possibleChars.length));
        }
        return codeVerifier;
      }

      service.generateCodeChallengeFromVerifier = async function generateCodeChallengeFromVerifier(v) {
        var hashed = await sha256(v);
        var base64encoded = base64urlencode(hashed);
        return base64encoded;
      }

      function sha256(str) {
        const shaObj = new jsSHA("SHA-256", "TEXT", { encoding: "UTF8" });
        shaObj.update(str);
        return shaObj.getHash("UINT8ARRAY");
      }

      function base64urlencode(a) {
        var str = "";
        var bytes = new Uint8Array(a);
        var len = bytes.byteLength;
        for (var i = 0; i < len; i++) {
          str += String.fromCharCode(bytes[i]);
        }
        return btoa(str)
          .replace(/\+/g, "-")
          .replace(/\//g, "_")
          .replace(/=+$/, "");
      }

      var serialize = function (obj) {
        var str = [];
        for (var p in obj) {
          if (obj.hasOwnProperty(p)) {
            str.push(encodeURIComponent(p) + "=" + encodeURIComponent(obj[p]));
          }
        }
        return str.join("&");
      };

        service.SetCredentials = function (username, password) {
          var authdata = base64.encode(username + ':' + password);

          // The V2 solrApi client picks this up automatically via the superagent plugin
          // registered in app.js .config().
          sessionStorage.setItem("auth.header", "Basic " + authdata);
          sessionStorage.setItem("auth.username", username);
        };

        service.ClearCredentials = function () {
          sessionStorage.removeItem("auth.header");
          sessionStorage.removeItem("auth.scheme");
          sessionStorage.removeItem("auth.realm");
          sessionStorage.removeItem("auth.username");
          sessionStorage.removeItem("auth.wwwAuthHeader");
          sessionStorage.removeItem("auth.statusText");
          localStorage.removeItem("auth.stateRandom");
          sessionStorage.removeItem("auth.nonce");
          sessionStorage.removeItem("auth.flow");
        };

        service.getAuthDataHeader = function () {
          try {
            var header64 = sessionStorage.getItem("auth.authDataHeader");
            var headerJson = base64.decode(header64);
            return JSON.parse(headerJson);
          } catch (e) {
            console.log("WARN: Wrong or missing X-Solr-AuthData header on 401 response " + e);
            return null;
          }
        };

        service.decodeJwtPart = function (jwtPart) {
          try {
            return JSON.parse(base64.urldecode(jwtPart));
          } catch (e) {
            console.log("WARN: Invalid format of JWT part: " + e);
            return {};
          }
        };

        service.isJwtCallback = function (hash) {
          var hp = this.decodeHashParams(hash);
          // console.log("Decoded hash as " + JSON.stringify(hp, undefined, 2)); // For debugging callbacks
          return (hp['access_token'] && hp['token_type'] && hp['state']) || (hp['code'] && hp['state'])|| hp['error'];
        };

        service.decodeHashParams = function(hash) {
          // access_token, token_type, expires_in, state, code
          if (hash == null || hash.length === 0) {
            return {};
          }
          var params = {};
          var parts = hash.split("&");
          for (var p in parts) {
            var kv = parts[p].split("=");
            if (kv.length === 2) {
              params[kv[0]] = decodeURIComponent(kv[1]);
            } else {
              console.log("Invalid callback URI, got parameter " + parts[p] + " but expected key=value");
            }
          }
          return params;
        };

        return service;
      }]);
