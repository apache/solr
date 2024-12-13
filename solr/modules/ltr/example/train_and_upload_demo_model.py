#!/usr/bin/env python3

import sys
import json
import http.client
import urllib.request, urllib.parse, urllib.error
import libsvm_formatter

from optparse import OptionParser

import solr
from solr.api import querying_api

def setupSolrClient(host, port):
    '''Configures the Solr client with the specified Solr host/port'''
    solr_client_config = solr.Configuration(
            host = "http://" + host + ":" + str(port) + "/api"
    )
    solr.Configuration.set_default(solr_client_config)

def setupSolr(collection, host, port, featuresFile, featureStoreName):
    '''Sets up solr with the proper features for the test'''

    conn = http.client.HTTPConnection(host, port)

    baseUrl = "/solr/" + collection
    featureUrl = baseUrl + "/schema/feature-store"

    conn.request("DELETE", featureUrl+"/"+featureStoreName)
    r = conn.getresponse()
    msg = r.read()
    if (r.status != http.client.OK and
        r.status != http.client.CREATED and
        r.status != http.client.ACCEPTED and
        r.status != http.client.NOT_FOUND):
        raise Exception("Status: {0} {1}\nResponse: {2}".format(r.status, r.reason, msg))


    # Add features
    headers = {'Content-type': 'application/json'}
    featuresBody = open(featuresFile)

    conn.request("POST", featureUrl, featuresBody, headers)
    r = conn.getresponse()
    msg = r.read()
    if (r.status != http.client.OK and
        r.status != http.client.ACCEPTED):
        print(r.status)
        print("")
        print(r.reason);
        raise Exception("Status: {0} {1}\nResponse: {2}".format(r.status, r.reason, msg))

    conn.close()


def generateQueries(userQueriesFile, solrFeatureStoreName, efiParams):
        with open(userQueriesFile) as input:
            solrQueryInfo = [] #A list of tuples with solrQueryBody,queryText,docId,scoreForPQ,source

            for line in input:
                line = line.strip();
                searchText,docId,score,source = line.split("|");
                solrQueryBody = generateQueryBody(solrFeatureStoreName,efiParams,searchText,docId)
                solrQueryInfo.append((solrQueryBody,searchText,docId,score,source))
        return solrQueryInfo;


def generateQueryBody(solrFeatureStoreName, efiParams, searchText, docId):
    concreteEfiParams = efiParams.replace("$USERQUERY", searchText.strip())
    featuresTransformer = "[features store=" + solrFeatureStoreName + " " + concreteEfiParams + "]"
    solrJsonParams = {
            "query": "id:" + docId,
            "fields": ["id", "score", featuresTransformer]
    }

    return solrJsonParams


def generateTrainingData(solrQueries, coreName):
    '''Given a list of solr queries, yields a tuple of query , docId , score , source , feature vector for each query.
    Feature Vector is a list of strings of form "key=value"'''
    try:
        queryClient = querying_api.QueryingApi()
        for solrQueryBody,query,docId,score,source in solrQueries:
            msgDict = queryClient.json_query("cores", coreName, solrQueryBody)
            fv = ""
            docs = msgDict['response']['docs']
            if len(docs) > 0 and "[features]" in docs[0]:
                if not msgDict['response']['docs'][0]["[features]"] == None:
                    fv = msgDict['response']['docs'][0]["[features]"];
                else:
                    print("ERROR NULL FV FOR: " + docId);
                    print(msg)
                    continue;
            else:
                print("ERROR FOR: " + docId);
                print(msg)
                continue;
            if msgDict.get("response_header") != None:
                status = msgDict.get("response_header").get("status")
                if status == 0:
                    #print "http connection was ok for: " + queryUrl
                    yield(query,docId,score,source,fv.split(","));
                else:
                    raise Exception("Status: {0} \nResponse: {2}".format(status, msgDict))
    except Exception as e:
        print(msg)
        print(e)

def uploadModel(collection, host, port, modelFile, modelName):
    modelUrl = "/solr/" + collection + "/schema/model-store"
    headers = {'Content-type': 'application/json'}
    with open(modelFile) as modelBody:
        conn = http.client.HTTPConnection(host, port)

        conn.request("DELETE", modelUrl+"/"+modelName)
        r = conn.getresponse()
        msg = r.read()
        if (r.status != http.client.OK and
            r.status != http.client.CREATED and
            r.status != http.client.ACCEPTED and
            r.status != http.client.NOT_FOUND):
            raise Exception("Status: {0} {1}\nResponse: {2}".format(r.status, r.reason, msg))

        conn.request("POST", modelUrl, modelBody, headers)
        r = conn.getresponse()
        msg = r.read()
        if (r.status != http.client.OK and
            r.status != http.client.CREATED and
            r.status != http.client.ACCEPTED):
                raise Exception("Status: {0} {1}\nResponse: {2}".format(r.status, r.reason, msg))


def main(argv=None):
    if argv is None:
        argv = sys.argv

    parser = OptionParser(usage="usage: %prog [options] ", version="%prog 1.0")
    parser.add_option('-c', '--config',
                      dest='configFile',
                      help='File of configuration for the test')
    (options, args) = parser.parse_args()

    if options.configFile == None:
        parser.print_help()
        return 1

    with open(options.configFile) as configFile:
        config = json.load(configFile)

        print("Uploading features ("+config["solrFeaturesFile"]+") to Solr")
        setupSolrClient(config["host"], config["port"])
        setupSolr(config["collection"], config["host"], config["port"], config["solrFeaturesFile"], config["solrFeatureStoreName"])

        print("Converting user queries ("+config["userQueriesFile"]+") into Solr queries for feature extraction")
        reRankQueries = generateQueries(config["userQueriesFile"], config["solrFeatureStoreName"], config["efiParams"])

        print("Running Solr queries to extract features")
        fvGenerator = generateTrainingData(reRankQueries, config["collection"])
        formatter = libsvm_formatter.LibSvmFormatter();
        formatter.processQueryDocFeatureVector(fvGenerator,config["trainingFile"]);

        print("Training model using '"+config["trainingLibraryLocation"]+" "+config["trainingLibraryOptions"]+"'")
        libsvm_formatter.trainLibSvm(config["trainingLibraryLocation"],config["trainingLibraryOptions"],config["trainingFile"],config["trainedModelFile"])

        print("Converting trained model ("+config["trainedModelFile"]+") to solr model ("+config["solrModelFile"]+")")
        formatter.convertLibSvmModelToLtrModel(config["trainedModelFile"], config["solrModelFile"], config["solrModelName"], config["solrFeatureStoreName"])

        print("Uploading model ("+config["solrModelFile"]+") to Solr")
        uploadModel(config["collection"], config["host"], config["port"], config["solrModelFile"], config["solrModelName"])


if __name__ == '__main__':
    sys.exit(main())
