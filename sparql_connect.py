#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging
from SPARQLWrapper import SPARQLWrapper, JSON, XML, N3, RDF
from rdflib import Namespace

DEBUG = False
SPARQL_ENDPOINT = 'http://localhost:8001/sparql/'
SPARQL_ENDPOINT_UPDATE = 'http://localhost:8001/update/'
GRAPH = "http://smob.me/subscribers"

FOAF=Namespace("http://xmlns.com/foaf/0.1/" )
RDFS=Namespace("http://www.w3.org/2000/01/rdf-schema#")
CERT=Namespace("http://www.w3.org/ns/auth/cert#")
RSA=Namespace("http://www.w3.org/ns/auth/rsa#")

class VirtuosoConnect:
    """This class is built on the sparql wrapper which queries a sparql endpoint
   The returned results are then parsed based on the content type returned 
   TODO: 1. The graph should be initialized onlu once since we will be workin
            on a single graph
         2. Make this a singleton class with all the initializations done once
            """
    
    def select(self, query):
        """This function executes a select sparql query and returns a wrapper object
        The wrapper object can later be converted to get any format of result needed"""
        prefix = """PREFIX sioc: <http://rdfs.org/sioc/ns#>
                    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                    PREFIX push: <http://push.deri.ie/smob/> 
                    PREFIX purl: <http://purl.org/dc/terms/>
                    PREFIX category: <http://dbpedia.org/resource/Category:>
                 """
        query = prefix + query        
        sparql = SPARQLWrapper(SPARQL_ENDPOINT)
        logging.debug('Going to execute: %r', query)
        sparql.setQuery(query)
        uris = self.returnJson(sparql, "callback")
        logging.debug('uris: %r', uris)
        return uris
    
    def foaf_exists(self, person_URI):
        """This function checks if the foaf profile of the person already present or not
            TODO: This can be done in terms of ask query which returns the JSON format"""
        #FIXME: hackish
        person_URI =  person_URI.rstrip("#me")
#        query = """
#               PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
#               PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
#               PREFIX foaf: <http://xmlns.com/foaf/0.1/>
#               SELECT COUNT(*) 
#               FROM <""" + GRAPH + """>
#               WHERE {
#              <"""+ person_URI +"""> a foaf:Person .
#              ?b foaf:primaryTopic <"""+ person_URI +"""> .
#               }"""
        # in 4store
#        query = """
#PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
#PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
#PREFIX foaf: <http://xmlns.com/foaf/0.1/>
#SELECT (COUNT(?b) as ?numberSubscribers)
#WHERE {
#    GRAPH <""" + GRAPH + """> {
#      <"""+ person_URI +"""> a foaf:Person .
#      ?b foaf:primaryTopic <"""+ person_URI +"""> .
#    }
#} """
        query = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT (COUNT(?b) as ?numberSubscribers)
WHERE {
  <"""+ person_URI +"""> a foaf:Person .
  ?b foaf:primaryTopic <"""+ person_URI +"""> .
}"""
        logging.debug("going to execute " + query)
        sparql = SPARQLWrapper(SPARQL_ENDPOINT)
        sparql.setQuery(query)

        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        count = results["results"]["bindings"][0]["numberSubscribers"]["value"]
        logging.debug("result query: ")
        logging.debug(count)

        #count_array = self.returnJson(sparql, "callret-0")
        #if int(count_array[0]) > 0:
        #    logging.debug("count of foaf profile: %s", count_array[0])
        
        if int(count) > 0:
            return True
        return False
        
    def insert(self, insQuery):
        """This function takes in a insert statment and returns 
        whether it was executed fine or not"""
        query = """
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rsa: <http://www.w3.org/ns/auth/rsa#>
PREFIX cert: <http://www.w3.org/ns/auth/cert#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
INSERT DATA { 
    GRAPH <""" + GRAPH + """> {
        { """+insQuery+"""}
}"""
        logging.debug("going to execute " + query)
        sparql = SPARQLWrapper(SPARQL_ENDPOINT_UPDATE)
        sparql.setQuery(query)
        results=sparql.query().convert()

    def insertTriples(self, triples):
        count = len(triples)
        tripleString = ""
        for row in triples:
            tripleString += row[0]+' '+row[1]+' '+row[2]+' . '
            if count%10 == 0:
                self.insert(tripleString) 
                tripleString = ""
            count = count-1
        if tripleString is not "":
            self.insert(tripleString)
            
    def returnJson(self, wrapper, variable):
        uris = []
        wrapper.setReturnFormat(JSON)
        results = wrapper.query().convert()
        for result in results["results"]["bindings"]:
            uris.append(result[variable]["value"])
        return uris
