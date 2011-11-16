import logging
import rdflib
import sparql_connect

from rdflib.graph import Graph
from rdflib.term import URIRef, Literal, BNode
from rdflib.namespace import Namespace, RDF
from rdflib import plugin

from get_private_webid_uri import get_private_uri
from get_private_webid_uri import request_with_client_cert

from google.appengine.api import urlfetch

HUB_CERTIFICATE = 'hub_cert.pem'
HUB_KEY = 'hub_key.key'


# Configure how we want rdflib logger to log messages
_logger = logging.getLogger("rdflib")
_logger.setLevel(logging.DEBUG)
_hdlr = logging.StreamHandler()
_hdlr.setFormatter(logging.Formatter('%(name)s %(levelname)s: %(message)s'))
_logger.addHandler(_hdlr)


#Add Namespaces here to use it through out the file
FOAF = Namespace("http://xmlns.com/foaf/0.1/")
PUSH = Namespace("http://vocab.deri.ie/push/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
CERT = Namespace("http://www.w3.org/ns/auth/cert#")
RSA = Namespace("http://www.w3.org/ns/auth/rsa#")
        
        
class ReadFOAF:
    """
        @author: Pavan Kapanipathi#
        class ReadFOAF: This class reads a foaf profile provided the source.
                Adds the required triples(publisher/subscriber) to the FOAF profile.
                Transforms the RDF/XML to tuple format, convinient to store it via sparql endpoint 
    """
    
    def __init__(self):
        self.triple_store = sparql_connect.VirtuosoConnect()
        plugin.register('sparql', rdflib.query.Processor,
                        'rdfextras.sparql.processor', 'Processor')
        plugin.register('sparql', rdflib.query.Result,
                'rdfextras.sparql.query', 'SPARQLQueryResult')
        
    def parsefoaf(self, location, pub, topic, callback):
        """
         Method: parsefoaf(location)
         @param location:Either the location or the foaf profile as a string
         Parses the foaf profile and provides the URI of the person who is represented in the FOAF
         Returns graph, person's uri
         
         TODO: Before the foaf triples are sent, need to check whether the publisher or the 
               subscriber are already in the rdf store.
                
        """
        store = Graph()
        #No need to add Namespaces, will be added when parsing the RDFXML
        #store.bind("dc", "http://http://purl.org/dc/elements/1.1/")
        store.bind("foaf", FOAF)
        store.bind("rdfs", RDFS)
        store.bind("rdf", RDF)
        store.bind("rsa", RSA)
        store.bind("cert", CERT)
        
        # FIXME
        # Next line get errors, see get_private_webid_uri file 
        import sys
        logging.debug(sys.modules)
        logging.debug(sys.path)
        foaf = get_private_uri(location, HUB_CERTIFICATE, HUB_KEY)
        #foaf = request_with_client_cert(location, HUB_CERTIFICATE, HUB_KEY)
        
        # getting the public foaf
        #response = urlfetch.fetch(location, validate_certificate=False)
        #logging.debug(response.status_code)
        #foaf = response.content
        #if result.status_code == 200:
        
        logging.debug("foaf: " + foaf)

        # insert the foaf into the graph
        store.parse(data=foaf, format="application/rdf+xml")
        #store.parse("http://www.w3.org/People/Berners-Lee/card.rdf")
        #for person in store.subjects(RDF.type, FOAF["Person"]):
             #print "Person:"+person
        
        # No need for the query, we can get person(s) from foaf directly from 
        # the graph
#        qres = store.query(
#            """SELECT DISTINCT ?a 
#               WHERE {
#              ?a a <http://xmlns.com/foaf/0.1/Person> .
#              ?b <http://xmlns.com/foaf/0.1/primaryTopic> ?a .
#               }""")
#        person_URI = ''
#        for row in qres.result:
#             person_URI = row

        # FIXME: what if there're more than one persons?
        person_URI = [p for p in store.subjects(RDF.type, FOAF["Person"])][0]

        # Check whether the foaf of the person is already present in the rdf store.
        # To speed up the execution we can keep a cache of the person_URIs whose foaf profiles 
        # are present.

        logging.info("Checking whether foaf: %s is already present in the RDF store", person_URI)
        if self.triple_store.foaf_exists(person_URI):
            # if foaf already in in the rdf store, then reset the graph as 
            # there's no need to add person foaf again
            # FIXME: is needed to add PuSH triples? 
            store = Graph()
            logging.info("foaf: %s is already present in the RDF store", person_URI)

        # Add the rest of the required triples to the graph
        store = self.addTriples(store, person_URI, pub, topic, callback)
        
        # Transform the graph to triples
        # no need for the manual transformation function, just
        #triples = self.to_tuples(store, location)
        triples = store.serialize(format="turtle")
        logging.debug("triples: "+triples)
#        prefixes = ["@prefix "+ns[0]+": "+ns[1].n3()+" .\n" for ns in store.namespaces()]
#        logging.debug("prefixes: ")
#        logging.debug(prefixes)
#        for p in prefixes: triples=triples.replace(p,"")
#        logging.debug("triples: ")
#        logging.debug(triples)
        
        # Now we're returning an string, no more a list
        return triples
    
    def addTriples(self, graph, uri, pub, topic, callback):
        """ Method: addTriples(graph, uri, pub, topic)
            @param graph: profile in the graph format
            uri: URI of the person who is represented in the FOAF profile
            pub: boolean whether he is a publisher or not
            topic: topic URL publishing/subscribing
            Add the corresponding triples based on the PUSH vocabulary
            Returns graph 
        """
        smobAccount = uri+"-smob"
        # Here we do need the PuSH namespace, as it has not been added before
        graph.bind("push", PUSH)
        graph.add((URIRef(uri), FOAF["holdsAccount"], URIRef(smobAccount)))
        if pub:
            graph.add((URIRef(topic), PUSH["has_owner"], URIRef(smobAccount)))
            logging.info("Adding triples to Publisher %s", uri)
        else:
            graph.add((URIRef(uri), PUSH["has_callback"], URIRef(callback)))
            graph.add((URIRef(topic), PUSH["has_subscriber"], URIRef(smobAccount)))
            logging.info("Adding triples to Subscriber %s", uri)
        return graph

    def to_tuples(self, graph, location):
        """
        Method: to_tuples(graph)
        @param graph:the graph which has to be converted to tuples
        Returns array of triples which can be Inserted via a sparql endpoint
        """
        logging.info("Transforming the graph with PUSH triples for %s", location)
        triples = []
        for s, o, p in graph:
            triple = []
            if s.__class__ is BNode:
                triple.append('_:'+str(s))
            else:
                if len(str(s))==0:
                    s = location
                triple.append('<'+str(s)+'>')
            triple.append('<'+str(o)+'>')
            if p.__class__ is BNode:
                triple.append('_:'+str(p))
            elif p.__class__ is URIRef:
                triple.append('<'+str(p)+'>')
            else:
                triple.append('"""'+str(p)+'"""')
            triples.append(triple)
        return triples

def main():
    read = ReadFOAF()
    triples = read.parsefoaf("http://localhost/smob/private", True, "http://topic.com/semanticweb", "")
    for row in triples:
        print row[0]+' '+row[1]+' '+row[2]+' .'
    #for s, o, p in store:
    #    if p.__class__ is BNode:
    #        print p
    #    print s+'\t'+p+'\t'+q
    #print store
    #     store.serialize(format='pretty-xml')
    #for statement in store:
        #print statement
    

if __name__ == "__main__":
    main()
