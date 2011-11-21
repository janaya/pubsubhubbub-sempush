import logging
import sys
import rdflib
from rdflib.graph import Graph
from rdflib.term import URIRef, Literal, BNode
from rdflib.namespace import Namespace, RDF
from rdflib import plugin
from SPARQLWrapper import SPARQLWrapper, JSON, XML, N3
from rdflib import Namespace

#from get_private_webid_uri import get_private_uri
#from get_private_webid_uri import request_with_client_cert
#from webid_auth.get_private_webid_uri import get_private_uri

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
REL = Namespace('http://purl.org/vocab/relationship/')

DEBUG = False
ENDPOINT_DATA = "http://localhost:8001/data/"
SPARQL_ENDPOINT = "http://localhost:8001/sparql/"
ENDPOINT_UPDATE = "http://localhost:8001/update/"

ns_profile = dict(foaf=FOAF, rel=REL, rdfs=RDFS, rdf=RDF)
ns_store = dict(foaf=FOAF, push=PUSH, rdfs=RDFS, rdf=RDF, cert=CERT, rsa=RSA)



# mapping Content-Type input to rdflib parsers and output to rdflib serializers 
# rdflib already do it when no format provided and source is an url
parsers = {'text/turtle':'n3',
            'application/rdf+xml':'xml',
            'text/html':'rdfa',
            'text/nt':'nt'}


serializers = {
            'application/rdf+xml':'rdf/xml',
            'text/nt':'nt',
            'text/turtle':'turtle',
            'text/n3':'n3'}

def ns_string(ns):
    return "\n".join(["PREFIX "+k+" : "+v.n3() for k,v in ns.items()])

class ProfileManager:
    def dereference_uri(self, foaf_uri):
        graph = Graph()
        [graph.bind(*x) for x in ns_profile.items()]

        # getting the public foaf and inserting into the graph

        # cant retrieve directly the url, GAE doesn't allow open sockets:
        # sock = socket.create_connection((self.host, self.port),
        # AttributeError: 'module' object has no attribute 'create_connection'
        #store.parse(location)

        # FIXME
        # Next line get errors, see get_private_webid_uri file 
        #foaf = get_private_uri(location, HUB_CERTIFICATE, HUB_KEY)
        #foaf = request_with_client_cert(location, HUB_CERTIFICATE, HUB_KEY)

        # so using GAE urlfetch...
        response = urlfetch.fetch(foaf_uri, validate_certificate=False)
        content_type = response.headers['content-type'].split(';')[0]
        logging.debug(content_type)
        #logging.debug(response.status_code)
        foaf = response.content
        #if result.status_code == 200:
        logging.debug("foaf: " + foaf)


        # can't parse n3 with GAE
        # rdflib/plugins/parsers/notation3.py", line 417, in runNamespace
        # base(), ".run-" + `time.time()` + "p"+ `os.getpid()` +"#")
        # AttributeError: 'module' object has no attribute 'getpid'
        #graph.parse(data=foaf, format="n3")
        
        # GAE doesn't allow to write to the filesystem either
        #f = open('temp.nt','w')
        #f.write(foaf)
        #f.close()
        #graph.parse('temp.nt', format="nt")

        # but can parse rdf+xml, so converting to it...
        graph.parse(data=foaf, format=parsers[content_type])

        # No need for the query, we can get person(s) from foaf directly from 
        # the graph
        # FIXME: what if there're more than one persons?
        person_URI = [p for p in graph.subjects(RDF.type, FOAF["Person"])][0]
        return graph

    def select(self, graph_uri):
        graph = Graph()
        query = """
SELECT * WHERE { GRAPH <""" + graph_uri + """> { ?s ?p ?o } }"""
        logging.debug("going to execute " + query)
        sparql = SPARQLWrapper(SPARQL_ENDPOINT)
        sparql.setQuery(query)
        sparql.setReturnFormat('rdf')
        try:
            ret = sparql.query()
            graph = ret.convert()
            #graph = sparql.query().convert()
            logging.debug(ret.variables)
            logging.debug(graph)
        except:
            sys.exc_info()
            logging.exception("WHAT HAPPENED?")
        return graph

    def insert(self, graph_uri, graph):
        triples = graph.serialize(format="turtle")
        urlfetch.fetch(url=ENDPOINT_DATA+graph_uri,
                    payload=triples,
                    method=urlfetch.PUT,
                    headers={'Content-Type': 'application/x-turtle'})
        # curl -T file.rdf -H 'Content-Type: application/x-turtle' 'http://localhost:8000/data/data.rdf'

    def update(self, graph_uri, graph):
        triples = graph.serialize(format="turtle")
        query = """
          INSERT DATA INTO <""" + graph_uri + """> {
          """ + triples + """
          }
        """

    def add_push_account(self, graph, uri, smobAccount):
        [graph.bind(*x) for x in ns_store.items()]
        graph.add((URIRef(uri), FOAF["holdsAccount"], URIRef(smobAccount)))
        return graph

    def add_topic_has_subscriber(self, graph, uri, topic, callback, smobAccount):
        graph.add((URIRef(uri), PUSH["has_callback"], URIRef(callback)))
        graph.add((URIRef(topic), PUSH["has_subscriber"], URIRef(smobAccount)))
        return graph

    def add_topic_has_owner(self, graph, topic, smobAccount):
        graph.add((URIRef(topic), PUSH["has_owner"], URIRef(smobAccount)))
        return graph

    def add_push_subscriber(self, graph, uri, topic, callback, smobAccount):
        graph = self.add_push_account(graph, uri, smobAccount)
        graph = self.add_topic_has_subscriber(graph, uri, topic, callback, smobAccount)
        return graph

    def add_push_publiher(self, graph, uri, topic, smobAccount):
        graph = self.add_push_account(graph, uri, smobAccount)
        graph = self.add_topic_has_owner(graph, topic, smobAccount)
        return graph

    def get_or_create_subscriber_profile(self, foaf_uri, callback, topic):
        foaf_graph = self.dereference_uri(foaf_uri)
        foaf_uri = [p for p in foaf_graph.subjects(RDF.type, FOAF["Person"])][0]
        store_graph = self.select(foaf_uri)
        smobAccount = foaf_uri+"-smob"
        if foaf_uri in store_graph.subjects(RDF.type, FOAF["Person"]):
            if topic not in store_graph.subjects(PUSH['has_subscriber'], URIRef(smobAccount)):
                graph = self.add_topic_has_subscriber(store_graph, foaf_uri, topic, callback, smobAccount)
                self.update(foaf_uri, graph)
        else:
            graph = self.add_push_subscriber(foaf_graph, foaf_uri, topic, callback, smobAccount)
            self.insert(foaf_uri, graph)
        

    def get_or_create_publisher_profile(self, foaf_uri, topic):
        foaf_graph = self.dereference_uri(foaf_uri)
        foaf_uri = [p for p in foaf_graph.subjects(RDF.type, FOAF["Person"])][0]
        store_graph = self.select(foaf_uri)
        smobAccount = foaf_uri+"-smob"
        if foaf_uri in store_graph.subjects(RDF.type, FOAF["Person"]):
            if topic not in store_graph.subjects(PUSH['has_owner'], URIRef(smobAccount)):
                graph = self.add_topic_has_owner(store_graph, topic, smobAccount)
                self.update(foaf_uri, graph)
        else:
            graph = self.add_push_publiher(foaf_graph, uri, topic, smobAccount)
            self.insert(foaf_uri, graph)

def main():
    so = SubscriberProfile()
    so.get_or_create_subscriber_profile("https://localhost/smob/private", "https://localhost/smob/callback", "http://localhost/smob2/me/rss")
    

if __name__ == "__main__":
    main()
