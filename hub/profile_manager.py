import logging
import sys
import rdflib
from rdflib.graph import Graph
from rdflib.term import URIRef, Literal, BNode
from rdflib.namespace import Namespace, RDF
from rdflib import plugin
from SPARQLWrapper import SPARQLWrapper, JSON, XML, N3
from rdflib import Namespace

from get_private_webid_uri import get_private_uri
from google.appengine.api import urlfetch
import urllib


# Configure how we want rdflib logger to log messages
_logger = logging.getLogger("rdflib")
_logger.setLevel(logging.DEBUG)
_hdlr = logging.StreamHandler()
_hdlr.setFormatter(logging.Formatter('%(name)s %(levelname)s: %(message)s'))
_logger.addHandler(_hdlr)


#FIXME: move namespaces to other file
#Add Namespaces here to use it through out the file
FOAF = Namespace("http://xmlns.com/foaf/0.1/")
PUSH = Namespace("http://vocab.deri.ie/push/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
CERT = Namespace("http://www.w3.org/ns/auth/cert#")
RSA = Namespace("http://www.w3.org/ns/auth/rsa#")
REL = Namespace('http://purl.org/vocab/relationship/')

ns_profile = dict(foaf=FOAF, rel=REL, rdfs=RDFS, rdf=RDF)
ns_store = dict(foaf=FOAF, push=PUSH, rdfs=RDFS, rdf=RDF, cert=CERT, rsa=RSA)

#FIXME: move config constants to other file
DEBUG = False
ENDPOINT_DATA = "http://localhost:8001/data/"
SPARQL_ENDPOINT = "http://localhost:8001/sparql/"
ENDPOINT_UPDATE = "http://localhost:8001/update/"

HUB_CERTIFICATE_URI = 'http://localhost:8080/cert'
HUB_CERTIFICATE_P12 = 'hub_cert.p12'
HUB_CERTIFICATE = 'hub_cert.pem'
HUB_KEY = 'hub_key.key'
HUB_WEBID = 'http://localhost:8080/me'



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

def jsondict2graph(json_dict):
    g = Graph()
    [g.bind(*x) for x in ns_store.items()]
    for triple in json_dict['results']['bindings']:
        ts = triple['s'].get('type',None)
        vs = triple['s']['value']
        if ts == 'uri':
            s = URIRef(vs)
        elif ts == 'literal':
            s = Literal(vs)
        elif ts == 'bnode':
            s = BNode(vs)
        #logging.debug(s)
        
        p = URIRef(triple['p']['value'])
        #logging.debug(p)
        
        to = triple['o'].get('type',None)
        vo = triple['o']['value']
        dto = triple['o'].get('datatype',None)
        if to == 'uri':
            o = URIRef(triple['o']['value'])
        elif to == 'literal':
            o = Literal(triple['o']['value'])
            if dto:
                o.datatype = URIRef(dto)
        elif ts == 'bnode':
            o = BNode(vo)
        #logging.debug(o)
        g.add((s,p,o))
    logging.debug(g.serialize(format='turtle'))
    return g

class ProfileManager:

    def create_callback(self, rpc):
        response = rpc.get_result()
        return response.content

    def get_graph_from_uri(self, foaf_uri):
        graph = Graph()
        [graph.bind(*x) for x in ns_profile.items()]

        # getting the public foaf and inserting into the graph

        # FIXME
        # cant retrieve directly the url, GAE doesn't allow open sockets:
        # sock = socket.create_connection((self.host, self.port),
        # AttributeError: 'module' object has no attribute 'create_connection'
        #store.parse(location)
        #response = get_private_uri(foaf_uri, HUB_CERTIFICATE, HUB_KEY)
        #foaf = response.read()

        # workaround using GAE urlfetch...
        #response = urlfetch.fetch(foaf_uri, validate_certificate=False)
        # passing certificate in another manual way...
        
        # not able either to open files
        #raise IOError(errno.EACCES, 'file not accessible', filename)
        #cert_file = open(HUB_CERTIFICATE_P12)
        #cert_data = cert_file.read()
        #response = urlfetch.fetch(foaf_uri, 
        #                          payload = cert_data
        #                          method=urlfetch.POST,
        #                          headers={'Content-Type': "application/x-x509-user-cert",
        #                                   'webid_uri': HUB_WEBID},
        #                          validate_certificate=False)
        
        # timeout due to it is processing the subscriber cert and webid requests?
        form_fields = {"cert_uri": HUB_CERTIFICATE_URI, "webid_uri": HUB_WEBID}
        form_data = urllib.urlencode(form_fields)
        response = urlfetch.fetch(url=foaf_uri,
                        payload=form_data,
                        method=urlfetch.POST,
                        headers={'Content-Type': 'application/x-www-form-urlencoded'},
                        validate_certificate=False,
                        deadline=60)

        # assert self.__rpc.state != apiproxy_rpc.RPC.IDLE, repr(self.state)
        # AssertionError: 0
        #rpc = urlfetch.create_rpc()
        #rpc.callback = self.create_callback(rpc)
        #rpc.deadline = 60
        #urlfetch.make_fetch_call(rpc = rpc,
        #                url=foaf_uri,
        #                payload=form_data,
        #                method=urlfetch.POST,
        #                headers={'Content-Type': 'application/x-www-form-urlencoded'},
        #                follow_redirects = True)
        #rpc.wait()

        foaf = response.content
        content_type = response.headers['content-type'].split(';')[0]
        logging.info(content_type)
        #logging.debug(response.status_code)
        #if response.status_code == 200:
        logging.info("foaf: " + foaf)


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

    def select_callback(self, restriction):
        #FIXME: refactor
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
        sparql.setQuery(query)
        logging.debug('Query: %r', query)
        uris = []
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        for result in results["results"]["bindings"]:
            uris.append(result["callback"]["value"])
        return uris

    def select(self, graph_uri):
        graph = Graph()
        #FIXME: hackish
        graph_uri = graph_uri.rstrip('#me')
        logging.debug("graph uri"+graph_uri)
        query = """
SELECT * WHERE { GRAPH <""" + graph_uri + """> { ?s ?p ?o } }"""
        logging.debug("going to execute " + query)
        sparql = SPARQLWrapper(SPARQL_ENDPOINT)
        sparql.setQuery(query)
        #sparql.setReturnFormat('rdf')
        # can't check content-type, so json
        sparql.setReturnFormat(JSON)
        try:
            ret = sparql.query()
            json_dict = ret.convert()
            logging.debug(json_dict)
            graph = jsondict2graph(json_dict)
            logging.debug(graph)
            return graph
        except:
            sys.exc_info()
            logging.exception("WHAT HAPPENED?")

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
        foaf_graph = self.get_graph_from_uri(foaf_uri)
        foaf_uri = [p for p in foaf_graph.subjects(RDF.type, FOAF["Person"])][0]
        store_graph = self.select(foaf_uri)
        smobAccount = foaf_uri+"-smob"
        if store_graph:
            if foaf_uri in store_graph.subjects(RDF.type, FOAF["Person"]):
                logging.debug("%s was already in the store" % foaf_uri) 
                if topic not in store_graph.subjects(PUSH['has_subscriber'], URIRef(smobAccount)):
                    logging.debug("%s was not yet in the store" % topic) 
                    graph = self.add_topic_has_subscriber(store_graph, foaf_uri, topic, callback, smobAccount)
                    self.update(foaf_uri, graph)
        else:
            graph = self.add_push_subscriber(foaf_graph, foaf_uri, topic, callback, smobAccount)
            self.insert(foaf_uri, graph)
            logging.debug("Added %s to the store" % foaf_uri) 
        

    def get_or_create_publisher_profile(self, foaf_uri, topic):
        foaf_graph = self.get_graph_from_uri(foaf_uri)
        foaf_uri = [p for p in foaf_graph.subjects(RDF.type, FOAF["Person"])][0]
        store_graph = self.select(foaf_uri)
        smobAccount = foaf_uri+"-smob"
        if store_graph:
            if foaf_uri in store_graph.subjects(RDF.type, FOAF["Person"]):
                logging.debug("%s was already in the store" % foaf_uri) 
                if topic not in store_graph.subjects(PUSH['has_owner'], URIRef(smobAccount)):
                    logging.debug("%s was not yet in the store" % topic) 
                    graph = self.add_topic_has_owner(store_graph, topic, smobAccount)
                    self.update(foaf_uri, graph)
        else:
            graph = self.add_push_publiher(foaf_graph, foaf_uri, topic, smobAccount)
            self.insert(foaf_uri, graph)
            logging.debug("Added %s to the store" % foaf_uri) 

def main():
    so = SubscriberProfile()
    so.get_or_create_subscriber_profile("https://localhost/smob/private", "https://localhost/smob/callback", "http://localhost/smob2/me/rss")
    

if __name__ == "__main__":
    main()
