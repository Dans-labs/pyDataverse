#!pip3 install mlcroissant
import mlcroissant as mlc
import requests
from rdflib import Graph, URIRef
import re

class Croissant():
    def __init__(self, host, doi, debug=False):
        self.known = {}
        self.url = "%s/api/datasets/export?exporter=OAI_ORE&persistentId=%s" % (host, doi)
        self.r = requests.get(self.url)
        self.g = Graph()
        self.DEBUG = debug
        self.g.parse(data=self.r.text, format="json-ld")
        self.crosswalks = { "name": "http://purl.org/dc/terms/title", "description": "https://dataverse.org/schema/citation/dsDescriptionValue", "url": "http://www.openarchives.org/ore/terms/describes",
             "citation": "https://dataverse.org/schema/citation/datasetContactName", "licence": "schema:license", "keywords": "https://dataverse.org/schema/citation/keywordValue"
             }          
        self.filecrosswalks = { "name": "http://schema.org/name", "description": "http://schema.org/name", "content_url": "http://schema.org/sameAs", "encoding_format": "http://schema.org/fileFormat",
                "md5": "https://dataverse.org/schema/core#checksum" }
        self.distributions = []
        self.record_sets = []
        self.lastID = False
        self.pointers = []

    def get_record(self):
        g = self.g
        self.subcrosswalks = { "distribution": "http://www.openarchives.org/ore/terms/aggregates" }
        self.s = self.subgraph(g, self.subcrosswalks["distribution"])
        for i in range(0,len(self.pointers)): #self.pointers:
            pointer = self.pointers[i]
            self.s = self.subgraph(g, pointer, SEARCH="SUBJECT")
            #print(self.get_fields(self.s, self.filecrosswalks["name"]))
            self.distributions.append(
            mlc.FileObject(
                name=self.clean_name_string(self.get_fields(self.s, self.filecrosswalks["name"])),
                description=self.get_fields(self.s, self.filecrosswalks["description"]),
                content_url=self.get_fields(self.s, self.filecrosswalks["content_url"]),
                encoding_format=self.get_fields(self.s, self.filecrosswalks["content_url"]),  # No official arff mimetype exist
                md5=self.get_fields(self.s, self.filecrosswalks["md5"])
            ))
        
        self.localmetadata = mlc.Metadata(
            name=self.clean_name_string(self.get_fields(g, self.crosswalks["name"])),
            description=self.get_fields(g, self.crosswalks["description"]),
            url=self.get_fields(g, self.crosswalks["url"]),
            keywords=self.get_fields(g, self.crosswalks["keywords"], REPEATED=False),
            #citation=get_fields(g, crosswalks["citation"]),
            license=self.get_fields(g, self.crosswalks["licence"], REPEATED=False),
            distribution=self.distributions,
            record_sets=self.record_sets,
        )

        return self.localmetadata.to_json()
        
    def clean_name_string(self, name):
        return re.sub("[^a-zA-Z0-9\\-_.]", "_", name)
        
    def subgraph(self, g, property_to_find, SEARCH='PREDICATE'):
        search_property = property_to_find
        subgraph = Graph()
        if search_property:
            if 'http' in property_to_find:
                search_property = URIRef(property_to_find)
            if self.DEBUG:
                print("Finding %s" % SEARCH)
            if SEARCH == 'PREDICATE':
                data = g.triples((None, search_property, None))
            if SEARCH == 'SUBJECT':
                if type(search_property) == str:
                    search_property = URIRef(property_to_find)
                data = g.triples((search_property, None, None))
            if SEARCH == 'OBJECT':
                data = g.triples((None, None, search_property))
                
            for s, p, o in data:
                subgraph.add((s, p, o))
                self.lastID = o
                self.pointers.append(o)
                if self.DEBUG:
                    print("%s %s %s" % (s, p, o))
            return subgraph
        
    def printgraph(self, g):
        for subject, predicate, obj in g:
            print("%s %s %s" % (subject, predicate, obj))
        return
            
    def get_fields(self, g, fieldname, SEARCH='PREDICATE', REPEATED=True):
        if fieldname:
            search_property = fieldname
            if 'http' in fieldname:
                search_property = URIRef(fieldname)
            fielddata = []
            data = []
            if self.DEBUG:
                print(SEARCH)
            if SEARCH == 'PREDICATE':
                data = g.triples((None, search_property, None))
            if SEARCH == 'SUBJECT':
                data = g.triples((search_property, None, None))
            if SEARCH == 'OBJECT':
                data = g.triples((None, None, search_property))
        
            for subject, predicate, obj in data:
                #print(f"Subject: {subject}, Predicate: {predicate}, Object: {obj}")
                try:
                    fielddata.append(obj.value)
                    #return fielddata
                except:
                    fielddata.append(obj.toPython())
            if REPEATED:
                return fielddata[0]
            else:
                return fielddata
