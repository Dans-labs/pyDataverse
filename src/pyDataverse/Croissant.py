#!pip3 install mlcroissant
#!pip3 install python-doi
import mlcroissant as mlc
import requests
import doi
from rdflib import Graph, URIRef, Namespace
import xml.etree.ElementTree as ET
from pyDataverse.api import SearchApi
import re
import json
from datetime import datetime
from dateutil import parser
_PARQUET_FILES = "parquet-files"

class SemanticMappings():
    def __init__(self, data=None, url="https://raw.githubusercontent.com/Dans-labs/pyDataverse/croissant/src/mappings/dataverse_to_croissant.ttl", debug=False):
        self.known = {}
        self.g = Graph()
        self.types = {}
        self.DEBUG = debug
        self.ns = "https://mlcommons.org/croissant"
        if data:
            self.g.parse(data=data, format='turtle')
        else:
            if url:
                self.g.parse(url, format='turtle')
                self.g.bind('rdfs', Namespace('http://www.w3.org/2000/01/rdf-schema#'))
        self.fields = {}
        for fieldname in self.get_fields(self.g, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", SEARCH="PREDICATE", REPEATED=False):
            data = list(self.get_fields(self.g, "%s#%s" % (self.ns, fieldname), SEARCH="SUBJECT", REPEATED=False))
            #canonical_name = data.pop(0)
            #canonical_type = data.pop(1)
            #self.fields[fieldname] = { 'type': canonical_type, 'fields': data }
            if type(data) == list:
                self.fields[fieldname] = data
                try:
                    self.types[fieldname] = data[1]
                except:
                    continue
                #self.fields[fieldname] = { 'type': canonical_type, 'fields': data }
                
    def get_fields(self, g, field_list, SEARCH='PREDICATE', REPEATED=True):
        if not isinstance(field_list, list):
            field_list = [ field_list ]
        
        for fieldname in field_list:
            if self.DEBUG:
                print("Lookup in graph: %s / %s" % (fieldname, field_list))
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
                if self.DEBUG:
                    print(f"Subject: {subject}, Predicate: {predicate}, Object: {obj}")
                try:
                    fielddata.append(obj.value)
                    #return fielddata
                except:
                    fielddata.append(obj.toPython())
            
            if REPEATED:
                if fielddata:
                    try:
                        return fielddata[0]
                    except:
                        #return fielddata
                        c = 1
                #else:
                    #return ''
            else:
                if fielddata:
                    return fielddata
        return ''

class Croissant():
    def __init__(self, host, doi, mappings, debug=False):
        self.known = {}
        self.oai_ore_url = "%s/api/datasets/export?exporter=OAI_ORE&persistentId=%s" % (host, doi)
        self.ddi_url = "%s/api/datasets/export?exporter=ddi&persistentId=%s" % (host, doi)
        self.schema_url = "%s/api/datasets/export?exporter=schema.org&persistentId=%s" % (host, doi)
        self.namespace = {'ns': 'ddi:codebook:2_5'}
        self.variables = []
        self.filevariables = {}
        self.filealias = {}
        self.files = self.get_files()
        self.root = self.read_ddi()
        self.r = requests.get(self.oai_ore_url)
        self.g = Graph()
        self.DEBUG = debug
        self.g.parse(data=self.r.text, format="json-ld")
        #https://dataverse.org/schema/citation/dsDescription#Text
        self.crosswalks = {}
        self.types = {}
        if mappings:
            self.crosswalks = mappings.fields
            self.types = mappings.types
        #self.crosswalks = { "name": "http://purl.org/dc/terms/title", "description": ["https://dataverse.org/schema/citation/dsDescriptionValue", "http://schema.org/description", "https://dataverse.org/schema/citation/dsDescription#Text", "https://dataverse.org/schema/citation/dsDescriptionText"], "url": "http://www.openarchives.org/ore/terms/describes",
        #     "citation": "https://dataverse.org/schema/citation/datasetContactName", "keywords": "https://dataverse.org/schema/citation/keywordValue", "creators": "https://dataverse.org/schema/citation/author#Name",
        #    "author": ["https://dataverse.org/schema/citation/authorName", "https://dataverse.org/schema/citation/author#Name"], "version": "http://schema.org/version", "in_language": ["http://purl.org/dc/terms/language", "https://portal.odissei.nl/schema/dansRights#dansMetadataLanguage"],
        #    "date_modified": "http://schema.org/dateModified", "date_published":"http://schema.org/datePublished", "date_created": "http://purl.org/dc/terms/dateSubmitted", "license": "http://schema.org/license",
        #    "publisher": "https://dataverse.org/schema/citation/productionPlace", "data_type": "http://rdf-vocabulary.ddialliance.org/discovery#kindOfData", "restricted": "https://dataverse.org/schema/core#restricted"
        #     }          
        self.filecrosswalks = { "name": "http://schema.org/name", "description": "http://schema.org/name", "content_url": "http://schema.org/sameAs", "encoding_format": "http://schema.org/fileFormat",
                "md5": "https://dataverse.org/schema/core#checksum", "contentSize": "https://dataverse.org/schema/core#filesize" }
        self.distributions = []
        self.record_sets = []
        self.lastID = False
        self.pointers = []

    def resolver(self, pid):
        if 'doi:' in pid:
            return self.resolve_doi(pid)
        if 'hdl:' in pid:
            return self.resolve_handle(pid)
        return 

    def navigator(self, q=None, base=None, limit=None):
        s = SearchApi(base_url='https://ssh.datastations.nl')
        if limit:
            self.limit = limit
        if not q:
            q = '*'
        try:
            return json.loads(s.search(q, per_page=self.limit, data_type='dataset').text)
        except:
            return
        
    def resolve_doi(self, pid):
        pid = pid.replace('doi:','')
        try:
            resolved_doi = doi.get_real_url_from_doi(pid)
            return resolved_doi
        except Exception as e:
            return f"Error occurred: {e}"
    
    def resolve_handle(self, handle):
        handle=handle.replace('hdl:','')
        resolver_url = f'http://hdl.handle.net/api/handles/{handle}'
        
        try:
            response = requests.get(resolver_url)
            if response.status_code == 200:
                return json.loads(response.text)['values'][0]['data']['value']
            else:
                return f"Failed to resolve Handle {handle}. Status code: {response.status_code}"
        except Exception as e:
            return f"Error occurred: {e}"

    def get_files(self):
        self.files = {}
        r = requests.get(self.schema_url)
        data = json.loads(r.text)
        if 'distribution' in data:
            for line in data['distribution']:
                fileinfo = {}
                if 'contentUrl' in line:
                    uid = re.sub(r'^.+?datafile\/', '', line['contentUrl'])
                    #print(line['contentUrl'])
                    #print(uid)
                    #files["f%s" % uid] = line['contentUrl']
                    fileinfo['contentUrl'] = line['contentUrl']
                    #mainid = line['@id'].replace('https://','').replace('doi.org/','doi:')
                    mainid = line['name']
                    fileinfo['name'] = mainid
                    self.files["f%s" % uid] = fileinfo
                    self.filevariables[mainid] = "f%s" % uid
                    filealias = mainid.split('.')[0]
                    self.filealias[filealias] = mainid
        return self.files

    def normalize(self, datastring, str_type):
        if 'date' in str_type:
            #return datastring
            if isinstance(datastring, list): 
                try:
                    #date_obj = datetime.strptime(datastring[0], '%Y-%m-%d')
                    date_obj = parser.parse(datastring[0])
                except:
                    date_obj = datetime.strptime(datastring[0], '%Y-%m-%d %H:%M:%S.%f')
            else:
                date_obj = datetime.strptime(datastring, '%Y-%m-%d')
            return date_obj.isoformat()
        else:
            return datastring
            
    def read_ddi(self):
        r = requests.get(self.ddi_url)
        self.root = ET.fromstring(r.text)
        var_elements = self.root.findall('.//ns:var', self.namespace)
        for var in var_elements:
            labels = var.findall('./ns:labl', self.namespace)
            thislabel = ''
            for node in labels:
                thislabel = node.text

            locnotes = var.find('./ns:location', self.namespace)
            fileid = locnotes.get('fileid')
            for attribute, value in var.attrib.items():
                # DEBUG
                #print(f"{attribute}: {value}")
                #print("\t%s" % fileid)
                for attribute, value in var.attrib.items():
                    variableinfo = {'var': attribute, 'value': value, 'label': thislabel, 'fileid': fileid}
                    if not variableinfo in self.variables:
                        self.variables.append( variableinfo )

        return self.root

    def fileid_lookup(self, subgraph, tagpoint):
        filename = self.get_fields(subgraph, tagpoint)
        if self.DEBUG:
            print("File TAG %s -> %s" % (tagpoint, filename))
            print(self.filealias)
        if not filename in self.filevariables:
            filealias = filename.split('.')[0]
            if filealias in self.filealias:
                filename = self.filealias[filealias]
                if filename in self.filevariables:
                    return self.filevariables[filename]
        else:
            return self.filevariables[filename]
        
        try:
            fileid = self.filevariables[self.get_fields(subgraph, tagpoint)]
            return fileid
        except:
            return
        
    def get_record(self):
        g = self.g
        #self.printgraph(g)
        self.subcrosswalks = { "distribution": "http://www.openarchives.org/ore/terms/aggregates" }
        self.s = self.subgraph(g, self.subcrosswalks["distribution"])
        self.record_sets: list[mlc.RecordSet] = []
        for i in range(0,len(self.pointers)): #self.pointers:
            pointer = self.pointers[i]
            self.s = self.subgraph(g, pointer, SEARCH="SUBJECT")
            #self.printgraph(self.s)
            #fileid = self.filevariables[self.get_fields(self.s, self.filecrosswalks["name"])]
            fileid = self.fileid_lookup(self.s, self.filecrosswalks["name"])
            if self.DEBUG:
                print("DEBUG file %s" % fileid)
            #print("FILE %s" % self.s, self.filecrosswalks["name"])
            self.distributions.append(
            mlc.FileObject(
                name=self.clean_name_string(self.get_fields(self.s, self.filecrosswalks["name"])),
                id=fileid, #self.files[fileid]['name'],
                description=self.get_fields(self.s, self.filecrosswalks["description"]),
                content_url=self.get_fields(self.s, self.filecrosswalks["content_url"]),
                encoding_format=self.get_fields(self.s, self.filecrosswalks["encoding_format"]),  # No official arff mimetype exist
                md5=self.get_fields(self.s, self.filecrosswalks["md5"]),
                content_size=self.get_fields(self.s, self.filecrosswalks["contentSize"])
            ))

        for variableID in range(0, len(self.variables)):
            variable = self.variables[variableID]
            if self.DEBUG:
                print(variable)
            transforms = []

            self.record_sets.append(mlc.RecordSet(
                #name=variable['value'],  # prefix to avoid duplicate with dataset name
                #description=variable['label'],
                fields=[
                    mlc.Field(
                        #source='source',
                        name=variable['value'],
                        description=variable['label'],
                        data_types=[mlc.DataType.TEXT],
                        source=mlc.Source(
                            id=self.clean_name_string("%s%s" % (variable['fileid'], "_fileobject")),
                            #node_type="distribution",
                            file_object=variable['fileid'],
                            extract=mlc.Extract(column=variable['value']),
                            transforms=transforms,
                        ),
                    ),
                ]))

        #self.record_sets = []
        #print(self.record_sets)
        #print(self.get_fields(g, self.crosswalks["name"]))
        #print(self.get_fields(g, self.crosswalks["description"]))
        #print(self.get_fields(g, self.crosswalks["keywords"], REPEATED=False))
        #print(self.get_fields(g, self.crosswalks["licence"], REPEATED=False))
        if self.DEBUG:
            self.printgraph(g)
        #print("DATE" % str(self.get_fields(g, self.crosswalks["date_published"], REPEATED=False)))
        self.localmetadata = mlc.Metadata(
            cite_as=self.get_fields(g, self.crosswalks["name"]),
            name=self.clean_name_string(self.get_fields(g, self.crosswalks["name"])),
            description=self.get_fields(g, self.crosswalks["description"]),
            #creators=self.get_fields(g, self.crosswalks["creators"], REPEATED=False),
            creators=self.get_fields(g, self.crosswalks["author"], REPEATED=False),
            url=self.get_fields(g, self.crosswalks["url"]),
            #date_created=self.get_fields(g, self.crosswalks["date_created"], REPEATED=False),
            #date_published=self.get_fields(g, self.crosswalks["date_published"], REPEATED=False),
            #date_modified=self.get_fields(g, self.crosswalks["date_modified"], REPEATED=False),
            keywords=self.get_fields(g, self.crosswalks["keywords"], REPEATED=False),
            publisher=self.get_fields(g, self.crosswalks["publisher"], REPEATED=False),
            #citation=get_fields(g, crosswalks["citation"]),
            license=self.get_fields(g, self.crosswalks["license"], REPEATED=False),
            sd_licence=self.get_fields(g, self.crosswalks["license"], REPEATED=False),
            version=self.get_fields(g, self.crosswalks["version"], REPEATED=True),
            distribution=self.distributions,
            record_sets=self.record_sets,
            in_language=self.get_fields(g, self.crosswalks["in_language"], REPEATED=False),
            #data_collection_type=self.get_fields(g, self.crosswalks["data_type"], REPEATED=True).split(','),
            #data_sensitive=self.get_fields(g, self.crosswalks["restricted"], REPEATED=False)
            #data_sensitive={'file': 'restricted'}
        )

        metadatajson  = self.localmetadata.to_json()
        self.senslocalmetadata = mlc.Metadata(
            data_sensitive={'file': 'restricted'}
        )
        for property in self.crosswalks:
            if 'date' in property:
                if self.DEBUG:
                    print("### DEBUG %s" % self.types[property])
                print(self.normalize(self.get_fields(g, self.crosswalks[property], REPEATED=False), self.types[property]))
            
                self.custommetadata = mlc.Metadata(
                    date_created = self.normalize(self.get_fields(g, self.crosswalks[property], REPEATED=False), self.types[property])
                )
                metadatajson  = { **metadatajson, **self.custommetadata.to_json() }
            
            
        #metadatajson  = { **self.localmetadata.to_json(), **self.senslocalmetadata.to_json() }

#        return self.localmetadata.to_json()
        return metadatajson
        
    def clean_name_string(self, name):
        name = name.replace('-','_')
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
            
    def get_fields(self, g, field_list, SEARCH='PREDICATE', REPEATED=True):
        if not isinstance(field_list, list):
            field_list = [ field_list ]
        
        for fieldname in field_list:
            #print("Lookup in graph: %s / %s" % (fieldname, field_list))
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
                #print(f"*** Subject: {subject}, Predicate: {predicate}, Object: {obj}")
                try:
                    #fielddata.append(f"{obj}")
                    #return fielddata
                    fielddata.append(obj.value)
                except:
                    fielddata.append(obj.toPython())
            if REPEATED:
                if fielddata:
                    try:
                        return fielddata[0]
                    except:
                        #return fielddata
                        c = 1
                #else:
                    #return ''
            else:
                if fielddata:
                    return fielddata
        return ''
