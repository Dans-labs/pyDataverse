#!pip3 install mlcroissant
#!pip3 install python-doi
import mlcroissant as mlc
import requests
import doi
from rdflib import Graph, URIRef
import xml.etree.ElementTree as ET
import re
import json
_PARQUET_FILES = "parquet-files"

class Croissant():
    def __init__(self, host, doi, debug=False):
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
        self.knownids = {}
        self.g.parse(data=self.r.text, format="json-ld")
        self.crosswalks = { "name": "http://purl.org/dc/terms/title", "description": ["https://dataverse.org/schema/citation/dsDescriptionValue", "http://schema.org/description", "https://dataverse.org/schema/citation/dsDescription#Text", "https://dataverse.org/schema/citation/dsDescriptionText"], "url": "http://www.openarchives.org/ore/terms/describes",
             "citation": "https://dataverse.org/schema/citation/datasetContactName", "keywords": "https://dataverse.org/schema/citation/keywordValue", "creators": "https://dataverse.org/schema/citation/author#Name",
            "author": ["https://dataverse.org/schema/citation/authorName", "https://dataverse.org/schema/citation/author#Name"], "version": "http://schema.org/version", "in_language": ["http://purl.org/dc/terms/language", "https://portal.odissei.nl/schema/dansRights#dansMetadataLanguage"],
            "date_modified": "http://schema.org/dateModified", "date_published":"http://schema.org/datePublished", "date_created": "http://purl.org/dc/terms/dateSubmitted", "license": "http://schema.org/license",
            "publisher": "https://dataverse.org/schema/citation/productionPlace", "data_type": "http://rdf-vocabulary.ddialliance.org/discovery#kindOfData", "restricted": "https://dataverse.org/schema/core#restricted"
             }          
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
                    fileinfo['contentUrl'] = line['contentUrl']
                    mainid = line['name']
                    fileinfo['name'] = mainid
                    self.files["f%s" % uid] = fileinfo
                    self.filevariables[mainid] = "f%s" % uid
                    filealias = mainid.split('.')[0]
                    self.filealias[filealias] = mainid
        return self.files
        
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
                for attribute, value in var.attrib.items():
                    variableinfo = {'var': attribute, 'value': value, 'label': thislabel, 'fileid': fileid}
                    if not variableinfo in self.variables:
                        self.variables.append( variableinfo )

        return self.root

    def fileid_lookup(self, subgraph, tagpoint):
        filename = self.get_fields(subgraph, tagpoint)
        if not filename:
            return
        if not filename in self.filevariables:
            filealias = filename.split('.')[0]
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
        #self.knownids = {}
        self.subcrosswalks = { "distribution": "http://www.openarchives.org/ore/terms/aggregates" }
        self.filesgraph = self.subgraph(g, self.subcrosswalks["distribution"])
        self.filepointers = self.pointers
        #self.printgraph(self.filesgraph)
        self.record_sets: list[mlc.RecordSet] = []
        for i in range(0,len(self.filepointers)): #self.pointers:
            pointer = self.filepointers[i]
            self.s = self.subgraph(g, pointer, SEARCH="SUBJECT")
            #self.printgraph(self.s)
            fileid = self.fileid_lookup(self.s, self.filecrosswalks["name"])
            if not fileid in self.knownids: # and fileid:
                self.knownids[fileid] = True
                if fileid:
                    self.distributions.append(
                    mlc.FileObject(
                name="%s" % self.clean_name_string(self.get_fields(self.s, self.filecrosswalks["name"])),
                id="%s" % (fileid), #self.files[fileid]['name'],
                description=self.get_fields(self.s, self.filecrosswalks["description"]),
                content_url=self.get_fields(self.s, self.filecrosswalks["content_url"]),
                encoding_format=self.get_fields(self.s, self.filecrosswalks["encoding_format"]),  # No official arff mimetype exist
                md5=self.get_fields(self.s, self.filecrosswalks["md5"]),
                content_size=self.get_fields(self.s, self.filecrosswalks["contentSize"])
                    ))

        for variableID in range(0, len(self.variables)):
            variable = self.variables[variableID]
            transforms = []

            self.record_sets.append(mlc.RecordSet(
                fields=[
                    mlc.Field(
                        #source='source',
                        name=str(variable['value']),
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

        #print(self.get_fields(g, self.crosswalks["date_created"]))
        self.localmetadata = mlc.Metadata(
            cite_as=self.get_fields(g, self.crosswalks["name"]),
            name=str(self.clean_name_string(self.get_fields(g, self.crosswalks["name"]))),
            description=self.get_fields(g, self.crosswalks["description"]),
            #creators=self.get_fields(g, self.crosswalks["creators"], REPEATED=False),
            creators=self.get_fields(g, self.crosswalks["author"], REPEATED=False),
            url=self.get_fields(g, self.crosswalks["url"]),
            date_created=self.get_fields(g, self.crosswalks["date_created"]),
            date_published=self.get_fields(g, self.crosswalks["date_published"]),
            #date_modified=self.get_fields(g, self.crosswalks["date_modified"]),
            keywords=self.get_fields(g, self.crosswalks["keywords"], REPEATED=False),
            publisher=self.get_fields(g, self.crosswalks["publisher"], REPEATED=False),
            #citation=get_fields(g, crosswalks["citation"]),
            license=self.get_fields(g, self.crosswalks["license"], REPEATED=False),
            #sd_licence=self.get_fields(g, self.crosswalks["license"], REPEATED=False),
            version=self.get_fields(g, self.crosswalks["version"], REPEATED=True),
            is_live_dataset=True,
            distribution=self.distributions,
            record_sets=self.record_sets,
            in_language=self.get_fields(g, self.crosswalks["in_language"], REPEATED=False),
            data_collection_type=self.get_fields(g, self.crosswalks["data_type"], REPEATED=True).split(','),
            data_sensitive=self.get_fields(g, self.crosswalks["restricted"], REPEATED=False)
            #data_sensitive={'file': 'restricted'}
        )

        return self.localmetadata.to_json()
        
    def clean_name_string(self, name):
        name = name.replace('-','_')
        return re.sub("[^a-zA-Z0-9\\-_.]", "_", name)
        
    def subgraph(self, g, property_to_find, SEARCH='PREDICATE'):
        self.pointers = []
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
                self.pointers.append(o) #o.toPython())
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
                #print(f"Subject: {subject}, Predicate: {predicate}, Object: {obj}")
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
