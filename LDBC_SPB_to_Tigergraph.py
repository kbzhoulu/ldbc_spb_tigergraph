#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#### Imports
# Python library
import os
from datetime import datetime
# Graph stuff
import rdflib 
from rdflib import Graph, OWL, RDF, RDFS
# Tigergraph library
import pyTigerGraph as tg
# pandas process dataset
import pandas as pd
import csv
# hash long strings
import hashlib


# In[ ]:


##### Logging
log_output = open("./data/output/LDBC_SPB-To-Tigergraph-log.log","w")
def log(msg,tab_level=0,output=log_output):
    timestamp = datetime.now()
    tabs = "\t" * (tab_level+1)
    message = f"{timestamp}{tabs}{msg}"
    print(message)
    output.write(f"{message}\n")


# In[ ]:


#### Paths
# The directory hosting all input data
data_path = "./data/input/"
# The directory hosting all output data
output_path = "./data/output/"


# In[ ]:


#### download and preprocess data 
# running sparql queries on your triple store via the sparql endpoint, e.g., installing ontotext graphdb on 
# your local machine and run the sparql queries via the endpoint http://localhost:7200/repositories/ldbc-spb 
# or you can directly run the query using the webpage and download it directly
# note: downloading all triples is time-consuming, the query might take a while to run
g = Graph()

log("running sparql to get all triple with object properties")

objectPropertyTriples = """
    SELECT ?s ?p ?o (CONCAT(str(?s), str(?p), str(?o)) as ?c)
    WHERE {
    SERVICE <http://localhost:7200/repositories/ldbc-spb> {
        ?s ?p ?o .
    }
    filter (!isLiteral(?o)) .
    }    
"""

objectproperty = g.query(objectPropertyTriples)

log("running successfully")

log("running sparql to get all triple with datatype property properties")

datatypePropertyTriples = """
    SELECT ?s ?p ?o (CONCAT(str(?s), str(?p), str(?o)) as ?c) (datatype(?o) as ?d) 
    WHERE {
    SERVICE <http://localhost:7200/repositories/ldbc-spb> {
        ?s ?p ?o .
    }
    filter isLiteral(?o) .
    }    
"""

datatypeproperty = g.query(datatypePropertyTriples)

log("running successfully")

datatype_file = os.path.join(data_path, "tigergraph/datatype.csv")
object_file = os.path.join(data_path, "tigergraph/object.csv")

with open(datatype_file, "w") as f:
    writer = csv.DictWriter(f, fieldnames=[str(v) for v in datatypeproperty.vars])
    writer.writeheader()
    for binding in datatypeproperty.bindings:
        writer.writerow({str(k): str(v) for k, v in binding.items()})
        
with open(object_file, "w") as f1:
    writer = csv.DictWriter(f1, fieldnames=[str(v) for v in objectproperty.vars])
    writer.writeheader()
    for binding in objectproperty.bindings:
        writer.writerow({str(k): str(v) for k, v in binding.items()})


# In[ ]:


#### read the csv file in to dataframe
# datatype property triples
log("reading triples from csv files")
datatype_file = os.path.join(data_path, "tigergraph/datatype.csv")
datatype_df = pd.read_csv(datatype_file, delimiter=',', header=0, low_memory=False)
# object property triples
object_file = os.path.join(data_path, "tigergraph/object.csv")
object_df = pd.read_csv(object_file, delimiter=',', header=0, low_memory=False)

log("load csv file into dataframe successfully")


# In[ ]:


#### convert the long IDs to hash
# datatype property id
log("hashing the long id")

datatype_df['dpiid'] = datatype_df.apply(lambda row: hashlib.md5(str(row.c).encode()).hexdigest(), axis = 1)
# value id
datatype_df['viid'] = datatype_df.apply(lambda row: hashlib.md5(str(row.o).encode()).hexdigest(), axis = 1)
# object property Id
object_df['id'] = object_df.apply(lambda row: hashlib.md5(str(row.c).encode()).hexdigest(), axis = 1)

log("generate hashsed IDs successfully")


# In[ ]:


#### save to files 
log("saving generated IDs with data to csv files")
# object property triples with hashed IDs
object_df.to_csv('./data/input/tigergraph/object_hashed.csv')
# datatype property triples with hashed IDs
datatype_df.to_csv('./data/input/tigergraph/datatype_hashed.csv')

log("saved successfully")


# In[ ]:


#### Tigergraph Solution Connection
log("connecting to graph solution.")

# Connection parameters
# Configure to your solution
hostname = "https://XXXXXXX.i.tgcloud.io"
username = "XXXXXXXX"
password = "XXXXXXXX"

conn = tg.TigerGraphConnection(host=hostname, username=username, password=password)

log("successfully connect to solution")


# In[ ]:


#### Create graph schema in tigergraph 
# create nodes and edges 
log("creating ldbc_spb schema in Tigergraph.")

results = conn.gsql(
    '''
    USE GLOBAL
    
    CREATE VERTEX ClassInstance (primary_id id STRING, uri STRING)
    CREATE VERTEX ObjectPropertyInstance (primary_id id STRING, uri STRING)
    CREATE VERTEX DatatypePropertyInstance (primary_id id STRING, uri STRING)
    CREATE VERTEX ValueInstance (primary_id id STRING, value STRING, datatype STRING)

    CREATE DIRECTED EDGE hasDatatypePropertyInstance (FROM ClassInstance, TO DatatypePropertyInstance) WITH REVERSE_EDGE="reverse_hasDatatypePropertyInstance"
    CREATE DIRECTED EDGE hasObjectPropertyInstance (FROM ClassInstance, TO ObjectPropertyInstance) WITH REVERSE_EDGE="reverse_hasObjectPropertyInstance"
    CREATE DIRECTED EDGE hasObjectInstance (FROM ObjectPropertyInstance, TO ClassInstance) WITH REVERSE_EDGE="reverse_hasObjectInstance"
    CREATE DIRECTED EDGE hasValueInstance (FROM DatatypePropertyInstance, TO ValueInstance) WITH REVERSE_EDGE="reverse_hasValueInstance"
    
    CREATE GLOBAL SCHEMA_CHANGE JOB attribute_index {
        ALTER VERTEX ClassInstance ADD INDEX ClassInstance_uri_index ON (uri);
        ALTER VERTEX ObjectPropertyInstance ADD INDEX ObjectPropertyInstance_uri_index ON (uri);
        ALTER VERTEX DatatypePropertyInstance ADD INDEX DatatypePropertyInstance_uri_index ON (uri);
        ALTER VERTEX ValueInstance ADD INDEX ValueInstance_value_index ON (value);
    }
    
    RUN GLOBAL SCHEMA_CHANGE JOB attribute_index
    CREATE GRAPH ldbc_spb(*)
    '''
)

log(results)


# In[ ]:


#### Map data to the schema
log("Maping LDBC SPB data to Tigergraph schema.")

results = conn.gsql('''
    USE GRAPH ldbc_spb
    
    CREATE LOADING JOB load_data FOR GRAPH ldbc_spb {
    DEFINE FILENAME OP;
    DEFINE FILENAME DP;

    LOAD OP TO EDGE hasObjectPropertyInstance VALUES($1, $5) USING SEPARATOR=",", HEADER="true", EOL="\\n", QUOTE="double";
    LOAD OP TO EDGE hasObjectInstance VALUES($5, $3) USING SEPARATOR=",", HEADER="true", EOL="\\n", QUOTE="double";
    LOAD OP TO VERTEX ClassInstance VALUES($1, $1) USING SEPARATOR=",", HEADER="true", EOL="\\n", QUOTE="double";
    LOAD OP TO VERTEX ObjectPropertyInstance VALUES($5, $2) USING SEPARATOR=",", HEADER="true", EOL="\\n", QUOTE="double";
    LOAD OP TO VERTEX ClassInstance VALUES($3, $3) USING SEPARATOR=",", HEADER="true", EOL="\\n", QUOTE="double";

    LOAD DP TO EDGE hasDatatypePropertyInstance VALUES($1, $6) USING SEPARATOR=",", HEADER="true", EOL="\\n", QUOTE="double";
    LOAD DP TO EDGE hasValueInstance VALUES($6, $7) USING SEPARATOR=",", HEADER="true", EOL="\\n", QUOTE="double";
    LOAD DP TO VERTEX ClassInstance VALUES($1, $1) USING SEPARATOR=",", HEADER="true", EOL="\\n", QUOTE="double";
    LOAD DP TO VERTEX DatatypePropertyInstance VALUES($6, $2) USING SEPARATOR=",", HEADER="true", EOL="\\n", QUOTE="double";
    LOAD DP TO VERTEX ValueInstance VALUES($7, $3, $5) USING SEPARATOR=",", HEADER="true", EOL="\\n", QUOTE="double";

    LOAD OP TO VERTEX ClassInstance VALUES($3, $1) USING SEPARATOR=",", HEADER="true", EOL="\\n", QUOTE="double";
    }
    
    RUN LOADING JOB load_data USING OP="ANY:object_hashed.csv", DP="ANY:datatype_hashed.csv"
    '''
)

log(results)


# In[ ]:


#### Connect to the graph with apiToken
graphname = "XXXXXXX"

conn.graphname = graphname
secret = conn.createSecret()
authToken = conn.getToken(secret)[0]

conn = tg.TigerGraphConnection(host=hostname, username=username, password=password, graphname=graphname, apiToken=authToken)


# In[ ]:


#### Load data to the graph
log("Loading ldbc spb object property data to Tigergraph schema.")

objectproperty_data_file = "./data/input/tigergraph/object_hashed.csv"
datatypeproperty_data_file = "./data/input/tigergraph/datatype_hashed.csv"

results = conn.uploadFile(objectproperty_data_file, fileTag='OP', jobName='load_data')
log(results)

log("Loading ldbc spb datatype property data to Tigergraph schema.")
results = conn.uploadFile(datatypeproperty_data_file, fileTag='DP', jobName='load_data')

log(results)


# In[ ]:


#### create queries
log("Create GSQL query")

basic_path = data_path + "queries/gsql/basic"

advanced_path = data_path + "queries/gsql/advanced"

def createQuery(file_path):
    with open(file_path, 'r') as f:
        query = f.read()
        results = conn.gsql(query)
        log(results)

for file in os.listdir(basic_path):
    if file.endswith(".txt"):
        file_path = f"{basic_path}/{file}"
        createQuery(file_path)
        
for file in os.listdir(advanced_path):
    if file.endswith(".txt"):
        file_path = f"{advanced_path}/{file}"
        createQuery(file_path)


# In[ ]:


# Install queries 
log("Installig all queries")

results = conn.gsql("""
    USE GRAPH ldbc_spb
    INSTALL QUERY ALL
""")

log(results)


# In[ ]:


#### initialize the parameters and run basic queries

# create a dictionary to store the queryname -> runtime pair
runtimes = {}

# execute 11 basic queries
for i in range(1, 12):
    starttime = datetime.now()
    results = conn.runInstalledQuery("basic_query" + str(i) + "_optimized")
    endtime = datetime.now()
    runtime = endtime - starttime
    runtimes["basic_query" + str(i) + "_optimized"] = runtime.total_seconds()

print("running 11 basic queries successfully")


# In[ ]:


#### initialize the parameters and run advanced queries

# execute 25 advanced queries
for i in range(1, 26):

    starttime = datetime.now()
    results = conn.runInstalledQuery("advanced_query" + str(i) + "_optimized")
    endtime = datetime.now()  
    runtime = endtime - starttime
    runtimes["advanced_query" + str(i) + "_optimized"] = runtime.total_seconds()

print("running 25 advanced queries successfully")


# In[ ]:


# print out the query performance 
for x, y in runtimes.items():
    print(x, y)

