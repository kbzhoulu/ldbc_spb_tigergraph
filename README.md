# RDF2TigerGraph: Towards Supporting RDF in TigerGraph Property Graph Database
Both RDF and property graph models provide ways to explore and graphically depict connected data. But the two graph models are very different, while each has different strengths in different use cases. RDF is a standard model for data interchange on the Web. RDF has features that facilitate data integration even if the underlying schemas of data sources are different. With the support of RDF Schema (RDFS) and Web Ontology Language (OWL), it enables the inference engine to discover more implicit knowledge in the graph. In contrast, property graphs were developed about efficient storage that would allow for fast querying and traversals across connected data. However, once after choosing one graph model, it is not convenient to losslessly transfer it to another one due to different reasons, e.g, time and cost consumption, technical challenges in converting between two graph models, and different query languages. To provide solutions to customers wanting to migrate their existing RDF graph models to property graph models, we propose a method to migrate [LBDC Semantic Publishing RDF Benchmark (SPB)](https://ldbcouncil.org/benchmarks/spb/) to property graphs using TigerGraph. In addition to mapping about 32 million triples, we also translate 36 SPARQL queries to GSQL, which is the query language used in the TigerGraph database, with an initial analysis of query performance.

## Reproducibility
* LDBC SPB benchmark generator instruction

	Please see details in [SPB sepecification](https://ldbcouncil.org/benchmarks/spb/ldbc-spb-v2.0-specification.pdf)
	We upload the generated RDF graph in turtle format which can be directly used. 

* Create Schema, Map data, Load CSV file, Run queries, Evaluate performance

	Run the ipynb file in Jupyter Notebook or run the python code directly 

## Data Sources
[LDBC Semantic Publishing Benchmark](https://ldbcouncil.org/benchmarks/spb/)

## Dependencies
rdflib, pytigergraph, pandas, hashlib, csv

## Graph Model Mapping
Mapping Rules and examples from RDF graphs to Tigergraph
```
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
@prefix ex: <http://example.com/> 
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>

ex:Tigergraph rdf:type ex:Private Company .
ex:Tigergraph ex:hasName "Tigergraph"^^xsd:string .
ex:Tigergraph ex:hasName "Tigergraph"@en .
```
| Mapping Rules | RDF Graphs | TigerGraph | Examples |
|:---:|:---:|:---:|:---:|
| Rule 1 | Subject | ClassInstance | ex:Tigergraph |
| Rule 2 | Predicate (Object Property) | ObjectPropertyInstance | rdf:type |
| Rule 3 | Predicate (Datatype Property) | DatatypePropertyInstance | ex:hasName |
| Rule 4 | Object (!isLiteral) | ClassInstance | ex:Private Company |
| Rule 5 | Object (isLiteral) | ValueInstance | "Tigergraph"^^xsd:string |
| Rule 6 | Literal Value | ValueInstance's attribute: value | Tigergraph |
| Rule 7 | Datatype | ValueInstance's attribute: datatype | xsd:string |
| Rule 8 | Language Tag | ValueInstance's attribute: langTag | en |

## Example Graphs in Diagram

![Schema Diagram in RDF graphs](./screenshots/rdfgraph.png)

*Figure 1. Example triples in RDF Graph*

![Schema Diagram in TigerGraph](./screenshots/tigergraph.png)

*Figure 2. Example triples in Tigergraph*

## SPARQL to GSQL Translation
Mappings and Examples:

| Mapping Rules | SPAQRL | GSQL | SPAQRL Examples | GSQL Examples |
|:---:|:---:|:---:| --- | --- |
| Rule 1 | SELECT | SELECT | ```SELECT ?cwork``` | ```SELECT s.uri as cwork INTO T``` |
| Rule 2 | WHERE | FROM & WHERE | ```WHERE {?cwork cwork:dateModified ?dateModified. }``` | ```FROM ClassInstance:s - (hasDatatypePropertyInstance>:e) - DatatypePropertyInstance:t - (hasValueInstance>:e1) - ValueInstance:t1 WHERE t.uri == "cwork:dateModified"``` |
| Rule 3 | FILTER | WHERE | ```FILTER (?dateModified >= "2011-09-01T00:00:00.000"^^xsd:dateTime)``` | ```WHERE to_datetime(t1.value) >= to_datetime("2011-09-01T00:00:00.000")``` |
| Rule 4 | OPTIONAL | ACCUM CASE | ```OPTIONAL {?cwork cwork:title ?title .}``` | ```ACCUM CASE t.uri WHEN "cwork:title" THEN s.@title += t1.value``` |
| Rule 5 | GROUP BY| GROUP BY | ```GROUP BY ?cwork``` | ```GROUP BY s.uri``` |
| Rule 6 | ORDER BY | ORDER BY| ```ORDER BY ASC(?dataModified) ASC(?cwork)``` | ```ORDER BY s.@dateModified ASC, cwork ASC``` |
| Rule 7 | LIMIT | LIMIT | ```LIMIT 5``` | ```LIMIT 5``` |

Example query description:
```
Retrieve properties dateModified, title, category, liveCoverage, audience for all 
creative works that are of a given type. The value of property dateModified of the 
retrieved creative works should be within a certain time range. Return 5 results 
ordered in ascending order by their dateModified.
```

![SPARQL](./screenshots/sparql.png)

*Figure 3. SPARQL query*

![SPARQL results](./screenshots/sparql_result.png)

*Figure 4. Results returned by running SPARQL query*

![GSQL](./screenshots/gsql.png)

*Figure 5. GSQL query*

![GSQL results](./screenshots/gsql_result.png)

*Figure 6. Results returned by running GSQL query*

## Query Performance Evaluation
* Platform: 
| Graph DBMS | TigerGraph & RDF triple store |
| OS | MacOS 12.4 Montery |
| CPU | 2.3GHz 8-core Intel i9 |
| Memory | 30G 2667 MHz DDR4 |

* Queries:
	11 basic and 25 advanced SPARQL and GSQL queries

* Matrix: 
	Average of Execution Time (10 times)

* Result: Average of Execution Time in TG/Average of Execution Time in RDF triple store (no engine level optimization)
	44.4% Faster in Tigergraph (1.5 - 83.9 SpeedUp) 
	27.8% Faster in RDF triplestore (1.2 - 8.7 SpeedUp)
	27.8% very close (within 0.03s)

## Contributers
```
Lu Zhou, Jay Yu 
Innovation and Development Center
TigerGraph, Inc.
```
