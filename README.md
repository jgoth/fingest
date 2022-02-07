# fingest
Financial Ingestion Framework for experimenting with Neo4j graph algorithms. Information persisted online into json using [yfinance](https://pypi.org/project/yfinance/).

## Schema


![Schema](https://github.com/jgoth/fingest/blob/main/schema.jpg?raw=true)

Above is the schema for the graph database

## Installation

(Requires a 4.x neo4j instance and CoreNLP Server)

### Unzip the data file

```
tar xvfz data.zip
```


### Configure the config.json file

Include the CoreNLP port, Neo4j Server user and password

### Run the graph import notebook

Open the graph import jupyter notebook and execute each block

This will load the Neo4j database


## Obtain communities of institutional holders
```
  CALL gds.louvain.stream({
    nodeQuery:'MATCH (i:InstHolder)
      RETURN  id(i) AS id',
    relationshipQuery:'MATCH (i1:InstHolder)<-[r1:HELD_BY]-(t:Ticker)-[r2:HELD_BY]->(i2:InstHolder)
      RETURN id(i1) AS source, id(i2) AS target'})
  YIELD nodeId, communityId
  WITH communityId, COLLECT(gds.util.asNode(nodeId).name) AS group
  WHERE size(group) > 1
  RETURN communityId, group
  ORDER BY size(group) ASC


╒═════════════╤══════════════════════════════════════════════════════════════════════╕
│"communityId"│"group"                                                               │
╞═════════════╪══════════════════════════════════════════════════════════════════════╡
│1810         │["F/M Investments, LLC","Mengis Capital Management, Inc."]            │
├─────────────┼──────────────────────────────────────────────────────────────────────┤
│2030         │["ARK ETF Tr-ARK Space Exploration & Innovation ETF","Fidelity Select │
│             │Portfolios - Defense and Aerospace"]                                  │
├─────────────┼──────────────────────────────────────────────────────────────────────┤
│2161         │["Direxion Shares ETF Tr-Direxion Low Priced Stock ETF","ProShares Tr-│
│             │Ultra Nasdaq Biotechnology Fd","Principal Healthcare Innovators Index │
│             │ETF"]                                                                 │
├─────────────┼──────────────────────────────────────────────────────────────────────┤

```

As a sanity check, this appears to be consistent with our intuition since community #2030 contains a space and defense/aerospace fund.
