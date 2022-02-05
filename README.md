# fingest
Financial Ingestion Framework




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
```
