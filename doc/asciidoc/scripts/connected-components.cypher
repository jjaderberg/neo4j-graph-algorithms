// tag::create-sample-graph[]

CREATE (nAlice:User {id:'Alice'})
,(nBridget:User {id:'Bridget'})
,(nCharles:User {id:'Charles'})
,(nDoug:User {id:'Doug'})
,(nMark:User {id:'Mark'})
,(nMichael:User {id:'Michael'})
CREATE (nAlice)-[:FRIEND{weight:0.5}]->(nBridget)
,(nAlice)-[:FRIEND{weight:4}]->(nCharles)
,(nMark)-[:FRIEND{weight:1}]->(nDoug)
,(nMark)-[:FRIEND{weight:2}]->(nMichael);

// end::create-sample-graph[]

// tag::unweighted-stream-sample-graph[]

CALL algo.unionFind.stream('User', 'FRIEND', {}) 
YIELD nodeId,setId
RETURN nodeId,setId LIMIT 20;

// end::unweighted-stream-sample-graph[]

// tag::unweighted-write-sample-graph[]

CALL algo.unionFind('User', 'FRIEND', {write:true, partitionProperty:"partition"}) 
YIELD nodes, setCount, loadMillis, computeMillis, writeMillis;

// end::unweighted-write-sample-graph[]

// tag::weighted-stream-sample-graph[]

CALL algo.unionFind.stream('User', 'FRIEND', {weightProperty:'weight', defaultValue:0.0, threshold:1.0}) 
YIELD nodeId,setId;

// end::weighted-stream-sample-graph[]

// tag::weighted-write-sample-graph[]

CALL algo.unionFind('User', 'FRIEND', {write:true, partitionProperty:"partition",weightProperty:'weight', defaultValue:0.0, threshold:1.0}) 
YIELD nodes, setCount, loadMillis, computeMillis, writeMillis;

// end::weighted-write-sample-graph[]

// tag::check-results-sample-graph[]

MATCH (u:User)
RETURN u.partition as partition,count(*) as size_of_partition 
ORDER by size_of_partition DESC LIMIT 20; 

// end::check-results-sample-graph[]

// tag::count-component-yelp[]

CALL algo.unionFind.stream('User', 'FRIEND', {}) 
YIELD nodeId,setId
RETURN count(distinct setId) as count_of_components;

// end::count-component-yelp[]

// tag::top-20-component-yelp[]

CALL algo.unionFind.stream('User', 'FRIEND', {}) 
YIELD nodeId,setId
RETURN setId,count(*) as size_of_component
ORDER BY size_of_component LIMIT 20;

// end::top-20-component-yelp[]

// tag::cypher-loading[]

CALL algo.unionFind(
  'MATCH (p:User) RETURN id(p) as id',
  'MATCH (p1:User)-[f:FRIEND]->(p2:User) 
   RETURN id(p1) as source, id(p2) as target, f.weight as weight',
  {graph:'cypher',write:true}
);

// end::cypher-loading[]
