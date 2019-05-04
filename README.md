# SubGraphMatchingHelper 
A Subgraph Matching Assistant that helps identify issues in implementations of Subgraph Matching.
This assitant works with algorithms like Naive, VF2 and GraphQL algorithms

## 7 hookup methods designed and implemented in the assistant
1) Check Candidates - validates the query node candidates based on the type of check. The differnt type of checks are - Basic(based on labels), Edge Count(based on number of neighbors) and Profile(based on the labels of the current nodes' and neighbors' labels)
2) Check Query Processing Order - ensures that the query processing order has all query nodes. It has an additional parameter for adding to check optimilatity of the order
3) Check Next Vertex - ensures that crrect query node is used when query node is used in the recursive SubgraphSearchProcedure().
4) Check Partial Ordering - checks if the embedding formed by the nodes in he embedding and the current query and data node under consideration form a valid embedding. There are two ways to check the correct partial ordering - one is using the ground truth, other is checking with graph. 
5) Check Updated State - validates that the correct pair of query and data nodes are used in the recursive call
6) Check Restored State - validates that the pair of query and data node added in the recursive call is removed once it completes the recrusive call
7) Check Full Embedding - checks if the full embedding found the implementation maches with the ground truth

These hookup methods are based on the general subgraph matching procedure as mentioned below:
 ```
SubgraphMatchingProcedure(Query Graph, Data Graph){
  M = []
  C = CandidateFiltering()
  O = QueryOrderProcessing()
  SubgraphSearchProcedure()
}

SubgraphSearchProcedure(Query Graph, Data Graph, Embedding){
  if |M| = |VQ| {
    Report M
  }
  else
  {
    u = next node in query order O
    for (v in (candidates of u, not in M)) {
    if(IsJoinable(u,v)){
      UpdateState(add (u,v) to M)
      SubGraphSearch(M, u, v)
      RestoreState(remove (u, v) from M)
    }
  }
}
 ```

The hookup methods are integrated in the above procedure like below:
```
SubgraphMatchingProcedure(Query Graph, Data Graph){
  M = []
  C = CandidateFiltering()
  CheckCandidate(C)
  O = QueryOrderProcessing()
  CheckQueryProcessingOrder())
  SubgraphSearchProcedure()
}

SubgraphSearchProcedure(Query Graph, Data Graph, Embedding){
  if |M| = |VQ| {
    Report M;
    CheckFullEmbedding()
  }
  else
  {
    u = next node in query order O
    CheckNextVertex()
    for (v in (candidates of u, not in M)) {
    if(IsJoinable(u,v)){
      UpdateState(add (u,v) to M)
      CheckUpdatedState()
      SubGraphSearch(M, u, v)
      RestoreState(remove (u, v) from M)
      CheckRestoredState()
    }
  }
}

 ```

