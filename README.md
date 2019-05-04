# SubGraphMatchingHelper 
A Subgraph Matching Assistant that helps identify issues in implementations of Subgraph Matching.

## 7 hookup methods designed and implemented in our assistant
1) Check Candidates
2) Check Query Processing Order
3) Check Next Vertex
4) Check Partial Ordering
5) Check Updated State
6) Check Restored State
7) Check Full Embedding

These hookup methods are based on the general subgraph matching procedure as mentioned below:
 ```
SubgraphMatchingProcedure(Query Graph, Data Graph){
  M = []
  C = CandidateFiltering()
  O = QueryOrderProcessing()
  SubgraphSearchProcedure()
}

SubgraphSearchProcedure(Query Graph, Data Graph, Embedding){
  if jMj = jVQj {
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
  if jMj = jVQj {
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

