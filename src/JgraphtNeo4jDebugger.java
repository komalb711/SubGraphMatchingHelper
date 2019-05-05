import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.neo4j.graphdb.*;

import java.util.*;

/**
 * @Author: Komal Bhavsar (kvb9573@rit.edu)
 * Rochester Institute of Technology
 * CS MS Capstone Project - Spring 2019
 *
 * JgraphtNeo4jDebugger.java
 *
 */

public class JgraphtNeo4jDebugger implements HookupInterface {
    private static String PROFILE = "profile";
    private static String EDGES_COUNT = "edge_count";
    private static String NODE_ID = "id";

    private String dataGraphName = "backbones_1RH4.grf";
    private Graph<LabeledVertex, DefaultEdge> queryGraph;
    private GraphDatabaseService dbService;
    private WriteReport report;
    private Set<Map<Integer, Integer>> groundTruth;
    private SubgraphMatchingType type;

    private Map<Integer, Integer> currentEmbedding;

    public void setDataAndQueryGraphName(Graph<LabeledVertex, DefaultEdge> queryGraph, String dataGraphName, String queryGraphName, GraphDatabaseService service) {
        this.queryGraph = queryGraph;
        this.dataGraphName = dataGraphName;
        this.dbService = service;
        this.report = new WriteReport(dataGraphName, queryGraphName);
        this.currentEmbedding = new HashMap<>();
    }

    public void setGroundTruth(Set<Map<Integer, Integer>> groundTruth, SubgraphMatchingType type) {
        this.groundTruth = new HashSet<>(groundTruth);
        this.type = type;
    }

    @Override
    public void checkCandidates(Map<Integer, Set<Integer>> candidates, CandidateType checkType) {
        this.report.writeCandidates(candidates, checkType);
        for (int key : candidates.keySet()) {
            int queryId = key;
            Set<Integer> inputCandidateList = new HashSet<>(candidates.get(queryId));
            Set<Integer> ourCandidateList = new HashSet<>();

            LabeledVertex queryVertex = queryGraph.vertexSet().stream().filter(v -> v.getNodeId() == queryId).findAny().get();
            List<LabeledVertex> neighbors = Graphs.neighborListOf(queryGraph, queryVertex);
            int minEdgeCount = Graphs.neighborListOf(queryGraph, queryVertex).size();

            ArrayList<String> queryProfile = new ArrayList<>();
            queryProfile.add(queryVertex.getLabel());
            for (LabeledVertex vertex : neighbors) {
                queryProfile.add(vertex.getLabel());
            }
            Collections.sort(queryProfile);

            ResourceIterator<Node> res = dbService.findNodes(Label.label(dataGraphName));//Label.label(queryVertex.getLabel()));
            while (res.hasNext()) {
                Node node = res.next();
                int nodeId = (Integer) node.getProperty(NODE_ID);


                if (!node.hasLabel(Label.label(queryVertex.getLabel()))) {
                    continue;
                }
                switch (checkType) {
                    case Profiles:
                        String[] profile = (String[]) node.getProperty(PROFILE);
                        if (!profileMatch(queryProfile, profile)) {
                            break;
                        }
                    case EdgeCount:
                        int edgeCount = (Integer) node.getProperty(EDGES_COUNT);
                        if (edgeCount < minEdgeCount) {
                            break;
                        }
                    case Basic:
                        ourCandidateList.add(nodeId);
                        break;
                }
            }

            inputCandidateList.removeAll(ourCandidateList);
            if (inputCandidateList.size() != 0) {
                this.report.candidateIssues(queryId, inputCandidateList);
            }
        }
        if (this.groundTruth != null && !groundTruth.isEmpty()) {
            this.candidatesWithGroundTruth(candidates);
        }
    }

    public void candidatesWithGroundTruth(Map<Integer, Set<Integer>> candidates) {

        Map<Integer, Set<Integer>> missingCandidates = new HashMap<>();

        for (Map<Integer, Integer> embedding : groundTruth) {
            for (int candidateKey : embedding.keySet()) {
                if (!candidates.get(candidateKey).contains(embedding.get(candidateKey))) {
                    if (!missingCandidates.containsKey(candidateKey)) {
                        missingCandidates.put(candidateKey, new HashSet<>());
                    }
                    missingCandidates.get(candidateKey).add(embedding.get(candidateKey));
                }
            }
        }

        if (!missingCandidates.isEmpty()) {
            this.report.writeMissingCandidates(missingCandidates);
        }
    }

    @Override
    public void checkQueryOrder(List<Integer> order, boolean showWarning) {
        this.report.writeOrder(order);
        List<Integer> set = new ArrayList<>(order);
        Set<LabeledVertex> queryGraphVertices = queryGraph.vertexSet();
        for (LabeledVertex vertex : queryGraphVertices) {
            if (!set.contains(vertex.getNodeId())) {
                this.report.queryOrderIssues(vertex.getNodeId());
                break;
            }
        }
        if (showWarning) {
            checkCartesianProduct(order);
            checkEfficientOrder(order);
        }
    }


    public void checkCartesianProduct(List<Integer> order) {
        List<Integer> qOrder = new ArrayList<>(order);
        int nId = qOrder.remove(0);
        LabeledVertex queryVertex = queryGraph.vertexSet().stream().filter(v -> v.getNodeId() == nId).findAny().get();
        List<LabeledVertex> neighbors = Graphs.neighborListOf(queryGraph, queryVertex);
        while (!qOrder.isEmpty()) {
            int id = qOrder.remove(0);
            queryVertex = queryGraph.vertexSet().stream().filter(v -> v.getNodeId() == id).findAny().get();
            if (neighbors.contains(queryVertex)) {
                neighbors.addAll(Graphs.neighborListOf(queryGraph, queryVertex));
            } else {
                report.queryOrderWarning("Not optimal Order: Cartesian Product : Q_" + id);
            }
        }
    }


    public void checkEfficientOrder(List<Integer> order) {
        Map<Integer, Integer> nodeNeighborCount = new HashMap<>();
        Set<LabeledVertex> vertexSet = queryGraph.vertexSet();
        for (LabeledVertex vertex : vertexSet) {
            int edgeCount = Graphs.neighborListOf(queryGraph, vertex).size();
            nodeNeighborCount.put(vertex.getNodeId(), edgeCount);
        }

        for (int idx = 1; idx < order.size(); idx++) {
            if (nodeNeighborCount.get(idx - 1) < nodeNeighborCount.get(idx)) {
                report.queryOrderWarning("Not optimal Order: Edge Coverage : Q_" + idx);
            }
        }
    }


    @Override
    public void checkNextVertex(List<Integer> order, int u) {
        int index = this.currentEmbedding.size();
        if (u != order.get(index)) {
            report.writeNextVertexError(order.get(index), u);
        }
    }

    @Override
    public void checkPartialEmbedding(Map<Integer, Integer> mapping, int u, int v, EmbeddingCheckType checkType, boolean isJoinable) {
        switch (checkType) {
            case WithGraph:
                boolean induced = true, nonInduced = true;
                switch (this.type) {
                    case Induced:
                        induced = inducedGraphMatch(mapping, u, v);
                    case NonInduced:
                        nonInduced = nonInducedGraphMatch(mapping, u, v);
                }
                if ((induced && nonInduced) != isJoinable) {
                    report.writePartialEmbeddingError(mapping, u, v, isJoinable, (induced && nonInduced), this.type);
                }
                break;

            case WithGroundTruth:
                partialEmbeddingMatchWithGroundTruth(mapping, u, v, isJoinable);
                break;
        }
    }

    private boolean inducedGraphMatch(Map<Integer, Integer> mapping, int u, int v) {

        mapping.put(u, v);
        boolean shouldJoin = true;
        for (Integer queryNodeId : mapping.keySet()) {
            LabeledVertex queryVertex = queryGraph.vertexSet().stream().filter(q -> q.getNodeId() == queryNodeId).findAny().get();
            List<LabeledVertex> neighbors = new ArrayList<>(queryGraph.vertexSet());
            neighbors.removeAll(Graphs.neighborListOf(queryGraph, queryVertex));
            neighbors.remove(queryVertex);
            for (LabeledVertex neighbor : neighbors) {
                if (mapping.containsKey(neighbor.getNodeId())) {
                    String query = "MATCH(N1:" + dataGraphName + "{id:" + mapping.get(queryNodeId) + "})-" +
                            "[r:link] -(N2:" + dataGraphName + "{id:" + mapping.get(neighbor.getNodeId()) + "}) " +
                            " WHERE N1.id <> N2.id RETURN SIGN(COUNT(r)) as RES";
                    Result res = dbService.execute(query);
                    long result = (Long) res.next().get("RES");

                    if (result == 1) {
                        shouldJoin = false;
                        break;
                    }
                }
            }
            if (!shouldJoin) {
                break;
            }
        }
        mapping.remove(u);
        if (!shouldJoin) {
            return false;
        }

        return true;
    }

    private boolean nonInducedGraphMatch(Map<Integer, Integer> mapping, int u, int v) {

        mapping.put(u, v);
        boolean shouldJoin = true;
        for (Integer queryNodeId : mapping.keySet()) {
            LabeledVertex queryVertex = queryGraph.vertexSet().stream().filter(q -> q.getNodeId() == queryNodeId).findAny().get();
            List<LabeledVertex> neighbors = new ArrayList<>(Graphs.neighborListOf(queryGraph, queryVertex));
            for (LabeledVertex neighbor : neighbors) {
                if (mapping.containsKey(neighbor.getNodeId())) {
                    String query = "MATCH(N1:" + dataGraphName + "{id:" + mapping.get(queryNodeId) + "})-" +
                            "[r:link] -(N2:" + dataGraphName + "{id:" + mapping.get(neighbor.getNodeId()) + "}) " +
                            " WHERE N1.id <> N2.id RETURN SIGN(COUNT(r)) as RES";

                    Result res = dbService.execute(query);
                    long result = (Long) res.next().get("RES");

                    if (result == 0) {
                        shouldJoin = false;
                        break;
                    }
                }
            }
            if (!shouldJoin) {
                break;
            }
        }
        mapping.remove(u);
        if (!shouldJoin) {
            return false;
        }

        return true;
    }

    private void partialEmbeddingMatchWithGroundTruth(Map<Integer, Integer> mapping, int u, int v, boolean isJoinable) {
        mapping.put(u, v);
        boolean shouldJoin = false;

        for (Map<Integer, Integer> groundTruthMapping : this.groundTruth) {
            boolean isMatch = groundTruthMapping.entrySet().containsAll(mapping.entrySet());
            if (isMatch) {
                shouldJoin = true;
                break;
            }
        }

        if (shouldJoin && !isJoinable) {
            //error
            report.writePartialEmbeddingError(mapping, u, v, isJoinable, shouldJoin, this.type);
        } else if (!shouldJoin && isJoinable) {
            //warning
            report.writePartialEmbeddingWarning(mapping, u, v, isJoinable, shouldJoin, this.type);
        }
        mapping.remove(u);

    }

    @Override
    public void checkFullEmbedding(Map<Integer, Integer> mapping) {
        boolean match = false;

        for (Map<Integer, Integer> gtMapping : groundTruth) {
            boolean equals = gtMapping.equals(mapping);
            if (equals) {
                match = true;
                break;
            }
        }

        if (!match) {
            //error
            report.writeFullEmbeddingError(mapping, this.type);
        }

    }


    @Override
    public void checkUpdatedState(Map<Integer, Integer> mapping, int u, int v) {
        this.currentEmbedding.put(u, v);
        boolean entryMissing = false, mismatchEmbedding = false;

        if (!mapping.containsKey(u) || mapping.get(u) != v) {
            entryMissing = true;
        }
        for (int key : mapping.keySet()) {
            if (!currentEmbedding.containsKey(key) || !Objects.equals(currentEmbedding.get(key), mapping.get(key))) {
                mismatchEmbedding = true;
                break;
            }
        }
        if (entryMissing) {
            report.writeStateError(this.currentEmbedding, mapping, u, v, true);
        }
        if (mismatchEmbedding) {
            report.writeStateError(this.currentEmbedding, mapping, u, v, true);
        }
    }

    @Override
    public void checkRestoredState(Map<Integer, Integer> mapping, int u, int v) {
        this.currentEmbedding.remove(u);
        boolean entryPresent = false, mismatchEmbedding = false;

        if (mapping.containsKey(u)) {
            entryPresent = true;
        }
        for (int key : mapping.keySet()) {
            if (!currentEmbedding.containsKey(key) || !Objects.equals(currentEmbedding.get(key), mapping.get(key))) {
                mismatchEmbedding = true;
                break;
            }
        }
        if (entryPresent) {
            report.writeStateError(this.currentEmbedding, mapping, u, v, false);
        }
        if (mismatchEmbedding) {
            report.writeStateError(this.currentEmbedding, mapping, u, v, false);
        }
    }

    public static boolean profileMatch(List<String> queryProfile, String[] dataProfile) {
        int v = 0, u = 0;
        while (v < dataProfile.length && u < queryProfile.size()) {
            if (dataProfile[v].equals(queryProfile.get(u))) {
                v++;
                u++;
            } else if (dataProfile[v].compareTo(queryProfile.get(u)) < 0) {
                v++;
            } else {
                return false;
            }
        }
        return true;
    }

    public void writeResults(Set<Map<Integer, Integer>> result) {
        this.report.writeFullEmbeddings(result, groundTruth);
    }

    public void finishReportWriting() {
        this.report.closeReport();
    }
}


