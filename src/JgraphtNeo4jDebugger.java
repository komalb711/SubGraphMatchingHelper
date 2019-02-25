import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;

import java.util.*;

public class JgraphtNeo4jDebugger implements HookupInterface {
    private static String PROFILE = "profile";
    private static String EDGES_COUNT = "edge_count";
    private static String NODE_ID = "id";

    private String dataGraphName = "backbones_1RH4.grf";
    private Graph<LabeledVertex, DefaultEdge> queryGraph;
    private GraphDatabaseService dbService;
    private WriteReport report;

    public void setDataAndQueryGraphName(Graph<LabeledVertex, DefaultEdge> queryGraph, String dataGraphName, String queryGraphName, GraphDatabaseService service) {
        this.queryGraph = queryGraph;
        this.dataGraphName = dataGraphName;
        this.dbService = service;
        this.report = new WriteReport(dataGraphName, queryGraphName);
    }

    @Override
    public void checkCandidates(Map<Integer, List<Integer>> candidates, CandidateType checkType) {
        this.report.writeCandidates(candidates, checkType);
        for (int key : candidates.keySet()) {
            int queryId = key;
            List<Integer> inputCandidateList = new ArrayList<>(candidates.get(queryId));
            List<Integer> ourCandidateList = new ArrayList<>();

            LabeledVertex queryVertex = queryGraph.vertexSet().stream().filter(v -> v.getNodeId() == queryId).findAny().get();
            List<LabeledVertex> neighbors = Graphs.neighborListOf(queryGraph, queryVertex);
            int minEdgeCount = Graphs.neighborListOf(queryGraph, queryVertex).size();

            ArrayList<String> queryProfile = new ArrayList<>();
            queryProfile.add(queryVertex.getLabel());
            for (LabeledVertex vertex : neighbors) {
                queryProfile.add(vertex.getLabel());
            }
            Collections.sort(queryProfile);

            ResourceIterator<Node> res = dbService.findNodes(Label.label(queryVertex.getLabel()));
            while (res.hasNext()) {
                Node node = res.next();
                int nodeId = (Integer) node.getProperty(NODE_ID);
                int edgeCount = (Integer) node.getProperty(EDGES_COUNT);
                String[] profile = (String[]) node.getProperty(PROFILE);

                switch (checkType) {
                    case Profiles:
                        if (!profileMatch(queryProfile, profile)) {
                            break;
                        }
                    case EdgeCount:
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
                System.out.println("Node:" + queryId + " : unexpected candidates:" + inputCandidateList);
                this.report.candidateIssues(queryId, inputCandidateList);
            }
        }
    }

    @Override
    public boolean checkSearchOrder(List<Integer> order, boolean showWarning) {
        this.report.writeOrder(order);
        boolean checkPassed = true;
        Set<Integer> set = new HashSet<>(order);
        Set<LabeledVertex> queryGraphVertices = queryGraph.vertexSet();
        if (set.size() < queryGraphVertices.size()) {
            checkPassed = false;
        }
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
        return checkPassed;
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

    public void finishReportWriting() {
        this.report.closeReport();
        ;
    }
}


