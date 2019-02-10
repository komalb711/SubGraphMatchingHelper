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
    private Graph<SingleLabeledVertex, DefaultEdge> queryGraph;
    private HashMap<Integer, SingleLabeledVertex> queryVertexMap;

    private GraphDatabaseService db;

    public void setDataGraph(String dataGraph) {
        this.dataGraphName = dataGraph;
    }

    public void setDataAndQueryGraphName(Graph<SingleLabeledVertex, DefaultEdge> queryGraph, GraphDatabaseService service) {
        this.queryGraph = queryGraph;
        this.db = service;
        this.queryVertexMap = new HashMap<>();
        Set<SingleLabeledVertex> vertices = queryGraph.vertexSet();
        for (SingleLabeledVertex vertex : vertices) {
            queryVertexMap.put(vertex.getNodeId(), vertex);
        }
    }

    @Override
    public boolean checkCandidateList(List<Integer> candidateList, int queryNodeId, CandidateType checkType) {
        List<Integer> inputCandidateList = new ArrayList<>(candidateList);
        List<Integer> ourCandidateList = new ArrayList<>();

        SingleLabeledVertex queryVertex = queryGraph.vertexSet().stream().filter(v -> v.getNodeId() == queryNodeId).findAny().get();
        List<SingleLabeledVertex> neighbors = Graphs.neighborListOf(queryGraph, queryVertex);
        int minEdgeCount = Graphs.neighborListOf(queryGraph, queryVertexMap.get(queryNodeId)).size();

        ArrayList<String> queryProfile = new ArrayList<>();
        queryProfile.add(queryVertexMap.get(queryNodeId).getLabel());
        for (SingleLabeledVertex vertex : neighbors) {
            queryProfile.add(vertex.getLabel());
        }
        Collections.sort(queryProfile);

        ResourceIterator<Node> res = db.findNodes(Label.label(queryVertex.getLabel()));
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
        boolean checkPass = inputCandidateList.size() == 0;

        return checkPass;
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
}


