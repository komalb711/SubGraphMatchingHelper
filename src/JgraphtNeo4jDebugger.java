import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.neo4j.graphdb.*;

import java.util.*;

public class JgraphtNeo4jDebugger implements HookupInterface {
    private String dataGraphName = "backbones_1RH4.grf";
    private Graph<CustomVertex, DefaultEdge> queryGraph;
    private HashMap<Integer, CustomVertex> queryVertexMap;

    private GraphDatabaseService db;

    public void setDataGraph(String dataGraph) {
        this.dataGraphName = dataGraph;
    }

    public void setDataAndQueryGraphName(Graph<CustomVertex, DefaultEdge> queryGraph, GraphDatabaseService service) {
        this.queryGraph = queryGraph;
        this.db = service;
        this.queryVertexMap = new HashMap<>();
        Set<CustomVertex> vertices = queryGraph.vertexSet();
        for (CustomVertex vertex : vertices) {
            queryVertexMap.put(vertex.getNodeId(), vertex);
        }

    }

    @Override
    public boolean checkCandidateList(List<Integer> candidateList, int queryNodeId, CandidateType checkType) {
        switch (checkType) {
            case Basic: {
                List<Integer> inputCandidateList = new ArrayList<>(candidateList);
                List<Integer> ourCandidateList = new ArrayList<>();
                try (Transaction tx = db.beginTx()) {
                    ResourceIterator<Node> res = db.findNodes(Label.label(queryVertexMap.get(queryNodeId).getLabel()));
                    while (res.hasNext()) {
                        Node node = res.next();
                        ourCandidateList.add((Integer) node.getProperty("id"));
                    }
                    tx.success();
                }
                inputCandidateList.removeAll(ourCandidateList);
                return inputCandidateList.size() == 0;
            }

            case EdgeCount: {
                List<Integer> inputCandidateList = new ArrayList<>(candidateList);
                List<Integer> ourCandidateList = new ArrayList<>();
                int minEdgeCount = Graphs.neighborListOf(queryGraph, queryVertexMap.get(queryNodeId)).size();
                try (Transaction tx = db.beginTx()) {
                    ResourceIterator<Node> res = db.findNodes(Label.label(queryVertexMap.get(queryNodeId).getLabel()));
                    while (res.hasNext()) {
                        Node node = res.next();
                        int edgeCount = (Integer) node.getProperty("edge_count");
                        if (edgeCount >= minEdgeCount) {
                            ourCandidateList.add((Integer) node.getProperty("id"));
                        }
                    }
                    tx.success();
                }
                inputCandidateList.removeAll(ourCandidateList);
                return inputCandidateList.size() == 0;
            }

            case Profiles: {
                List<Integer> inputCandidateList = new ArrayList<>(candidateList);
                List<Integer> ourCandidateList = new ArrayList<>();
                List<CustomVertex> neighbors = Graphs.neighborListOf(queryGraph, queryVertexMap.get(queryNodeId));
                ArrayList<String> queryProfile = new ArrayList<>();
                queryProfile.add(queryVertexMap.get(queryNodeId).getLabel());
                for (CustomVertex vertex : neighbors) {
                    queryProfile.add(vertex.getLabel());
                }
                Collections.sort(queryProfile);

                try (Transaction tx = db.beginTx()) {
                    ResourceIterator<Node> res = db.findNodes(Label.label(queryVertexMap.get(queryNodeId).getLabel()));
                    while (res.hasNext()) {
                        Node node = res.next();
                        String[] profile = (String[]) node.getProperty("profile");
                        if (profileMatch(queryProfile, profile)) {
                            ourCandidateList.add((Integer) node.getProperty("id"));
                        }
                    }
                    tx.success();
                }
                inputCandidateList.removeAll(ourCandidateList);
                return inputCandidateList.size() == 0;
            }
        }
        return false;

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


