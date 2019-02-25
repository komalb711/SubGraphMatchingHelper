import javafx.util.Pair;
import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * CSCI 729 - Topics in Data Management - Graph Databases
 * Assignment 8
 * Problem 1
 *
 * @Author - Komal Bhavsar (kvb9573@rit.edu)
 */


public class VF2 {

    private static String PROJECT_NAME = "PROTEIN";
    private GraphDatabaseService db;

    private List<Pair<Integer, Integer>> relationshipMap;
    private Map<String, String> variableLabelMap;
    private Map<Integer, List<Integer>> candidateMap;
    private Graph<LabeledVertex, DefaultEdge> queryGraph;
    private Map<Integer, List<String>> queryProfiles;

    private ArrayList<Pair<Integer, Integer>> markedMapping;

    private static List<Integer> nodeOrder;

    private String dataGraphLabel = "backbones_1RH4";

    private float gamma;

    private static List<Integer> allDataNodes;
    private static HashMap<Integer, LabeledVertex> allQueryNodes;

    private static List<String> validEmbeddings;

    private JgraphtNeo4jDebugger neo4jDebugger;

    public static void main(String[] args) {
        VF2 obj = new VF2();
        String queryFilePath = "Proteins/query";
        String dataFilePath = "Proteins/target";
        String groundTruthFilename = "Proteins/ground_truth/Proteins.8.gtr";

        obj.neo4jDebugger = new JgraphtNeo4jDebugger();

        obj.query(queryFilePath, dataFilePath, groundTruthFilename);
    }

    private void query(String queryFilePath, String dataFilePath, String groundTruthFilename) {
        File dataFolder = new File(dataFilePath);
        String[] dataFiles = dataFolder.list();

        File queryFolder = new File(queryFilePath);
        String[] queryFiles = queryFolder.list();

        if (dataFiles == null || queryFiles == null) {
            return;
        }

        db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(new File(PROJECT_NAME))
                .setConfig(GraphDatabaseSettings.pagecache_memory, "1024M")
                .setConfig(GraphDatabaseSettings.string_block_size, "60")
                .setConfig(GraphDatabaseSettings.array_block_size, "300")
                .newGraphDatabase();

        validEmbeddings = new ArrayList<>();

        try (Transaction tx = db.beginTx()) {
            for (String dataFile : dataFiles) {
                dataFile = "backbones_1RH4.grf";
                System.out.println("DATA FILE:" + dataFile);
                dataGraphLabel = dataFile.split(".grf")[0];
                for (String queryFile : queryFiles) {
//                queryFile = "backbones_1EMA.8.sub.grf";
                    if (queryFile.endsWith(".8.sub.grf")) {
                        System.out.println("QUERY FILE:" + queryFile);
                        createQueryJGraph(queryFilePath, queryFile);
                        neo4jDebugger.setDataAndQueryGraphName(queryGraph, dataGraphLabel, queryFile, db);
                        findProfileForQueryGraph();
                        validEmbeddings.clear();
                        long startTime = System.currentTimeMillis();

                        if (!computeCandidates(dataGraphLabel))
                            continue;
                        neo4jDebugger.checkCandidates(candidateMap, CandidateType.Profiles);
                        searchSpaceReduction();
                        computeOrder();
                        nodeOrder.clear();
                        nodeOrder.addAll(candidateMap.keySet());
                        nodeOrder.remove(0);

                        neo4jDebugger.checkSearchOrder(nodeOrder, true);
                        startMatch();
                        checkGroundTruth(validEmbeddings, queryFile, dataFile, groundTruthFilename);
                        long endTime = System.currentTimeMillis();
                        System.out.println("Execution Time:" + (endTime - startTime) + "milliseconds\n");
                        neo4jDebugger.finishReportWriting();
                    }
                }
                break;
            }
            tx.success();
        }
        db.shutdown();
    }

    private void startMatch() {
        int uNode = nodeOrder.get(0);
        for (int vNode : candidateMap.get(uNode)) {
            StateWithCustomVertex state = new StateWithCustomVertex();
            state.addMapping(uNode, vNode);
            match(state);
        }
    }

    private void match(StateWithCustomVertex currentState) {
        if (currentState.getM().size() == variableLabelMap.size()) {
            validEmbeddings.add(currentState.getM().toString());
        } else {
            computeTnN(currentState);
            int nodeCnt = currentState.getM().size();
            int queryNodeId = nodeOrder.get(nodeCnt);

            List<Integer> candidates = Utils.intersection(candidateMap.get(queryNodeId), currentState.getT1());
            List<LabeledVertex> queryNeighbor = Graphs.neighborListOf(queryGraph, allQueryNodes.get(queryNodeId));

            for (int dataNodeId : candidates) {
                if (currentState.getM().containsValue(dataNodeId))
                    continue;
                ArrayList<Integer> dataNodeNeighbours = getNeighboursOfNode(dataNodeId);
                if (rule1(currentState, queryNeighbor, dataNodeNeighbours) && rule2(currentState, queryNeighbor, dataNodeNeighbours) && rule3(currentState, queryNeighbor, dataNodeNeighbours)) {
                    StateWithCustomVertex copy = currentState.createDeepCopy();
                    copy.addMapping(queryNodeId, dataNodeId);
                    match(copy);
                }
            }
        }
    }

    private boolean rule1(StateWithCustomVertex currentState, List<LabeledVertex> queryNeighbor, List<Integer> dataNeighbor) {
        List<LabeledVertex> queryNeighboursInMapping = Utils.intersection(queryNeighbor, getVertexListFromNodeList(new ArrayList<>(currentState.getM().keySet())));
        for (LabeledVertex node : queryNeighboursInMapping) {
            if (currentState.getM().containsKey(node.getNodeId())) {
                int value = currentState.getM().get(node.getNodeId());
                if (!dataNeighbor.contains(value)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean rule2(StateWithCustomVertex state, List<LabeledVertex> queryNeighbor, List<Integer> dataNeighbor) {
        List<Integer> data = Utils.intersection(dataNeighbor, state.getT1());
        List<LabeledVertex> query = Utils.intersection(queryNeighbor, state.getT2());
        if (data.size() >= query.size()) {
            return true;
        }
        return false;
    }

    private boolean rule3(StateWithCustomVertex state, List<LabeledVertex> queryNeighbor, List<Integer> dataNeighbor) {
        List<Integer> data = Utils.intersection(dataNeighbor, state.getN1());
        List<LabeledVertex> query = Utils.intersection(queryNeighbor, state.getN2());
        if (data.size() >= query.size()) {
            return true;
        }
        return false;
    }

    private void computeTnN(StateWithCustomVertex state) {
        ArrayList<Integer> neighbours = getNeighbours(new ArrayList<>(state.getM().values()));
        neighbours.removeAll(state.getM().values());
        state.setT1(neighbours);

        List<Integer> temp = allDataNodes;
        temp.removeAll(state.getT1());
        temp.removeAll(state.getM().values());
        state.setN1(temp);

        List<LabeledVertex> queryNodeNeighbours = new ArrayList<>();
        for (int u : state.getM().keySet()) {
            queryNodeNeighbours.addAll(Graphs.neighborListOf(queryGraph, allQueryNodes.get(u)));
        }
        queryNodeNeighbours.removeAll(getVertexListFromNodeList(new ArrayList<>(state.getM().keySet())));
        state.setT2(queryNodeNeighbours);

        List<LabeledVertex> temp1 = state.getN2();
        temp1.removeAll(state.getT2());
        temp1.removeAll(getVertexListFromNodeList(new ArrayList<>(state.getM().keySet())));
        state.setN2(temp1);
    }

    private ArrayList<Integer> getNeighbours(List<Integer> nodes) {
        ArrayList<Integer> results = new ArrayList<>();
        for (int node : nodes) {
            results.addAll(getNeighboursOfNode(node));
        }
        return results;
    }

    private ArrayList<Integer> getNeighboursOfNode(int node) {
        ArrayList<Integer> results = new ArrayList<>();
        Node n = db.findNode(Label.label(dataGraphLabel), "id", node);
        for (Relationship rel : n.getRelationships()) {
            int sId = (Integer) rel.getStartNode().getProperty("id");
            int eId = (Integer) rel.getEndNode().getProperty("id");
            if (sId != node)
                results.add(sId);
            else
                results.add(eId);
        }
        return results;
    }

    private List<Integer> getNodeListFromVertexList(List<LabeledVertex> vertexList) {
        List<Integer> nodeList = new ArrayList<>();
        for (LabeledVertex vertex : vertexList) {
            nodeList.add(vertex.getNodeId());
        }
        return nodeList;
    }

    private List<LabeledVertex> getVertexListFromNodeList(List<Integer> nodeList) {
        List<LabeledVertex> vertexList = new ArrayList<>();
        for (int nodeId : nodeList) {
            vertexList.add(allQueryNodes.get(nodeId));
        }
        return vertexList;
    }

    private void printCandidates() {
        for (int key : candidateMap.keySet()) {
            System.out.println(key + ":" + candidateMap.get(key));
        }
    }

    private void searchSpaceReduction() {
        List<LabeledVertex> vertexSetU = new ArrayList<>();
        List<Integer> vertexSetV = new ArrayList<>();

        HashMap<Integer, List<Integer>> bipartite_graph_edges = new HashMap<>();
        markedMapping = new ArrayList<>();
        boolean check;

        for (int u : candidateMap.keySet()) {
            for (int v : candidateMap.get(u)) {
                markedMapping.add(new Pair<>(u, v));
            }
        }

        while (!markedMapping.isEmpty()) {
            Pair<Integer, Integer> pair = markedMapping.remove(0);
            int u = pair.getKey();
            int v = pair.getValue();
            bipartite_graph_edges.clear();
            vertexSetU.clear();
            vertexSetV.clear();
            check = false;
            List<LabeledVertex> queryNeighbors = Graphs.neighborListOf(queryGraph, allQueryNodes.get(u));
            List<Integer> dataNeighbors = getNeighboursOfNode(v);
            vertexSetU.addAll(queryNeighbors);
            vertexSetV.addAll(dataNeighbors);

            if (vertexSetU.size() > vertexSetV.size()) {
                (candidateMap.get(u)).remove((Object) v);
                continue;
            }

            for (LabeledVertex u_dash : queryNeighbors) {
                List<Integer> validEndpoints = Utils.intersection(dataNeighbors, candidateMap.get(u_dash.getNodeId()));
                if (validEndpoints.size() == 0) {
                    check = true;
                    break;
                }
                for (int endpoint : validEndpoints) {
                    if (!bipartite_graph_edges.containsKey(u_dash.getNodeId())) {
                        bipartite_graph_edges.put(u_dash.getNodeId(), new ArrayList<>());
                    }
                    bipartite_graph_edges.get(u_dash.getNodeId()).add(endpoint);
                }
            }
            if (check) {
                (candidateMap.get(u)).remove((Object) v);
                continue;
            }
            if (!checkSemiPerfectMatching(bipartite_graph_edges)) {
                (candidateMap.get(u)).remove((Object) v);
                markNodes(u, v);
            }
        }
    }


    private boolean checkSemiPerfectMatching(HashMap<Integer, List<Integer>> bipartite_graph_edges) {
        HashMap<Integer, Integer> mapping = new HashMap<>();

        for (int u : bipartite_graph_edges.keySet()) {
            List<Integer> neighbours = bipartite_graph_edges.get(u);
            for (int v : neighbours) {
                if (!mapping.containsKey(u) && !mapping.containsValue(v)) {
                    mapping.put(u, v);
                    if (findSemiPerfectMatching(bipartite_graph_edges, mapping)) {
                        return true;
                    }
                }
            }
            if (mapping.size() == bipartite_graph_edges.keySet().size()) {
                return true;
            }

        }
        return false;
    }

    private boolean findSemiPerfectMatching(HashMap<Integer, List<Integer>> bipartite_graph_edges, HashMap<Integer, Integer> mapping) {
        if (mapping.size() == bipartite_graph_edges.keySet().size()) {
            return true;
        }
        for (int u : bipartite_graph_edges.keySet()) {
            if (!mapping.containsKey(u)) {
                List<Integer> neighbors = bipartite_graph_edges.get(u);
                for (int v : neighbors) {
                    if (!mapping.containsValue(v)) {
                        mapping.put(u, v);
                        findSemiPerfectMatching(bipartite_graph_edges, mapping);
                        if (mapping.size() == bipartite_graph_edges.keySet().size()) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    private void markNodes(int u, int v) {
        List<LabeledVertex> queryNeighbors = Graphs.neighborListOf(queryGraph, allQueryNodes.get(u));
        List<Integer> dataNeighbors = getNeighboursOfNode(v);
        for (LabeledVertex u_node : queryNeighbors) {
            List<Integer> validEndpoints = Utils.intersection(dataNeighbors, candidateMap.get(u_node.getNodeId()));
            for (int endpoint : validEndpoints) {
                markedMapping.add(new Pair<>(u_node.getNodeId(), endpoint));
            }
        }
    }


    private void computeOrder() {
        nodeOrder = new ArrayList<>();

        int minValue = 100000;
        int minIdx = -1;
        for (int key : candidateMap.keySet()) {
            if (candidateMap.get(key).size() < minValue) {
                minValue = candidateMap.get(key).size();
                minIdx = key;
            }
        }

        int startNode = minIdx;
        nodeOrder.add(startNode);

        double previousCost = 1;

        while (nodeOrder.size() < variableLabelMap.size()) {
            ArrayList<LabeledVertex> neighborList = new ArrayList<>();
            for (int u : nodeOrder) {
                neighborList.addAll(Graphs.neighborListOf(queryGraph, allQueryNodes.get(u)));
            }
            neighborList.removeAll(getVertexListFromNodeList(nodeOrder));
            double minCost = 1000000;
            int minCostNode = -1;
            for (LabeledVertex neighbor : neighborList) {
                List<LabeledVertex> temp = Graphs.neighborListOf(queryGraph, neighbor);
                int gammaPower = 0;
                for (LabeledVertex t : temp) {
                    if (nodeOrder.contains(t.getNodeId())) {
                        gammaPower++;
                    }
                }
                double currentCost = previousCost * candidateMap.get(neighbor.getNodeId()).size() * Math.pow(gamma, gammaPower);
                if (currentCost < minCost) {
                    minCostNode = neighbor.getNodeId();
                    minCost = currentCost;
                }
            }
            nodeOrder.add(minCostNode);
            previousCost = minCost;
        }
    }

    private boolean computeCandidates(String label) {
        candidateMap = new HashMap<>();
        allDataNodes = new ArrayList<>();
        gamma = 0;
        int nodeCount = 0;
        int edgeCount = 0;

        for (String key : variableLabelMap.keySet()) {
            String lbl = variableLabelMap.get(key);
            ArrayList<Integer> candidates = new ArrayList<>();
            ResourceIterator<Node> res = db.findNodes(Label.label(label));
            while (res.hasNext()) {
                Node node = res.next();
                allDataNodes.add((Integer) node.getProperty("id"));
                edgeCount += (Integer) node.getProperty("edge_count");
                nodeCount++;
                if (node.hasLabel(Label.label(lbl)) && matchProfiles(node, key)) {
                    candidates.add((Integer) node.getProperty("id"));
                }
            }
            candidateMap.put(Integer.parseInt(key), candidates);
        }


        edgeCount = edgeCount / 2;
        gamma = ((float) edgeCount * 2) / ((float) nodeCount * (nodeCount - 1));
        for (int key : candidateMap.keySet()) {
            if (candidateMap.get(key).size() == 0) {
                return false;
            }
        }
        return true;
    }


    private boolean matchProfiles(Node dataNode, String queryNode) {
        int nodeId = Integer.parseInt(queryNode);
        String[] profile = (String[]) dataNode.getProperty("profile");
        int v = 0, u = 0;
        while (v < profile.length && u < queryProfiles.get(nodeId).size()) {
            if (profile[v].equals(queryProfiles.get(nodeId).get(u))) {
                v++;
                u++;
            } else if (profile[v].compareTo(queryProfiles.get(nodeId).get(u)) < 0) {
                v++;
            } else {
                return false;
            }
        }
        return true;
    }

    private void findProfileForQueryGraph() {
        queryProfiles = new HashMap<>();
        for (LabeledVertex node : queryGraph.vertexSet()) {
            ArrayList<String> profiles = new ArrayList<>();
            profiles.add(node.getLabel());
            for (LabeledVertex neighbor : Graphs.neighborListOf(queryGraph, node)) {
                profiles.add(neighbor.getLabel());
            }
            Collections.sort(profiles);
            queryProfiles.put(node.getNodeId(), profiles);
        }
    }


    private void createQueryJGraph(String filePath, String filename) {
        List<String> content = readFile(filePath + "/" + filename);
        parseFileContent(content);
        queryGraph = new SimpleGraph<>(DefaultEdge.class);

        allQueryNodes = new HashMap<>();

        for (String key : variableLabelMap.keySet()) {
            LabeledVertex vertex = new SingleLabeledVertex(Integer.parseInt(key), variableLabelMap.get(key));
            queryGraph.addVertex(vertex);
            allQueryNodes.put(Integer.parseInt(key), vertex);
        }
        for (Pair<Integer, Integer> pair : relationshipMap) {
            Integer end1 = pair.getKey();
            Integer end2 = pair.getValue();
            queryGraph.addEdge(allQueryNodes.get(end1), allQueryNodes.get(end2));
        }
    }


    private void parseFileContent(List<String> content) {
        relationshipMap = new ArrayList<>();
        variableLabelMap = new HashMap<>();
        content.remove(0);
        for (String line : content) {
            String[] tokens = line.split(" ");
            if (tokens.length > 1) {
                if (tokens[1].matches("[0-9]+")) {
                    if (!(relationshipMap.contains(new Pair<>(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[0]))) || relationshipMap.contains(new Pair<>(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]))))) {
                        relationshipMap.add(new Pair<>(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1])));
                    }
                } else {
                    variableLabelMap.put(tokens[0], tokens[1]);
                }
            }
        }
    }

    private ArrayList<String> readFile(String filename) {
        try {
            ArrayList<String> list = new ArrayList<>();
            BufferedReader reader = new BufferedReader(new FileReader(filename));
            String line;
            while ((line = reader.readLine()) != null) {
                list.add(line);
            }
            reader.close();
            return list;
        } catch (Exception e) {
            System.err.format("Exception occurred trying to read '%s'.", filename);
            e.printStackTrace();
            return null;
        }
    }

    private void checkGroundTruth(List<String> results, String queryFilename, String dataFilename, String groundTruthFilename) {
        try {
            BufferedReader reader;
            reader = new BufferedReader(new FileReader(groundTruthFilename));

            String line;
            while ((line = reader.readLine()) != null) {
                boolean solChecked = false;
                if (line.contains("T:" + dataFilename)) {
                    if (reader.readLine().contains("P:" + queryFilename)) {
                        int matched = 0;
                        String solCountStr = reader.readLine();
                        String cnt = solCountStr.substring(2, solCountStr.length());
                        int solutionCount = Integer.parseInt(cnt);
                        for (int solIdx = 0; solIdx < solutionCount; solIdx++) {
                            String solution = reader.readLine();
                            for (String result : results) {
                                if (solution.contains(reformResultString(result))) {
                                    matched++;
                                }
                            }
                        }
                        System.out.println("True Positive:" + matched);
                        System.out.println("False Positive:" + (results.size() - matched));
                        System.out.println("False Negative:" + (solutionCount - matched));
                        solChecked = true;
                    }
                }
                if (solChecked) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String reformResultString(String result) {
        result = result.replaceAll(",", ";");
        result = result.replaceAll("=", ",");
        result = result.replaceAll(" ", "");
        result = result.replaceAll("}", "");
        result = result.replaceAll("\\{", "");
        return result;

    }


}
