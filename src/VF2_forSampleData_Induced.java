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


public class VF2_forSampleData_Induced {

    private static String PROJECT_NAME = "SAMPLES";
    private GraphDatabaseService db;

    private List<Pair<Integer, Integer>> relationshipMap;
    private Map<String, String> variableLabelMap;
    private Map<Integer, Set<Integer>> candidateMap;
    private Graph<LabeledVertex, DefaultEdge> queryGraph;
    private Map<Integer, List<String>> queryProfiles;

    private ArrayList<Pair<Integer, Integer>> markedMapping;

    private static List<Integer> nodeOrder;

    private String dataGraphLabel ;//= "backbones_1RH4";

    private float gamma;

    private static Set<Integer> allDataNodes;
    private static HashMap<Integer, LabeledVertex> allQueryNodes;

    private static Set<Map<Integer, Integer>> validEmbeddings;

    private JgraphtNeo4jDebugger neo4jDebugger;

    public static void main(String[] args) {
        VF2_forSampleData_Induced obj = new VF2_forSampleData_Induced();
        String queryFilePath = "Proteins/Samples";
        String dataFilePath = "Proteins/Samples";
        String groundTruthFilename = "Proteins/Samples/GroundTruth-Induced.text";

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

        validEmbeddings = new HashSet<>();
        nodeOrder = new ArrayList<>();

        try (Transaction tx = db.beginTx()) {
            for (String dataFile : dataFiles) {
//                dataFile = "s_3_2.txt";
                if(!dataFile.endsWith(".txt")){continue;}
                System.out.println("DATA FILE:" + dataFile);
                dataGraphLabel = dataFile.split(".txt")[0];
                for (String queryFile : queryFiles) {
//                queryFile = "s_4_2.txt";
                    if (queryFile.endsWith(".txt")) {

                        System.out.println("QUERY FILE:" + queryFile);
                        createQueryJGraph(queryFilePath, queryFile);

                        neo4jDebugger.setDataAndQueryGraphName(queryGraph, dataGraphLabel, queryFile, db);
                        Set<Map<Integer, Integer>> groundTruth = getGroundTruth(queryFile, dataFile,groundTruthFilename);
                        neo4jDebugger.setGroundTruth(groundTruth, SubgraphMatchingType.Induced);
                        findProfileForQueryGraph();
                        validEmbeddings.clear();
                        nodeOrder.clear();
                        long startTime = System.currentTimeMillis();

                        // compute candidates
                        if(!computeCandidates(dataGraphLabel)){continue;}
//                        candidateMap.get(0).add(1);

                        // call assistant to check our candidates
                        neo4jDebugger.checkCandidates(candidateMap, CandidateType.Basic);

                        // compute processing order
                        computeOrder();
//                        nodeOrder.addAll(candidateMap.keySet());
//                        nodeOrder.remove(0);

                        // call assistant to check our query order
                        neo4jDebugger.checkQueryOrder(nodeOrder, false);

                        // start recursive search procedure
                        searchProcHelper();
                        System.out.println("Results:" + queryFile + " " + dataFile);
                        System.out.println(validEmbeddings);
                        // check the embeddings with ground truth
//                        checkGroundTruth(validEmbeddings, queryFile, dataFile, groundTruthFilename);
                        checkResults(validEmbeddings, groundTruth);

                        long endTime = System.currentTimeMillis();
                        System.out.println("Execution Time:" + (endTime - startTime) + "milliseconds\n");
                        neo4jDebugger.finishReportWriting();
//                        break;
                    }

                }
//                break;
            }
            tx.success();
        }
        db.shutdown();
    }

    private void searchProcHelper() {
        int uNode = nodeOrder.get(0);

        StateWithCustomVertex state = new StateWithCustomVertex();
        for (int vNode : candidateMap.get(uNode)) {
            state.addMapping(uNode, vNode);
            neo4jDebugger.checkUpdatedState(state.getM(), uNode, vNode);
            searchProc(state);
            state.removeMapping(uNode);
            neo4jDebugger.checkRestoredState(state.getM(), uNode, vNode);
        }
    }

    private void searchProc(StateWithCustomVertex currentState) {
        if (currentState.getM().size() == nodeOrder.size()) {
            neo4jDebugger.checkFullEmbedding(currentState.getM());
            validEmbeddings.add(currentState.getM());
        } else {
            computeTnN(currentState);
            int nodeCnt = currentState.getM().size();
            int queryNodeId = nodeOrder.get(nodeCnt);
            neo4jDebugger.checkNextVertex(nodeOrder, queryNodeId);

            Set<Integer> candidates = Utils.intersection(candidateMap.get(queryNodeId), currentState.getT1());
            Set<LabeledVertex> queryNeighbor = Graphs.neighborSetOf(queryGraph, allQueryNodes.get(queryNodeId));

            for (int dataNodeId : candidates) {
                if (currentState.getM().containsValue(dataNodeId))
                    continue;
                Set<Integer> dataNodeNeighbours = getNeighboursOfNode(dataNodeId);
                boolean isJoinable = rule1(currentState, queryNeighbor, dataNodeNeighbours) && rule2(currentState, queryNeighbor, dataNodeNeighbours) && rule3(currentState, queryNeighbor, dataNodeNeighbours);
                neo4jDebugger.checkPartialEmbedding(currentState.getM(), queryNodeId, dataNodeId, EmbeddingCheckType.WithGroundTruth, isJoinable);
                if (isJoinable) {
                    StateWithCustomVertex copy = currentState.createDeepCopy();
                    copy.addMapping(queryNodeId, dataNodeId);
                    neo4jDebugger.checkUpdatedState(copy.getM(), queryNodeId, dataNodeId);
                    searchProc(copy);
                    neo4jDebugger.checkRestoredState(currentState.getM(), queryNodeId, dataNodeId);
                }
            }
        }
    }

    private boolean rule1(StateWithCustomVertex currentState, Set<LabeledVertex> queryNeighbor, Set<Integer> dataNeighbor) {
//        Set<LabeledVertex> queryNeighboursInMapping = Utils.intersection(queryNeighbor, getVertexListFromNodeList((currentState.getM().keySet())));
//        for (LabeledVertex node : queryNeighboursInMapping) {
//            if (currentState.getM().containsKey(node.getNodeId())) {
//                int value = currentState.getM().get(node.getNodeId());
//                if (!dataNeighbor.contains(value)) {
//                    return false;
//                }
//            }
//        }
//        return true;

        Set<Integer> dataNeighboursInMapping = Utils.intersection(dataNeighbor, new HashSet<>(currentState.getM().values()));
        Set<LabeledVertex> queryNeighboursInMapping = Utils.intersection(queryNeighbor, getVertexListFromNodeList(currentState.getM().keySet()));

        List<Integer> keys = new ArrayList<>(currentState.getM().keySet());
        List<Integer> values = new ArrayList<>(currentState.getM().values());

        for(int node: dataNeighboursInMapping){
            int position = values.indexOf(node);
            int val = keys.get(position);

            LabeledVertex queryVertex = queryGraph.vertexSet().stream().filter(v -> v.getNodeId() == val).findAny().get();
            if (queryNeighbor.contains(queryVertex)) {
                continue;
            }
            return false;
        }

        for(LabeledVertex node: queryNeighboursInMapping){
            if (currentState.getM().containsKey(node.getNodeId())){
                int value = currentState.getM().get(node.getNodeId());
                if (!dataNeighbor.contains(value)){
                    return false;
                }
            }
        }
        return true;

    }

    private boolean rule2(StateWithCustomVertex state, Set<LabeledVertex> queryNeighbor, Set<Integer> dataNeighbor) {
        Set<Integer> data = Utils.intersection(dataNeighbor, state.getT1());
        Set<LabeledVertex> query = Utils.intersection(queryNeighbor, state.getT2());
        if (data.size() >= query.size()) {
            return true;
        }
        return false;
    }

    private boolean rule3(StateWithCustomVertex state, Set<LabeledVertex> queryNeighbor, Set<Integer> dataNeighbor) {
        Set<Integer> data = Utils.intersection(dataNeighbor, state.getN1());
        Set<LabeledVertex> query = Utils.intersection(queryNeighbor, state.getN2());
        if (data.size() >= query.size()) {
            return true;
        }
        return false;
    }

    private void computeTnN(StateWithCustomVertex state) {
        Set<Integer> neighbours = getNeighbours(new HashSet<>(state.getM().values()));
        neighbours.removeAll(state.getM().values());
        state.setT1(neighbours);

        Set<Integer> temp = allDataNodes;
        temp.removeAll(state.getT1());
        temp.removeAll(state.getM().values());
        state.setN1(temp);

        Set<LabeledVertex> queryNodeNeighbours = new HashSet<>();
        for (int u : state.getM().keySet()) {
            queryNodeNeighbours.addAll(Graphs.neighborListOf(queryGraph, allQueryNodes.get(u)));
        }
        queryNodeNeighbours.removeAll(getVertexListFromNodeList(state.getM().keySet()));
        state.setT2(queryNodeNeighbours);

        Set<LabeledVertex> temp1 = state.getN2();//new HashSet<>(queryGraph.vertexSet());
        temp1.removeAll(state.getT2());
        temp1.removeAll(getVertexListFromNodeList(state.getM().keySet()));
        state.setN2(temp1);
    }

    private Set<Integer> getNeighbours(Set<Integer> nodes) {
        Set<Integer> results = new HashSet<>();
        for (int node : nodes) {
            results.addAll(getNeighboursOfNode(node));
        }
        return results;
    }

    private Set<Integer> getNeighboursOfNode(int node) {
        Set<Integer> results = new HashSet<>();
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

    private Set<Integer> getNodeListFromVertexList(Set<LabeledVertex> vertexList) {
        Set<Integer> nodeList = new HashSet<>();
        for (LabeledVertex vertex : vertexList) {
            nodeList.add(vertex.getNodeId());
        }
        return nodeList;
    }

    private Set<LabeledVertex> getVertexListFromNodeList(Set<Integer> nodeList) {
        Set<LabeledVertex> vertexList = new HashSet<>();
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
            Set<Integer> dataNeighbors = getNeighboursOfNode(v);
            vertexSetU.addAll(queryNeighbors);
            vertexSetV.addAll(dataNeighbors);

            if (vertexSetU.size() > vertexSetV.size()) {
                (candidateMap.get(u)).remove((Object) v);
                continue;
            }

            for (LabeledVertex u_dash : queryNeighbors) {
                Set<Integer> validEndpoints = Utils.intersection(dataNeighbors, candidateMap.get(u_dash.getNodeId()));
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
        Set<Integer> dataNeighbors = getNeighboursOfNode(v);
        for (LabeledVertex u_node : queryNeighbors) {
            Set<Integer> validEndpoints = Utils.intersection(dataNeighbors, candidateMap.get(u_node.getNodeId()));
            for (int endpoint : validEndpoints) {
                markedMapping.add(new Pair<>(u_node.getNodeId(), endpoint));
            }
        }
    }


    private void computeOrder() {
//        nodeOrder = new ArrayList<>();

        int minValue = Integer.MAX_VALUE;
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
            Set<LabeledVertex> neighborList = new HashSet<>();
            for (int u : nodeOrder) {
                neighborList.addAll(Graphs.neighborListOf(queryGraph, allQueryNodes.get(u)));
            }
            neighborList.removeAll(getVertexListFromNodeList(new HashSet<>(nodeOrder)));
            double minCost = Integer.MAX_VALUE;
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
        allDataNodes = new HashSet<>();
        gamma = 0;
        int nodeCount = 0;
        int edgeCount = 0;

        for (String key : variableLabelMap.keySet()) {
            String lbl = variableLabelMap.get(key);
            Set<Integer> candidates = new HashSet<>();
            ResourceIterator<Node> res = db.findNodes(Label.label(label));
            while (res.hasNext()) {
                Node node = res.next();
                allDataNodes.add((Integer) node.getProperty("id"));
                edgeCount += (Integer) node.getProperty("edge_count");
                nodeCount++;
                if (node.hasLabel(Label.label(lbl))) {
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

    private  Set<Map<Integer, Integer>> getGroundTruth(String queryFilename, String dataFilename, String groundTruthFilename){
        Set<Map<Integer, Integer>> groundTruthMappingList = new HashSet<>();
        try {
            BufferedReader reader;
            reader = new BufferedReader(new FileReader(groundTruthFilename));

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains("QUERY FILE:" + queryFilename)) {
                    if (reader.readLine().contains("DATA FILE:" + dataFilename)) {
                        String cnt = reader.readLine();
//                        String cnt = solCountStr.substring(2, solCountStr.length());
                        int solutionCount = Integer.parseInt(cnt);
                        for (int solIdx = 0; solIdx < solutionCount; solIdx++) {
                            Map<Integer, Integer>  groundTruthMap = new HashMap<>();
                            String solution = reader.readLine();
                            solution = solution.substring(2, solution.length());
                            String[] items = solution.split(";");
                            for(String item:items){
                                String[] values = item.split(",");
                                groundTruthMap.put(Integer.parseInt(values[0]), Integer.parseInt(values[1]));
                            }
                            groundTruthMappingList.add(groundTruthMap);
                        }
                        break;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return groundTruthMappingList;
        }
    }

    private void checkResults(Set<Map<Integer, Integer>> result, Set<Map<Integer, Integer>> groundTruth){
        int matched = 0;

        for(Map<Integer, Integer> gtMapping: groundTruth){
            for(Map<Integer, Integer> rMapping: result){
                boolean match = true;
                for(int key: rMapping.keySet()){
                    if(!gtMapping.containsKey(key) || !Objects.equals(gtMapping.get(key), rMapping.get(key))){
                        match = false;
                        break;
                    }
                }
                matched += match? 1:0;
            }
        }
        System.out.println("True Positive:" + matched);
        System.out.println("False Positive:" + (result.size() - matched));
        System.out.println("False Negative:" + (groundTruth.size() - matched));
    }

}
