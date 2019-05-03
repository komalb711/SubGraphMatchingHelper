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


public class GraphQL_SampleData_NonInduced {

    private static String PROJECT_NAME = "SAMPLES";
    private GraphDatabaseService db;

    private List<Pair<Integer, Integer>> relationshipMap;
    private Map<String, String> variableLabelMap;
    private Map<Integer, Set<Integer>> candidateMap;
    private Graph<LabeledVertex, DefaultEdge> queryGraph;
    private Map<Integer, List<String>> queryProfiles;

    private ArrayList<Pair<Integer, Integer>> markedMapping;

    private static List<Integer> nodeOrder;

    private String dataGraphLabel;//= "backbones_1RH4";

    private float gamma;

    private static Set<Integer> allDataNodes;
    private static HashMap<Integer, LabeledVertex> allQueryNodes;

    private static Set<Map<Integer, Integer>> validEmbeddings;

    private JgraphtNeo4jDebugger neo4jDebugger;

    public static void main(String[] args) {
        GraphQL_SampleData_NonInduced obj = new GraphQL_SampleData_NonInduced();
        String queryFilePath = "Proteins/Samples";
        String dataFilePath = "Proteins/Samples";
        String groundTruthFilename = "Proteins/Samples/GroundTruth-NonInduced.text";

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
                if (!dataFile.endsWith(".txt")) {
                    continue;
                }
                dataGraphLabel = dataFile.split(".txt")[0];
                for (String queryFile : queryFiles) {
//                    queryFile = "s_3_1.txt";
                    if (queryFile.endsWith(".txt")) {
                        System.out.println("DATA FILE:" + dataFile);
                        System.out.println("QUERY FILE:" + queryFile);
                        createQueryJGraph(queryFilePath, queryFile);

                        neo4jDebugger.setDataAndQueryGraphName(queryGraph, dataGraphLabel, queryFile, db);
                        Set<Map<Integer, Integer>> groundTruth = getGroundTruth(queryFile, dataFile, groundTruthFilename);
                        neo4jDebugger.setGroundTruth(groundTruth, SubgraphMatchingType.NonInduced);
                        findProfileForQueryGraph();
                        validEmbeddings.clear();
                        nodeOrder.clear();
                        long startTime = System.currentTimeMillis();

                        // compute candidates
                        if (!computeCandidates(dataGraphLabel)) {
                            continue;
                        }
                        searchSpaceReduction();

                        // call assistant to check our candidates
                        neo4jDebugger.checkCandidates(candidateMap, CandidateType.Basic);

                        // compute processing order
                        computeOrder();

                        // call assistant to check our query order
                        neo4jDebugger.checkQueryOrder(nodeOrder, false);

                        search(new HashMap<>(), 0);

                        neo4jDebugger.writeResults(validEmbeddings);

                        // check the embeddings with ground truth
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

    private void search(HashMap<Integer, Integer> embedding, int index){
        if(embedding.size() == nodeOrder.size()){
            neo4jDebugger.checkFullEmbedding(embedding);
            validEmbeddings.add(new HashMap<>(embedding));
        } else{
            int nextNodeId =  nodeOrder.get(index);
            neo4jDebugger.checkNextVertex(nodeOrder, nextNodeId);
            for(int id: candidateMap.get(nextNodeId)){
                if(embedding.containsValue(id)){
                    continue;
                }
                boolean isJoinable = edgeChecking(embedding, nextNodeId, id);// && edgeCheckingInduced(nextNodeId, id, embedding);
                neo4jDebugger.checkPartialEmbedding(embedding, nextNodeId, id, EmbeddingCheckType.WithGraph, isJoinable);
                if(isJoinable) {
                    embedding.put(nextNodeId, id);
                    neo4jDebugger.checkUpdatedState(embedding, nextNodeId, id);
                    search(embedding,index+1);
                    embedding.remove(nextNodeId);
                    neo4jDebugger.checkRestoredState(embedding, nextNodeId, id);
                }
            }
        }
    }

    public boolean edgeCheckingInduced(int u,  int v, HashMap<Integer,Integer> embedding) {

        for(LabeledVertex i :  queryGraph.vertexSet()){
            if(!Graphs.neighborListOf(queryGraph, allQueryNodes.get(u)).contains(i) && embedding.containsKey(i.getNodeId()) ){
                int val =  embedding.get(i.getNodeId());
                String query = " MATCH (n:"+dataGraphLabel+":"+variableLabelMap.get(String.valueOf(i.getNodeId()))+
                        " {id:"+val +" } ) -- (n2:"+dataGraphLabel+":"+ variableLabelMap.get(String.valueOf(u))+
                        " { id: "+v +"} ) return n.id";
                ArrayList<Integer> res = runResult(query,db);
                if(res.size()> 0){
                    return false;
                }
            }
        }
        return true;
    }

    public static ArrayList<Integer>  runResult(String result,GraphDatabaseService db){
        ArrayList<Integer> neigh = new ArrayList<>();
        Result res = db.execute(result);

        while(res.hasNext()){
            Map<String, Object> obj = res.next();
            neigh.add((Integer)obj.get("n.id"));
        }
        return neigh;
    }


    private boolean edgeChecking(HashMap<Integer, Integer> embedding, int queryNode, int dataNode){
        if(embedding.size() == 0)
            return true;
        List<LabeledVertex> neighbours = Graphs.neighborListOf(queryGraph,allQueryNodes.get(queryNode));
        for(int i=0; i<neighbours.size();i++){
            int node = neighbours.get(i).getNodeId();
            if(embedding.containsKey(node)){


                if(!doesEdgeExist(embedding.get(node), variableLabelMap.get(String.valueOf(node)), dataNode, variableLabelMap.get(String.valueOf(queryNode)))){
                    return false;
                }
            }
        }
        return true;
    }

    private boolean doesEdgeExist(int node1, String label1, int node2, String label2){

        String query = "MATCH (N1:"+dataGraphLabel +":" + label1 +
                " {id : "+ node1+"}) -- (N2:"+dataGraphLabel+  ") " +
                "\nRETURN N2.id" ;
        Result res = db.execute(query);
        ArrayList<Integer> results = new ArrayList<>();
        while(res.hasNext()){
            results.add(Integer.parseInt(res.next().get("N2.id").toString()));
        }
        res.close();

        if(results.contains(node2))
            return true;
        return false;
    }


    private Set<LabeledVertex> getVertexListFromNodeList(Set<Integer> nodeList) {
        Set<LabeledVertex> vertexList = new HashSet<>();
        for (int nodeId : nodeList) {
            vertexList.add(allQueryNodes.get(nodeId));
        }
        return vertexList;
    }


    private void computeOrder() {
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
            ArrayList<LabeledVertex> neighborList = new ArrayList<>();
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
                int queryEdgeCount = Graphs.neighborSetOf(queryGraph,allQueryNodes.get(Integer.parseInt(key))).size();
                if (node.hasLabel(Label.label(lbl)) && edgeCount>=queryEdgeCount && matchProfiles(node, key) ) {
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

    private Set<Map<Integer, Integer>> getGroundTruth(String queryFilename, String dataFilename, String groundTruthFilename) {
        Set<Map<Integer, Integer>> groundTruthMappingList = new HashSet<>();
        try {
            BufferedReader reader;
            reader = new BufferedReader(new FileReader(groundTruthFilename));

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains("QUERY FILE:" + queryFilename)) {
                    if (reader.readLine().contains("DATA FILE:" + dataFilename)) {
                        String cnt = reader.readLine();
                        int solutionCount = Integer.parseInt(cnt);
                        for (int solIdx = 0; solIdx < solutionCount; solIdx++) {
                            Map<Integer, Integer> groundTruthMap = new HashMap<>();
                            String solution = reader.readLine();
                            solution = solution.substring(2, solution.length());
                            String[] items = solution.split(";");
                            for (String item : items) {
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

    private void checkResults(Set<Map<Integer, Integer>> result, Set<Map<Integer, Integer>> groundTruth) {
        int matched = 0;

        System.out.println(result);
        for (Map<Integer, Integer> gtMapping : groundTruth) {
            for (Map<Integer, Integer> rMapping : result) {
                boolean match = true;
                for (int key : rMapping.keySet()) {
                    if (!gtMapping.containsKey(key) || !Objects.equals(gtMapping.get(key), rMapping.get(key))) {
                        match = false;
                        break;
                    }
                }
                matched += match ? 1 : 0;
            }
        }
        System.out.println("True Positive:" + matched);
        System.out.println("False Positive:" + (result.size() - matched));
        System.out.println("False Negative:" + (groundTruth.size() - matched));
    }

    private  Set<Integer> getNeighboursOfNode(int node){
        Set<Integer> results = new HashSet<>();
        try (Transaction tx = db.beginTx()) {
            Node n = db.findNode(Label.label(dataGraphLabel), "id", node);
            for (Relationship rel: n.getRelationships()){
                int sId = (Integer) rel.getStartNode().getProperty("id");
                int eId = (Integer) rel.getEndNode().getProperty("id");
                if(sId != node)
                    results.add(sId);
                else
                    results.add(eId);
            }

            tx.success();
        }
        return results;
    }

    private void searchSpaceReduction(){
        ArrayList<Integer> vertexSetU  = new ArrayList<>();
        ArrayList<Integer> vertexSetV  = new ArrayList<>();

        HashMap<Integer, ArrayList<Integer>> bipartite_graph_edges = new HashMap<>();
        markedMapping = new ArrayList<>();
        boolean check = false;

        for(int u : candidateMap.keySet()){
            for( int v : candidateMap.get(u)){
                markedMapping.add(new Pair<>(u,v));
            }
        }

        while(!markedMapping.isEmpty()){
//        for(Pair<Integer, Integer> pair : markedMapping) {
            Pair<Integer, Integer> pair = markedMapping.remove(0);
            int u = pair.getKey();
            int v = pair.getValue();
            bipartite_graph_edges.clear();
            vertexSetU.clear();
            vertexSetV.clear();
            check = false;
            Set<LabeledVertex> queryNeighbors = Graphs.neighborSetOf(queryGraph, allQueryNodes.get(u));
            Set<Integer> dataNeighbors = getNeighboursOfNode(v);
            for(LabeledVertex uNode: queryNeighbors){
                vertexSetU.add(uNode.getNodeId());
            }
//            vertexSetU.addAll(queryNeighbors);
            vertexSetV.addAll(dataNeighbors);

            if(vertexSetU.size()>vertexSetV.size()){
//                markedMapping.remove(pair);
//                System.out.println("Candidate " + v + " removed for query node " + u);
                (candidateMap.get(u)).remove((Object)v);
                continue;
            }

            for (LabeledVertex u_dash : queryNeighbors) {
                Set<Integer> validEndpoints = Utils.intersection(dataNeighbors, candidateMap.get(u_dash.getNodeId()));
                if(validEndpoints.size() == 0){
                    check = true;
                    break;
                }
                for (int endpoint : validEndpoints) {
                    if(!bipartite_graph_edges.containsKey(u_dash.getNodeId())){
                        bipartite_graph_edges.put(u_dash.getNodeId(), new ArrayList<Integer>());
                    }
                    bipartite_graph_edges.get(u_dash.getNodeId()).add(endpoint);
                }
            }
            if(check) {
//                System.out.println("Candidate " + v + " removed for query node " + u);
                (candidateMap.get(u)).remove((Object)v);
                continue;
            }
            if (checkSemiPerfectMatching(bipartite_graph_edges)) {
//                System.out.println("U: " + u + " V: " + v+ " removed from marked list");
//                markedMapping.remove(pair);
            }
            else{
//                System.out.println("Candidate " + v + " removed for query node " + u);
                (candidateMap.get(u)).remove((Object)v);
                markNodes(u,v);
            }
        }
    }


    private boolean checkSemiPerfectMatching( HashMap<Integer, ArrayList<Integer>> bipartite_graph_edges){
        HashMap<Integer, Integer> mapping = new HashMap<>();

        for(int u : bipartite_graph_edges.keySet()){
            ArrayList<Integer> neighbours = bipartite_graph_edges.get(u);
            for(int v : neighbours){
                if(!mapping.containsKey(u) && !mapping.containsValue(v)){
                    mapping.put(u,v);
                    if(findSemiPerfectMatching(bipartite_graph_edges, mapping)){
                        return true;
                    }
                }
            }
            if(mapping.size() == bipartite_graph_edges.keySet().size()){
                return true;
            }
        }
        return false;
    }

    private boolean findSemiPerfectMatching(HashMap<Integer, ArrayList<Integer>> bipartite_graph_edges, HashMap<Integer, Integer> mapping){
        if(mapping.size() == bipartite_graph_edges.keySet().size()){
            return true;
        }
        for(int u : bipartite_graph_edges.keySet()){
            if (!mapping.containsKey(u)){
                ArrayList<Integer> neighbors = bipartite_graph_edges.get(u);
                for(int v: neighbors){
                    if(!mapping.containsValue(v)){
                        mapping.put(u,v);
                        findSemiPerfectMatching(bipartite_graph_edges, mapping);
                        if(mapping.size() == bipartite_graph_edges.keySet().size()){
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    private void markNodes(int u, int v){
        Set<LabeledVertex> queryNeighbors = Graphs.neighborSetOf(queryGraph, allQueryNodes.get(u));
        Set<Integer> dataNeighbors = getNeighboursOfNode(v);
        for(LabeledVertex u_node : queryNeighbors){
            Set<Integer> validEndpoints = Utils.intersection(dataNeighbors, candidateMap.get(u_node.getNodeId()));
            for (int endpoint : validEndpoints) {
                markedMapping.add(new Pair<>(u_node.getNodeId(), endpoint));
            }
        }
    }


}
