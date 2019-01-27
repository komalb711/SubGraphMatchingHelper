import javafx.util.Pair;
import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.io.*;
import java.util.*;

/**
 * CSCI 729 - Topics in Data Management - Graph Databases
 * Assignment 8
 * Problem 1
 * @Author - Komal Bhavsar (kvb9573@rit.edu)
 */


public class NaiveMatching_induced {

    private static String PROJECT_NAME = "PROTEIN";
    private GraphDatabaseService db;

    private List<Pair<Integer, Integer>> relationshipMap;
    private Map<String, String> variableLabelMap;
    private Map<Integer, Integer> edgeCounts;
    private Map<Integer, List<Integer>> candidateMap;
    static Graph<Integer, DefaultEdge> queryGraph;
    private Map<Integer, List<String>> queryProfiles;
    private ArrayList<Pair<Integer, Integer>> markedMapping;
    private static ArrayList<Integer> nodeOrder;

    private String dataGraphLabel = "backbones_1RH4";

    private float gamma;

    private static ArrayList<String> validEmbeddings;

    public static void main(String[] args) {
        NaiveMatching_induced obj = new NaiveMatching_induced();
        String queryFilePath = "Proteins/query";
        String dataFilePath = "Proteins/target";
        String groundTruthFilename = "Proteins/ground_truth/Proteins.8.gtr";
        obj.query(queryFilePath, dataFilePath, groundTruthFilename);
    }

    private void query(String queryFilePath, String dataFilePath, String groundTruthFilename){
        File dataFolder = new File(dataFilePath);
        String[] dataFiles = dataFolder.list();

        File queryFolder = new File(queryFilePath);
        String[] queryFiles = queryFolder.list();

        db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(new File(PROJECT_NAME))
                .setConfig(GraphDatabaseSettings.pagecache_memory, "1024M" )
                .setConfig(GraphDatabaseSettings.string_block_size, "60" )
                .setConfig(GraphDatabaseSettings.array_block_size, "300" )
                .newGraphDatabase();

        validEmbeddings = new ArrayList<>();
        for ( String queryFile : queryFiles) {
//            queryFile = "backbones_1EMA.8.sub.grf";
//            queryFile = "backbones_1EMA.8.sub.grf";
            if (queryFile.endsWith(".8.sub.grf")) {
                System.out.println("QUERY FILE:" + queryFile);
                createQueryJGraph( queryFilePath, queryFile);
                findProfileForQueryGraph();
                for(String dataFile : dataFiles) {
//                    dataFile = "backbones_1RH4.grf";
                    validEmbeddings.clear();
                    long startTime = System.currentTimeMillis();
                    System.out.println("DATA FILE:" + dataFile);
                    dataGraphLabel = dataFile.split(".grf")[0];
                    if(!computeCandidates(dataGraphLabel))
                        continue;
//                    printCandidates();
                    searchSpaceReduction();
//                    printCandidates();
                    computeOrder();
                    search(new HashMap<Integer, Integer>());
                    checkGroundTruth(validEmbeddings, queryFile, dataFile, groundTruthFilename);
                    long endTime = System.currentTimeMillis();
                    System.out.println("Execution Time:" + (endTime-startTime) + "milliseconds\n");
                }
            }
        }
        db.shutdown();
    }

    private void search(HashMap<Integer, Integer> embedding){
        if(embedding.size() == variableLabelMap.size()){
            validEmbeddings.add(embedding.toString());
        } else{
            int nodeCnt = embedding.size();
            int queryNodeId =  nodeOrder.get(nodeCnt);
            for(int dataNodeId: candidateMap.get(queryNodeId)){
                if(embedding.containsValue(dataNodeId)){
                    continue;
                }
                if(edgeCheckingNonInduced(embedding, queryNodeId, dataNodeId) && edgeCheckingInduced(embedding, queryNodeId, dataNodeId)) {
                    embedding.put(queryNodeId, dataNodeId);
                    search(embedding);
                    embedding.remove(queryNodeId);
                }
            }
        }
    }

    private boolean edgeCheckingNonInduced(HashMap<Integer, Integer> embedding, int queryNode, int dataNode){
        if(embedding.size() == 0)
            return true;

        List<Integer> queryNeighbor = Graphs.neighborListOf(queryGraph,queryNode);
        List<Integer> embeddingNeighbors = Utils.intersection(queryNeighbor, new ArrayList<>(embedding.keySet()));
        for(int node: embeddingNeighbors){
            if(!doesEdgeExist(embedding.get(node), dataNode)){
                return false;
            }
        }
        return true;
    }

    private boolean edgeCheckingInduced(HashMap<Integer, Integer> embedding, int queryNode, int dataNode){
        if(embedding.size() == 0)
            return true;

        List<Integer> queryNodes= new ArrayList<>(queryGraph.vertexSet());
        queryNodes.removeAll(Graphs.neighborListOf(queryGraph, queryNode));
        queryNodes = Utils.intersection(queryNodes, new ArrayList<>(embedding.keySet()));

        for(Integer node: queryNodes){
            if(doesEdgeExist(dataNode, embedding.get(node))){
                return false;
            }
        }
        return true;
    }

    private boolean doesEdgeExist(int node1, int node2){
        boolean retValue = false;
        try (Transaction tx = db.beginTx()) {
            Node n = db.findNode(Label.label(dataGraphLabel), "id", node1);
            for (Relationship rel: n.getRelationships()){
                int sId = (Integer) rel.getStartNode().getProperty("id");
                int eId = (Integer) rel.getEndNode().getProperty("id");
                if(sId == node2 || eId == node2){
                    retValue = true;
                    break;
                }
            }
            tx.success();
        }
        return retValue;
    }

    private  ArrayList<Integer> getNeighboursOfNode(int node){
        ArrayList<Integer> results = new ArrayList<>();
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

    private void printCandidates(){
        for(int key : candidateMap.keySet()){
            System.out.println(key+":"+candidateMap.get(key));
        }
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
            List<Integer> queryNeighbors = Graphs.neighborListOf(queryGraph, u);
            List<Integer> dataNeighbors = getNeighboursOfNode(v);
            vertexSetU.addAll(queryNeighbors);
            vertexSetV.addAll(dataNeighbors);

            if(vertexSetU.size()>vertexSetV.size()){
//                markedMapping.remove(pair);
//                System.out.println("Candidate " + v + " removed for query node " + u);
                (candidateMap.get(u)).remove((Object)v);
                continue;
            }

            for (int u_dash : queryNeighbors) {
                List<Integer> validEndpoints = Utils.intersection(dataNeighbors, candidateMap.get(u_dash));
                if(validEndpoints.size() == 0){
                    check = true;
                    break;
                }
                for (int endpoint : validEndpoints) {
                    if(!bipartite_graph_edges.containsKey(u_dash)){
                        bipartite_graph_edges.put(u_dash, new ArrayList<Integer>());
                    }
                    bipartite_graph_edges.get(u_dash).add(endpoint);
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
        List<Integer> queryNeighbors = Graphs.neighborListOf(queryGraph, u);
        List<Integer> dataNeighbors = getNeighboursOfNode(v);
        for(int u_node : queryNeighbors){
            List<Integer> validEndpoints = Utils.intersection(dataNeighbors, candidateMap.get(u_node));
            for (int endpoint : validEndpoints) {
                markedMapping.add(new Pair(u_node, endpoint));
            }
        }
    }


    private void computeOrder(){
        nodeOrder = new ArrayList<>();

        int minValue = 100000;
        int minidx = -1;
        for(int key : candidateMap.keySet()){
            if(candidateMap.get(key).size()<minValue){
                minValue = candidateMap.get(key).size();
                minidx = key;
            }
        }

        int startNode = minidx;
        nodeOrder.add(startNode);

        double previousCost = 1;

        while(nodeOrder.size()<variableLabelMap.size()){
            ArrayList<Integer> neighborList = new ArrayList<>();
            for(int u : nodeOrder) {
                neighborList.addAll(Graphs.neighborListOf(queryGraph, u));
            }
            neighborList.removeAll(nodeOrder);
            double minCost = 1000000;
            int minCostNode = -1;
            for(int neighbor: neighborList){
                List<Integer> temp = Graphs.neighborListOf(queryGraph, neighbor);
                int gammaPower = 0;
                for(int t: temp){
                    if(nodeOrder.contains(t)){
                        gammaPower++;
                    }
                }
                double currentCost = previousCost*candidateMap.get(neighbor).size()*Math.pow(gamma, gammaPower);
                if(currentCost<minCost){
                    minCostNode = neighbor;
                    minCost = currentCost;
                }
            }
            nodeOrder.add(minCostNode);
            previousCost = minCost;

        }
    }


    private boolean computeCandidates(String label){
        candidateMap = new HashMap<>();
        gamma = 0;
        int nodeCount = 0;
        int edgeCount = 0;

        try (Transaction tx = db.beginTx()) {
            for (String key : variableLabelMap.keySet()) {
                String lbl = variableLabelMap.get(key);
                ArrayList<Integer> candidates = new ArrayList<>();
                ResourceIterator<Node> res = db.findNodes(Label.label(label));
                while (res.hasNext()) {
                    Node node = res.next();
                    edgeCount+=(Integer)node.getProperty("edge_count");
                    nodeCount++;
                    if(node.hasLabel(Label.label(lbl)) && matchProfiles(node, key) ) {
                        candidates.add((Integer) node.getProperty("id"));
                    }
                }
                candidateMap.put(Integer.parseInt(key), candidates);
            }
            tx.success();
        }

        edgeCount = edgeCount/2;
        gamma = ((float)edgeCount*2)/((float)nodeCount*(nodeCount-1));
        for(int key: candidateMap.keySet()){
            if(candidateMap.get(key).size()==0){
                return false;
            }
        }
        return true;

    }

    private boolean matchProfiles(Node dataNode, String queryNode){
        int nodeId = Integer.parseInt(queryNode);
        String[] profile = (String[]) dataNode.getProperty("profile");
        int v = 0, u = 0;
        while( v < profile.length && u < queryProfiles.get(nodeId).size()){
            if (profile[v].equals(queryProfiles.get(nodeId).get(u))){
                v++;
                u++;
            }
            else if(profile[v].compareTo(queryProfiles.get(nodeId).get(u))<0){
                v++;
            } else {
                return false;
            }
        }
        return true;
    }

    private void findProfileForQueryGraph(){
        queryProfiles = new HashMap<>();
        for(int node : queryGraph.vertexSet()){
            ArrayList<String> profiles = new ArrayList<>();
            profiles.add(variableLabelMap.get(String.valueOf(node)));
            for(int neighbors: Graphs.neighborListOf(queryGraph,node)){
                profiles.add(variableLabelMap.get(String.valueOf(neighbors)));
            }
            Collections.sort(profiles);
            queryProfiles.put(node, profiles);
        }
    }


    private void createQueryJGraph(String filePath,String filename){
        ArrayList<String> content = readFile(filePath + "/" + filename);
        parseFileContent(content);
        queryGraph = new SimpleGraph<Integer, DefaultEdge>(DefaultEdge.class);

        for(String key : variableLabelMap.keySet()){
            queryGraph.addVertex(Integer.parseInt(key));
        }
        for (Pair<Integer, Integer>  pair: relationshipMap) {
            Integer end1 = pair.getKey();
            Integer end2 = pair.getValue();
            queryGraph.addEdge(end1,end2);
        }
    }


    private void parseFileContent(ArrayList<String> content){
        relationshipMap = new ArrayList<>();
        variableLabelMap = new HashMap<>();
        edgeCounts = new HashMap<>();
        content.remove(0);
        int nodeId = 0;
        for( String line : content){
            String[] tokens = line.split(" ");
            if(tokens.length > 1) {
                if (tokens[1].matches("[0-9]+")) {
                    if(!(relationshipMap.contains(new Pair(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[0]))) || relationshipMap.contains(new Pair(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]))))){
                        relationshipMap.add(new Pair(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1])));
                    }
                } else {
                    variableLabelMap.put(tokens[0], tokens[1]);
                }
            }
            else {
                edgeCounts.put(nodeId,Integer.parseInt(tokens[0]));
                nodeId++;
            }
        }
    }

    private ArrayList<String> readFile(String filename){
        try
        {
            ArrayList<String> list= new ArrayList<>();
            BufferedReader reader = new BufferedReader(new FileReader(filename));
            String line;
            while ((line = reader.readLine()) != null)
            {
                list.add(line);
            }
            reader.close();
            return list;
        }
        catch (Exception e)
        {
            System.err.format("Exception occurred trying to read '%s'.", filename);
            e.printStackTrace();
            return null;
        }
    }

    private void checkGroundTruth(ArrayList<String> results, String queryFilename, String dataFilename, String groundTruthFilename){
        try {
            BufferedReader reader = null;
            reader = new BufferedReader(new FileReader(groundTruthFilename));

            String line;
            while ((line = reader.readLine()) != null)
            {
                boolean solChecked = false;
                if(line.contains("T:"+dataFilename)){
                    if(reader.readLine().contains("P:" + queryFilename))
                    {
                        int matched = 0, unmatched = 0;
                        String solCountStr = reader.readLine();
                        String cnt = solCountStr.substring(2, solCountStr.length());
                        int solutionCount = Integer.parseInt(cnt);
                        for(int solIdx = 0; solIdx<solutionCount; solIdx++){
                            String solution = reader.readLine();
                            for(int idx = 0; idx<results.size(); idx++){
                                if(solution.contains(reformResultString(results.get(idx)))) {
                                    matched ++;
                                }
                            }
                        }
                        System.out.println("True Positive:" + matched);
                        System.out.println("False Positive:" + (results.size()-matched));
                        System.out.println("False Negative:" + (solutionCount-matched));
                        solChecked = true;
                    }
                }
                if (solChecked){
                    break;
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String reformResultString(String result){
        result = result.replaceAll(",", ";");
        result = result.replaceAll("=", ",");
        result = result.replaceAll(" ", "");
        result = result.replaceAll("}","");
        result = result.replaceAll("\\{","");
        return result;

    }


}
