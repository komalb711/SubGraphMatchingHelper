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


public class VF2_induced {

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

    private static ArrayList<Integer> allDataNodes;
    private static ArrayList<Integer> allQueryNodes;

    private static ArrayList<String> validEmbeddings;

    public static void main(String[] args) {
        VF2_induced obj = new VF2_induced();
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
            if (queryFile.endsWith(".8.sub.grf")) {
                System.out.println("QUERY FILE:" + queryFile);
                createQueryJGraph( queryFilePath, queryFile);
                findProfileForQueryGraph();
                allQueryNodes = new ArrayList<>(queryGraph.vertexSet());
                for(String dataFile : dataFiles) {
//                    dataFile = "backbones_1RH4.grf";
                    validEmbeddings.clear();
                    long startTime = System.currentTimeMillis();
                    System.out.println("DATA FILE:" + dataFile);
                    dataGraphLabel = dataFile.split(".grf")[0];
                    if(!computeCandidates(dataGraphLabel))
                        continue;
                    printCandidates();
                    searchSpaceReduction();
                    printCandidates();
                    computeOrder();
                    startMatch();
                    checkGroundTruth(validEmbeddings, queryFile, dataFile, groundTruthFilename);
                    long endTime = System.currentTimeMillis();
                    System.out.println("Execution Time:" + (endTime-startTime) + "milliseconds\n");
//                    break;
                }
//                break;
            }
        }
        db.shutdown();
    }

    private void startMatch(){
        int uNode = nodeOrder.get(0);
        for(int vNode: candidateMap.get(uNode)){
            State state = new State();
            state.addMapping(uNode, vNode);
            match(state);
        }

    }

    private  void match(State currentState){
        if(currentState.getM().size() == variableLabelMap.size()){
            validEmbeddings.add(currentState.getM().toString());
        } else{

            computeTnN(currentState);

            int nodeCnt = currentState.getM().size();
            int queryNodeId =  nodeOrder.get(nodeCnt);

            List<Integer> candidates = Utils.intersection(candidateMap.get(queryNodeId), currentState.getT1());

            List<Integer> queryNeighbor = Graphs.neighborListOf(queryGraph, queryNodeId);

            for(int dataNodeId: candidates){
                if (currentState.getM().containsValue(dataNodeId))
                    continue;
                ArrayList<Integer> dataNodeNeighbours = getNeighboursOfNode(dataNodeId);
                if(rule1(currentState, queryNeighbor, dataNodeNeighbours) && rule2(currentState, queryNeighbor, dataNodeNeighbours) && rule3(currentState,queryNeighbor, dataNodeNeighbours)){
                    State copy = currentState.createDeepCopy();
                    copy.addMapping(queryNodeId,dataNodeId);
                    match(copy);
                }
            }

        }
    }

    private boolean rule1(State currentState, List<Integer> queryNeighbor, List<Integer> dataNeighbor){

        List<Integer> dataNeighboursInMapping = Utils.intersection(dataNeighbor, new ArrayList<>(currentState.getM().values()));
        List<Integer> queryNeighboursInMapping = Utils.intersection(queryNeighbor, new ArrayList<>(currentState.getM().keySet()));

        ArrayList<Integer> keys = new ArrayList<>(currentState.getM().keySet());
        ArrayList<Integer> values = new ArrayList<>(currentState.getM().values());

        for(int node: dataNeighboursInMapping){
            int position = values.indexOf(node);
            int val = keys.get(position);

            if (!queryNeighbor.contains(val)){
                return false;
            }
        }

        for(int node: queryNeighboursInMapping){
            if (currentState.getM().containsKey(node)){
                int value = currentState.getM().get(node);
                if (!dataNeighbor.contains(value)){
                    return false;
                }
            }
        }
        return true;
    }

    private boolean rule2(State state, List<Integer> queryNeighbor, List<Integer> dataNeighbor){
        List<Integer> data = Utils.intersection(dataNeighbor, state.getT1());
        List<Integer> query = Utils.intersection(queryNeighbor, state.getT2());

        if( data.size()>= query.size() ) {
            return true;
        }
        return false;
    }

    private boolean rule3(State state, List<Integer> queryNeighbor, List<Integer> dataNeighbor){
        List<Integer> data = Utils.intersection(dataNeighbor, state.getN1());
        List<Integer> query = Utils.intersection(queryNeighbor, state.getN2());

        if( data.size()>= query.size() ) {
            return true;
        }
        return false;
    }

    private void computeTnN(State state){
        ArrayList<Integer> neighbours = getNeighbours(new ArrayList<>(state.getM().values()));
        neighbours.removeAll(state.getM().values());
        state.setT1(neighbours);

        ArrayList<Integer> temp = allDataNodes;
        temp.removeAll(state.getT1());
        temp.removeAll(state.getM().values());
        state.setN1(temp);

        List<Integer> queryNodeNeighbours = new ArrayList<>();
        for(int u : state.getM().keySet()) {
            queryNodeNeighbours.addAll(Graphs.neighborListOf(queryGraph, u));
        }
        queryNodeNeighbours.removeAll(state.getM().keySet());
        state.setT2(queryNodeNeighbours);

        temp = allQueryNodes;
        temp.removeAll(state.getT2());
        temp.removeAll(state.getM().keySet());
        state.setN2(temp);
    }

    private  ArrayList<Integer> getNeighbours(List<Integer> nodes){
        ArrayList<Integer> results = new ArrayList<>();
        for(int node: nodes){
            results.addAll(getNeighboursOfNode(node));
        }
        return results;
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

    private void computeOrder(){
        nodeOrder = new ArrayList<>();

        int minValue = 100000;
        int minIdx = -1;
        for(int key : candidateMap.keySet()){
            if(candidateMap.get(key).size()<minValue){
                minValue = candidateMap.get(key).size();
                minIdx = key;
            }
        }

        int startNode = minIdx;
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

    private void printCandidates(){
        for(int key : candidateMap.keySet()){
            System.out.println(key+":" + candidateMap.get(key).size() + ":" +candidateMap.get(key));
        }
    }

    private void searchSpaceReduction(){
        ArrayList<Integer> vertexSetU  = new ArrayList<>();
        ArrayList<Integer> vertexSetV  = new ArrayList<>();

        HashMap<Integer, ArrayList<Integer>> bipartite_graph_edges = new HashMap<>();
        markedMapping = new ArrayList<>();
        boolean check;

        for(int u : candidateMap.keySet()){
            for( int v : candidateMap.get(u)){
                markedMapping.add(new Pair<>(u,v));
            }
        }

        while(!markedMapping.isEmpty()){
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
                System.out.println("Candidate " + v + " removed for query node " + u);
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
                System.out.println("Candidate " + v + " removed for query node " + u);
                (candidateMap.get(u)).remove((Object)v);
                continue;
            }
            if (checkSemiPerfectMatching(vertexSetU, vertexSetV, bipartite_graph_edges)) {
                System.out.println("U: " + u + " V: " + v+ " removed from marked list");
//                markedMapping.remove(pair);
            }
            else{
                System.out.println("Candidate " + v + " removed for query node " + u);
                (candidateMap.get(u)).remove((Object)v);
                markNodes(u,v);
            }
        }
    }

    private boolean checkSemiPerfectMatching(ArrayList<Integer> vertexSetU, ArrayList<Integer> vertexSetV, HashMap<Integer, ArrayList<Integer>>  bipartite_graph_edges){
        HashMap<Integer, Integer>  mapping = new HashMap<>();
        while (true){
            HashMap<Integer, Integer>  augmentingPath = findAugmentingPath(vertexSetU, vertexSetV, bipartite_graph_edges, mapping);
            if(augmentingPath.isEmpty()){
                return false;
            }

            //xor M and path
            HashMap<Integer, Integer>  temp = new HashMap<>();

            for(int key: augmentingPath.keySet()){
                if(!mapping.containsKey(key) || (mapping.containsKey(key) && !(mapping.get(key).equals(augmentingPath.get(key))))){
                    temp.put(key, augmentingPath.get(key));
                }
            }

            for(int key: mapping.keySet()){
                if(!augmentingPath.containsKey(key) || (augmentingPath.containsKey(key) && !augmentingPath.get(key).equals(mapping.get(key)))){
                    temp.put(key, mapping.get(key));
                }
            }

            mapping = temp;

            if(mapping.size() == bipartite_graph_edges.keySet().size())
                break;
        }
        return mapping.size() == bipartite_graph_edges.keySet().size();
    }

    private HashMap<Integer, Integer>  findAugmentingPath(ArrayList<Integer> vertexSetU, ArrayList<Integer> vertexSetV,  HashMap<Integer, ArrayList<Integer>>  bipartite_graph_edges, HashMap<Integer, Integer>  mapping){
        HashMap<Integer, Integer>  path = new HashMap<>();
        HashMap<Integer, Integer>  reverseEdges = new HashMap<>();

        for(int key: mapping.keySet()){
            reverseEdges.put(mapping.get(key), key);
            bipartite_graph_edges.get(key).remove((Object)mapping.get(key));
        }

        ArrayList<Integer> freeVerticesU = new ArrayList();
        ArrayList<Integer> freeVerticesV = new ArrayList();

        for(int u: vertexSetU){
            if(!mapping.containsKey(u)){
                freeVerticesU.add(u);
            }
        }

        for(int v: vertexSetV){
            if(!mapping.containsValue(v)){
                freeVerticesV.add(v);
            }
        }

        int u = freeVerticesU.get(0);
        boolean even = true;
        while(true){
            if(even) {
                ArrayList<Integer> neighbors = bipartite_graph_edges.get(u);
                List<Integer> common = Utils.intersection(neighbors, freeVerticesV);
                if (common.size() > 0) {
                    path.put(u, common.get(0));
                    return path;
                } else if(neighbors.size()>0){
                    path.put(u, neighbors.get(0));
                    u = neighbors.get(0);
                } else{
                    path.clear();
                    return path;
                }
            }
            else {
                if(reverseEdges.containsKey(u)){
                    path.put(u, reverseEdges.get(u));
                    u = reverseEdges.get(u);
                }
            }
            even = !even;
            if(freeVerticesV.contains(path.get(u))){
                return path;
            }

        }
//        return path;


//        while(true){
//            for( Pair<Integer, Integer> pair : directedGraph){
//                if (pair.getKey() == startU) {
//                    path.add(pair);
//                    break;
//                }
//            }
//            while (path.get(path.size()-1).getValue()!=endV){
//                Pair<Integer, Integer> lastEdge = path.get(path.size()-1);
//                boolean added = false;
//                for( Pair<Integer, Integer> pair : directedGraph){
//                    if (pair.getKey() == lastEdge.getValue() && !path.contains(pair)) {
//                        path.add(pair);
//                        added = true;
//                        break;
//                    }
//                }
//                if(added){
//                    break;
//                }
//            }
//            directedGraph.add(new Pair<>(endV, startU));
//        }



//        for(Pair<Integer, Integer> edge: bipartite_graph_edges){
//            if(freeVerticesU.contains(edge.getKey())){
//                freeVerticesU.remove(edge.getKey());
//            }
//            if(freeVerticesV.contains(edge.getValue())){
//                freeVerticesV.remove(edge.getValue());
//            }
//        }

    }


    private void markNodes(int u, int v){
        System.out.println("Adding nodes to marked mapping");
        List<Integer> queryNeighbors = Graphs.neighborListOf(queryGraph, u);
        List<Integer> dataNeighbors = getNeighboursOfNode(v);
        for(int u_node : queryNeighbors){
            List<Integer> validEndpoints = Utils.intersection(dataNeighbors, candidateMap.get(u_node));
            for (int endpoint : validEndpoints) {
                System.out.println(new Pair(u_node, endpoint));
                markedMapping.add(new Pair(u_node, endpoint));
            }
        }
    }


    private boolean computeCandidates(String label){
        candidateMap = new HashMap<>();
        allDataNodes = new ArrayList<>();
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
                    allDataNodes.add((Integer)node.getProperty("id"));
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
