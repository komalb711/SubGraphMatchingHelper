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


public class Naive_SampleData_NonInduced {

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
        Naive_SampleData_NonInduced obj = new Naive_SampleData_NonInduced();
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
//                dataFile = "s_3_1.txt";
                if (!dataFile.endsWith(".txt")) {
                    continue;
                }
                System.out.println("DATA FILE:" + dataFile);
                dataGraphLabel = dataFile.split(".txt")[0];
                for (String queryFile : queryFiles) {
//                    queryFile = "s_3_1.txt";
                    if (queryFile.endsWith(".txt")) {

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

                        // call assistant to check our candidates
                        neo4jDebugger.checkCandidates(candidateMap, CandidateType.Basic);

                        // compute processing order
                        computeOrder();

                        // call assistant to check our query order
                        neo4jDebugger.checkQueryOrder(nodeOrder, false);

                        // start recursive search procedure
                        search(new HashMap<>());

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

    private void search(HashMap<Integer, Integer> embedding){
        if(embedding.size() == queryGraph.vertexSet().size()){
            neo4jDebugger.checkFullEmbedding(embedding);
            validEmbeddings.add(new HashMap<>(embedding));
        } else{
            int nodeCnt = embedding.size();
            int nextNodeId =  nodeOrder.get(nodeCnt);
            neo4jDebugger.checkNextVertex(nodeOrder, nextNodeId);
            for(int id: candidateMap.get(nextNodeId)){
                if(embedding.containsValue(id)){
                    continue;
                }
                boolean isJoinable = edgeChecking(embedding, nextNodeId, id);
                neo4jDebugger.checkPartialEmbedding(embedding, nextNodeId, id, EmbeddingCheckType.WithGroundTruth, isJoinable);

                if(isJoinable) {
                    embedding.put(nextNodeId, id);
                    neo4jDebugger.checkUpdatedState(embedding, nextNodeId, id);
                    search(embedding);
                    embedding.remove(nextNodeId);
                    neo4jDebugger.checkRestoredState(embedding, nextNodeId, id);
                }
            }
        }
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
//                        String cnt = solCountStr.substring(2, solCountStr.length());
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

}
