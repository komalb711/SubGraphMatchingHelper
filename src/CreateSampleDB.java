import javafx.util.Pair;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * @Author: Komal Bhavsar (kvb9573@rit.edu)
 * Rochester Institute of Technology
 * CS MS Capstone Project - Spring 2019
 *
 * CreateSampleDB.java
 *
 */


public class CreateSampleDB {

    private static String PROJECT_NAME = "SAMPLES";
    private static BatchInserter inserter;
    private static CreateSampleDB instance = null;
    private HashMap<Integer, String> nodeLabelMapping;
    private Map<Integer, Integer> nodeEdgeCounts;
    private List<Pair<Integer, Integer>> relationshipMapping;
    private Map<Integer, List<String>> neighbors;

    public static CreateSampleDB getInstance() {
        if (instance == null) {
            instance = new CreateSampleDB();
        }
        return instance;
    }

    public static void main(String[] args) {
        try {
            String filename = "Proteins/Samples";
            CreateSampleDB obj = CreateSampleDB.getInstance();
            obj.loadData(filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
        checkData();
    }

    public void parseData(ArrayList<String> content) {
        int nodeCount = Integer.parseInt(content.remove(0));
        nodeLabelMapping = new HashMap<>();
        nodeEdgeCounts = new HashMap<>();
        relationshipMapping = new ArrayList<>();
        neighbors = new HashMap<>();
        for (int i = 0; i < nodeCount; i++) {
            String line = content.remove(0);
            String[] node = line.split(" ");
            nodeLabelMapping.put(Integer.parseInt(node[0]), node[1]);
        }
        while (content.size() > 0) {
            int nodeId = -1;
            ArrayList<String> temp = new ArrayList<>();
            int edgeCount = Integer.parseInt(content.remove(0));

            String nodeLabel = "";
            for (int i = 0; i < edgeCount; i++) {
                String edge = content.remove(0);
                String[] nodes = edge.split(" ");
                if (!(relationshipMapping.contains(new Pair(Integer.parseInt(nodes[0]), Integer.parseInt(nodes[1])))
                        || relationshipMapping.contains(new Pair(Integer.parseInt(nodes[1]), Integer.parseInt(nodes[0]))))) {
                    relationshipMapping.add(new Pair(Integer.parseInt(nodes[0]), Integer.parseInt(nodes[1])));
                }
                nodeId = Integer.parseInt(nodes[0]);
                nodeLabel = nodeLabelMapping.get(Integer.parseInt(nodes[0]));
                temp.add(nodeLabelMapping.get(Integer.parseInt(nodes[1])));
            }
            nodeEdgeCounts.put(nodeId, edgeCount);
            temp.add(nodeLabel);
            neighbors.put(nodeId, temp);
        }
    }

    public void loadData(String filePath) throws IOException {
        long startTime = System.currentTimeMillis();
        File folder = new File(filePath);
        String[] files = folder.list();

        connectToNeo4j();
        int gId = 0;
        for (String file : files) {
            if (!file.contains(".txt")) {
                continue;
            }
            ArrayList<String> content = readFile(filePath + "/" + file);
            String label = file.split(".txt")[0];
            System.out.println(label);
            parseData(content);
            for (int i : nodeLabelMapping.keySet()) {
                HashMap<String, Object> map = new HashMap<>();
                map.put("id", i);
                map.put("edge_count", nodeEdgeCounts.get(i));
                Collections.sort(neighbors.get(i));
                System.out.println(neighbors.get(i));
                map.put("profile", neighbors.get(i).stream().toArray(String[]::new));//neighbors.get(i).toArray(new String[neighbors.get(i).size()]));
                System.out.println(map);
                inserter.createNode(getUniqueNodeId(i, gId), map, Label.label(label), Label.label(nodeLabelMapping.get(i)));
            }
            for (int i = 0; i < relationshipMapping.size(); i++) {
                Pair<Integer, Integer> pair = relationshipMapping.get(i);
                inserter.createRelationship(getUniqueNodeId(pair.getKey(), gId), getUniqueNodeId(pair.getValue(), gId), RelationshipType.withName("link"), null);
            }
            gId++;
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Load Time:" + (endTime - startTime) + " milliseconds");
        closeConnectionToNeo4j();
    }

    private static int getUniqueNodeId(int nodeId, int gId) {
        return gId * 10000 + nodeId;
    }

    private static void checkData() {
        GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
        GraphDatabaseService db = dbFactory.newEmbeddedDatabase(new File(PROJECT_NAME));
        Result res = db.execute("MATCH(G:L) RETURN G.id, G.edge_count");
        while (res.hasNext()) {
            Map<String, Object> obj = res.next();
            System.out.println(obj.get("G.id") + " " + obj.get("G.edge_count"));
        }
        res.close();
        db.shutdown();
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

    private void connectToNeo4j() throws IOException {
        inserter = BatchInserters.inserter(new File(PROJECT_NAME));
    }

    private void closeConnectionToNeo4j() {
        inserter.shutdown();
    }
}
