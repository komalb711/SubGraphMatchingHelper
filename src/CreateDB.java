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
 * CSCI 729 - Topics in Data Management - Graph Databases
 * Assignment 7
 * Problem 1
 *
 * @Author - Komal Bhavsar (kvb9573@rit.edu)
 */


public class CreateDB {

    private static String PROJECT_NAME = "PROTEIN";
    private static BatchInserter inserter;
    private static CreateDB instance = null;
    private HashMap<Integer, String> nodeLabelMapping;
    private List<Integer> nodeEdgeCounts;
    private List<Pair<Integer, Integer>> relationshipMapping;
    private List<List<String>> neighbors;

    public static CreateDB getInstance() {
        if (instance == null) {
            instance = new CreateDB();
        }
        return instance;
    }

    public static void main(String[] args) {
//        try {
//            String filename = "Proteins/target";
//            CreateDB obj = CreateDB.getInstance();
//            obj.loadData(filename);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        checkData();
    }

    public void parseData(ArrayList<String> content) {
        int nodeCount = Integer.parseInt(content.remove(0));
        nodeLabelMapping = new HashMap<>();
        nodeEdgeCounts = new ArrayList<>();
        relationshipMapping = new ArrayList<>();
        neighbors = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            String line = content.remove(0);
            String[] node = line.split(" ");
            nodeLabelMapping.put(Integer.parseInt(node[0]), node[1]);
        }
        while (content.size() > 0) {
            ArrayList<String> temp = new ArrayList<>();
            int edgeCount = Integer.parseInt(content.remove(0));
            nodeEdgeCounts.add(edgeCount);
            String nodeLabel = "";
            for (int i = 0; i < edgeCount; i++) {
                String edge = content.remove(0);
                String[] nodes = edge.split(" ");
                if (!(relationshipMapping.contains(new Pair(Integer.parseInt(nodes[0]), Integer.parseInt(nodes[1])))
                        || relationshipMapping.contains(new Pair(Integer.parseInt(nodes[1]), Integer.parseInt(nodes[0]))))) {
                    relationshipMapping.add(new Pair(Integer.parseInt(nodes[0]), Integer.parseInt(nodes[1])));
                }
                nodeLabel = nodeLabelMapping.get(Integer.parseInt(nodes[0]));
                temp.add(nodeLabelMapping.get(Integer.parseInt(nodes[1])));
            }
            temp.add(nodeLabel);
            neighbors.add(temp);
        }

    }

    public void loadData(String filePath) throws IOException {
        long startTime = System.currentTimeMillis();
        File folder = new File(filePath);
        String[] files = folder.list();

        connectToNeo4j();
        int gId = 0;
        for (String file : files) {
            ArrayList<String> content = readFile(filePath + "/" + file);
            String label = file.split(".grf")[0];
            parseData(content);
            for (int i = 0; i < nodeLabelMapping.size(); i++) {
                HashMap<String, Object> map = new HashMap<String, Object>();
                map.put("id", i);
                map.put("edge_count", nodeEdgeCounts.get(i));
//                map.put("profile", getStringOfNeighbors(neighbors.get(i)));
                Collections.sort(neighbors.get(i));
                map.put("profile", neighbors.get(i).toArray(new String[neighbors.get(i).size()]));
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
        return gId * 100000 + nodeId;
    }

    private static void checkData() {
        GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
        GraphDatabaseService db = dbFactory.newEmbeddedDatabase(new File(PROJECT_NAME));
//        Result res = db.execute("MATCH(G:backbones_1RH4:C) RETURN G.id, G.edge_count, G.profile");
        Result res = db.execute("MATCH(N1:backbones_1RH4{id:0})-[r] -(N2:backbones_1RH4{id:6}) WHERE N1.id <> N2.id RETURN SIGN(COUNT(r)) as RES");
        System.out.println("Sample query: Graph from target file backbones_140L and label N");
        while (res.hasNext()) {
//            res.next().get("RES");
            Map<String, Object> obj = res.next();
            long result = (long) obj.get("RES");
            System.out.println("Result::" + result + ":type:" + obj.get("RES").getClass().getName());
            String[] profile = (String[]) obj.get("G.profile");
            System.out.println(obj.get("G.id") + " " + obj.get("G.edge_count") + " " + Arrays.toString(profile));
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
