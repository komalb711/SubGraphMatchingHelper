import javafx.util.Pair;
import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import org.neo4j.graphdb.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class Debugger implements HookupInterface {

    private static String DATASET_NAME = "PROTEIN";
    private String dataGraphName = "backbones_1RH4.grf";
    private String queryGraphName = "backbones_1EMA.8.sub.grf";
    static Graph<Integer, DefaultEdge> queryGraph;

    private List<Pair<Integer, Integer>> relationshipMap;
    private Map<String, String> variableLabelMap;
    private Map<Integer, Integer> edgeCounts;

    private GraphDatabaseService db;

    public void setDataAndQueryGraphName(String queryGraph, GraphDatabaseService service) {
        queryGraphName = queryGraph;
        db = service;
        createQueryJGraph(queryGraphName);
    }

    private void createQueryJGraph(String filename) {
        ArrayList<String> content = readFile(filename);
        parseFileContent(content);
        queryGraph = new SimpleGraph<Integer, DefaultEdge>(DefaultEdge.class);

        for (String key : variableLabelMap.keySet()) {
            queryGraph.addVertex(Integer.parseInt(key));
        }
        for (Pair<Integer, Integer> pair : relationshipMap) {
            Integer end1 = pair.getKey();
            Integer end2 = pair.getValue();
            queryGraph.addEdge(end1, end2);
        }
    }

    private void parseFileContent(ArrayList<String> content) {
        relationshipMap = new ArrayList<>();
        variableLabelMap = new HashMap<>();
        edgeCounts = new HashMap<>();
        content.remove(0);
        int nodeId = 0;
        for (String line : content) {

            String[] tokens = line.split(" ");
            if (tokens.length > 1) {
                if (tokens[1].matches("[0-9]+")) {
                    if (!(relationshipMap.contains(new Pair(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[0]))) || relationshipMap.contains(new Pair(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]))))) {
                        relationshipMap.add(new Pair(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1])));
                    }
                } else {
                    variableLabelMap.put(tokens[0], tokens[1]);
                }
            } else {
                edgeCounts.put(nodeId, Integer.parseInt(tokens[0]));
                nodeId++;
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


    @Override
    public boolean checkCandidateList(List<Integer> candidateList, int queryNodeId) {
        List<Integer> inputCandidateList = new ArrayList<>(candidateList);
        List<Integer> ourCandidateList = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Node> res = db.findNodes(Label.label(variableLabelMap.get(queryNodeId + "")));
            while (res.hasNext()) {
                Node node = res.next();
                ourCandidateList.add((Integer) node.getProperty("id"));
            }
            tx.success();
        }
        inputCandidateList.removeAll(ourCandidateList);
        if (inputCandidateList.size() == 0) {
            return true;
        }
        return false;
    }

}
