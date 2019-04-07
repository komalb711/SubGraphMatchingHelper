import javafx.util.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

/**
 * CSCI ^629 - Topics in Data Management - Graph Databases
 * Assignment 4 - Graph Databases
 * Question 4
 * Author - Komal Bhavsar
 */

public class Parser {
    static List<Pair<String, String>> relationshipMap;
    static Map<String, String> variableLabelMap;
    static List<Integer> edgeCounts;

    public static void main(String[] args) {
//        String query = getQuery("Proteins/query/backbones_1EE1.16.sub.grf");
//        System.out.println("\nQues1 Result:\n\n"+query);
    }

    public static String getQuery(String filePath, String filename, String targetFilename, Boolean induced){
        ArrayList<String> content = readFile(filePath + "/" + filename);
//        System.out.println("Input file data:");
//        for (String line : content){
//            System.out.println(line);
//        }
        parseFileContent(content);
        return formCypherQuery( targetFilename.split(".txt")[0], induced);
    }

    public static void parseFileContent(ArrayList<String> content){
        relationshipMap = new ArrayList<>();
        variableLabelMap = new HashMap<>();
        edgeCounts = new ArrayList<>();
        content.remove(0);
        for( String line : content){
            String[] tokens = line.split(" ");
            if(tokens.length > 1) {
                if (tokens[1].matches("[0-9]+")) {
                    if(!(relationshipMap.contains(new Pair("N"+tokens[1], "N"+tokens[0])) || relationshipMap.contains(new Pair("N"+tokens[0], "N"+tokens[1])))){
                        relationshipMap.add(new Pair("N"+tokens[0], "N"+tokens[1]));
                    }
                } else {
                    variableLabelMap.put("N"+tokens[0], tokens[1]);
                }
            }
            else {
                edgeCounts.add(Integer.parseInt(tokens[0]));
            }
        }
    }

    private static String formCypherQuery( String label, boolean induced){
        StringBuffer buffer = new StringBuffer();
        buffer.append("MATCH ");

        HashMap<String, Boolean> variableUsedMap = new HashMap<>();
        for (String key : variableLabelMap.keySet()) {
            variableUsedMap.put(key, false);
        }

        for (Pair<String, String>  pair: relationshipMap) {
            String end1 = pair.getKey();
            String end2 = pair.getValue();
            boolean label1Used = variableUsedMap.get(end1);
            boolean label2Used = variableUsedMap.get(end2);
            if(!label1Used){
                variableUsedMap.put(end1, true);
                String label1 = variableLabelMap.get(end1);
                end1 = end1 + ":" + label +":"+label1;
            }
            if (!label2Used) {
                variableUsedMap.put(end2, true);
                String label2 = variableLabelMap.get(end2);
                end2 = end2 + ":" + label +":" + label2;
            }
            buffer.append("("+end1 +") -- " + "("+end2 +"),\n");
        }
        String query = buffer.toString().substring(0, buffer.length()-2);

        buffer = new StringBuffer(query);
        buffer.append("\nWHERE \n");

        if(induced) {
            for (String i :variableLabelMap.keySet()) {
                String node1 = i;
                for (String j :variableLabelMap.keySet()) {
                    if (!Objects.equals(i, j)) {
                        String node2 = j;
                        if (!relationshipMap.contains(new Pair(node1, node2)) && !relationshipMap.contains(new Pair(node2, node1))) {
                            buffer.append("NOT (" + node1 + ") -[:link]- (" + node2 + ") \nAND ");
                        }
                    }
                }
            }
        }

        for (String key1 : variableLabelMap.keySet()) {
            for (String key2 : variableLabelMap.keySet()) {
                if(!Objects.equals(key1, key2)) {
                    buffer.append( key1 +" <>  " + key2 + " \nAND ");
                }
            }
        }

        query = buffer.toString().substring(0, buffer.length()-4);

        buffer = new StringBuffer(query);
        buffer.append("\nRETURN DISTINCT ");
        for (Map.Entry<String, String> entry : variableLabelMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            buffer.append(key + ".id , ");
        }
        query = buffer.toString().substring(0, buffer.length()-2);
        buffer = new StringBuffer(query);
        return buffer.toString();
    }

    private static ArrayList<String> readFile(String filename){
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
}
