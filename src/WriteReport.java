import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author: Komal Bhavsar (kvb9573@rit.edu)
 * Rochester Institute of Technology
 * CS MS Capstone Project - Spring 2019
 *
 * WriteReport.java
 *
 */

public class WriteReport {

    private String fileName;
    private FileWriter fileWriter;
    private boolean hasIssues;
    private boolean hasWarning;

    public WriteReport(String dataGraphName, String queryGraphName) {
        this.fileName = "Report_" + (new java.sql.Date(System.currentTimeMillis())
                + ".txt");
        this.hasIssues = false;
        this.hasWarning = false;
        try {
            this.fileWriter = new FileWriter(fileName, true);
            this.fileWriter.write("\n\n\nDataGraph:" + dataGraphName);
            this.fileWriter.write("\nQueryGraph:" + queryGraphName);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeCandidates(Map<Integer, Set<Integer>> candidates, CandidateType checkType) {
        try {
            this.fileWriter.write("\nCandidates:Filter Type:" + checkType.toString());
            for (int queryNode : candidates.keySet()) {
                this.fileWriter.write("\nQ_" + queryNode + ": " + candidates.get(queryNode));
            }
            hasIssues = false;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void candidateIssues(int queryNodeId, Set<Integer> unexpectedCandidates) {
        try {
            if (hasIssues == false) {
                this.fileWriter.write("\nCandidate Issues:");
            }
            hasIssues = true;
            this.fileWriter.write("\n:Q_" + queryNodeId + ": unexpected candidates:" + unexpectedCandidates);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeOrder(List<Integer> order) {
        try {
            this.fileWriter.write("\nQuery Order:\n");
            this.fileWriter.write(order.toString());
            hasIssues = false;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void queryOrderIssues(int queryNode) {
        try {
            if (hasIssues == false) {
                this.fileWriter.write("\nQuery Order Issues:");
            }
            hasIssues = true;
            this.fileWriter.write("\nMissing query node:" + queryNode);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void queryOrderWarning(String message) {
        try {
            if (hasWarning == false) {
                this.fileWriter.write("\nQuery Order Warning:");
            }
            hasWarning = true;
            this.fileWriter.write("\n" + message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writePartialEmbeddingError(Map<Integer, Integer> mapping, int u, int v, boolean isJoinable, boolean shouldJoin, SubgraphMatchingType type) {
        try {
            this.fileWriter.write("\nPartial Mapping Issue(" + type.toString() + "): Mapping" + mapping.toString() + " u:" + u + ", v:" + v + " isJoinable:" + isJoinable + " shouldJoin:" + shouldJoin);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writePartialEmbeddingWarning(Map<Integer, Integer> mapping, int u, int v, boolean isJoinable, boolean shouldJoin, SubgraphMatchingType type) {
        try {
            this.fileWriter.write("\nPartial Mapping Warning(" + type.toString() + "): Mapping" + mapping.toString() + " u:" + u + ", v:" + v + " isJoinable:" + isJoinable + " shouldJoin:" + shouldJoin);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeFullEmbeddingError(Map<Integer, Integer> mapping, SubgraphMatchingType type) {
        try {
            this.fileWriter.write("\nFull Embedding did not match(" + type.toString() + "): Mapping" + mapping.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeNextVertexError(int expectedVertex, int actualVertex) {
        try {
            this.fileWriter.write("\nError in Next Vertex, should be" + expectedVertex + " but got " + actualVertex);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeStateError(Map<Integer, Integer> expected, Map<Integer, Integer> mapping, int u, int v, boolean updateState) {
        try {
            this.fileWriter.write("\nCurrent Embedding Issue " + (updateState ? "for UpdateState" : "for Restore State") + ":( u:" + u + ", v:" + v);
            this.fileWriter.write("\nCurrent Embedding:" + mapping.toString());
            this.fileWriter.write("\nExpected Embedding:" + expected.toString() + "\n");
            hasWarning = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeMissingCandidates(Map<Integer, Set<Integer>> missingCandidateMap) {
        try {
            this.fileWriter.write("\nCandidate Issues:(Missing Candidates from Ground Truth)");
            for (int key : missingCandidateMap.keySet()) {
                this.fileWriter.write("\nQ" + key + " : " + missingCandidateMap.get(key));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeFullEmbeddings(Set<Map<Integer, Integer>> implementationResult, Set<Map<Integer, Integer>> groundTruth) {
        try {
            this.fileWriter.write("\nImplementation Result: Count(" + implementationResult.size() + ")");
            for (Map<Integer, Integer> embedding : implementationResult) {
                this.fileWriter.write("\n" + embedding.toString());
            }
            this.fileWriter.write("\nGroundTruth Result:Count(" + groundTruth.size() + ")");
            for (Map<Integer, Integer> embedding : groundTruth) {
                this.fileWriter.write("\n" + embedding.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeReport() {
        try {
            this.fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
