import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author: Komal Bhavsar (kvb9573@rit.edu)
 * Rochester Institute of Technology
 * CS MS Capstone Project - Spring 2019
 *
 * HookupInterface.java
 *
 */

public interface HookupInterface {

    void checkCandidates(Map<Integer, Set<Integer>> candidates, CandidateType checkType);

    void checkQueryOrder(List<Integer> order, boolean showWarning);

    void checkNextVertex(List<Integer> order, int u);

    void checkPartialEmbedding(Map<Integer, Integer> mapping, int u, int v, EmbeddingCheckType checkType, boolean isJoinable);

    void checkFullEmbedding(Map<Integer, Integer> mapping);

    void checkUpdatedState(Map<Integer, Integer> mapping, int u, int v);

    void checkRestoredState(Map<Integer, Integer> mapping, int u, int v);

}
