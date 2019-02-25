import java.util.List;
import java.util.Map;

public interface HookupInterface {

    public void checkCandidates(Map<Integer, List<Integer>> candidates, CandidateType checkType);

    public boolean checkSearchOrder(List<Integer> order, boolean showWarning);
}
