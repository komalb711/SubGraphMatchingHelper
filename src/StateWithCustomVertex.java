import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StateWithCustomVertex {

    private HashMap<Integer, Integer> M;
    private List<Integer> T1;
    private List<CustomVertex> T2;
    private List<Integer> N1;
    private List<CustomVertex> N2;

    public StateWithCustomVertex(){
        M = new HashMap<>();
        T1 = new ArrayList<>();
        T2 = new ArrayList<>();
        N1 = new ArrayList<>();
        N2 = new ArrayList<>();
    }

    public StateWithCustomVertex createDeepCopy(){
        StateWithCustomVertex copy = new StateWithCustomVertex();
        copy.setM(new HashMap<>(getM()));
        copy.setT1(new ArrayList<>(getT1()));
        copy.setT2(new ArrayList<>(getT2()));
        copy.setN1(new ArrayList<>(getN1()));
        copy.setN2(new ArrayList<>(getN2()));
        return copy;
    }

    public HashMap<Integer, Integer> getM() {
        return M;
    }

    public void setM(HashMap<Integer, Integer> m) {
        M = m;
    }

    public List<Integer> getT1() {
        return T1;
    }

    public List<CustomVertex> getT2() {
        return T2;
    }

    public List<Integer> getN1() {
        return N1;
    }

    public List<CustomVertex> getN2() {
        return N2;
    }

    public void addMapping(int u, int v){
        M.put(u, v);
    }

    public void removeMapping(int u){
        M.remove(u);
    }

    public void setT1(List<Integer> t1) {
        T1 = t1;
    }

    public void setT2(List<CustomVertex> t2) {
        T2 = t2;
    }

    public void setN1(List<Integer> n1) {
        N1 = n1;
    }

    public void setN2(List<CustomVertex> n2) {
        N2 = n2;
    }

    public void printState(){
        System.out.println("Mapping:");
        for( int key: M.keySet() ){
            System.out.print(key+" : " + M.get(key) + ", ");
        }
        System.out.println("T1:");
        for( int node: T1 ){
            System.out.print(node + " ");
        }
        System.out.println("T2:");
        for( CustomVertex node: T2 ){
            System.out.print(node.getNodeId() + " ");
        }

        System.out.println("N1:");
        for( int node: N1 ){
            System.out.print(node + " ");
        }
        System.out.println("N2:");
        for( CustomVertex node: N2 ){
            System.out.print(node.getNodeId() + " ");
        }
    }

}
