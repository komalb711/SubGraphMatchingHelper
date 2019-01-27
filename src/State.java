import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class State {

    private HashMap<Integer, Integer> M;
    private List<Integer> T1;
    private List<Integer> T2;
    private List<Integer> N1;
    private List<Integer> N2;

    public State(){
        M = new HashMap<>();
        T1 = new ArrayList<>();
        T2 = new ArrayList<>();
        N1 = new ArrayList<>();
        N2 = new ArrayList<>();
    }

    public State createDeepCopy(){
        State copy = new State();
        copy.setM(new HashMap<Integer, Integer>(getM()));
        copy.setT1(new ArrayList<Integer>(getT1()));
        copy.setT2(new ArrayList<Integer>(getT2()));
        copy.setN1(new ArrayList<Integer>(getN1()));
        copy.setN2(new ArrayList<Integer>(getN2()));
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

    public List<Integer> getT2() {
        return T2;
    }

    public List<Integer> getN1() {
        return N1;
    }

    public List<Integer> getN2() {
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

    public void setT2(List<Integer> t2) {
        T2 = t2;
    }

    public void setN1(List<Integer> n1) {
        N1 = n1;
    }

    public void setN2(List<Integer> n2) {
        N2 = n2;
    }

    public void printState(){
        System.out.println("Mapping:");
        for( int key: M.keySet() ){
            System.out.println(key+" : " + M.get(key));
        }
        System.out.println("T1:");
        for( int node: T1 ){
            System.out.println(node);
        }
        System.out.println("T2:");
        for( int node: T2 ){
            System.out.println(node);
        }

        System.out.println("N1:");
        for( int node: N1 ){
            System.out.println(node);
        }
        System.out.println("N2:");
        for( int node: N2 ){
            System.out.println(node);
        }
    }


}
