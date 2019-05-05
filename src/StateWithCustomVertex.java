import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @Author: Komal Bhavsar (kvb9573@rit.edu)
 * Rochester Institute of Technology
 * CS MS Capstone Project - Spring 2019
 *
 * StateWithCustomVertex.java
 *
 */

public class StateWithCustomVertex {

    private Map<Integer, Integer> M;
    private Set<Integer> T1;
    private Set<LabeledVertex> T2;
    private Set<Integer> N1;
    private Set<LabeledVertex> N2;

    public StateWithCustomVertex() {
        M = new TreeMap<>();
        T1 = new HashSet<>();
        T2 = new HashSet<>();
        N1 = new HashSet<>();
        N2 = new HashSet<>();
    }

    public StateWithCustomVertex createDeepCopy() {
        StateWithCustomVertex copy = new StateWithCustomVertex();
        copy.setM(new TreeMap<>(getM()));
        copy.setT1(new HashSet<>(getT1()));
        copy.setT2(new HashSet<>(getT2()));
        copy.setN1(new HashSet<>(getN1()));
        copy.setN2(new HashSet<>(getN2()));
        return copy;
    }

    public Map<Integer, Integer> getM() {
        return M;
    }

    public void setM(Map<Integer, Integer> m) {
        M = m;
    }

    public Set<Integer> getT1() {
        return T1;
    }

    public Set<LabeledVertex> getT2() {
        return T2;
    }

    public Set<Integer> getN1() {
        return N1;
    }

    public Set<LabeledVertex> getN2() {
        return N2;
    }

    public void addMapping(int u, int v) {
        M.put(u, v);
    }

    public void removeMapping(int u) {
        M.remove(u);
    }

    public void setT1(Set<Integer> t1) {
        T1 = t1;
    }

    public void setT2(Set<LabeledVertex> t2) {
        T2 = t2;
    }

    public void setN1(Set<Integer> n1) {
        N1 = n1;
    }

    public void setN2(Set<LabeledVertex> n2) {
        N2 = n2;
    }

    public void printState() {
        System.out.println("Mapping:");
        for (int key : M.keySet()) {
            System.out.print(key + " : " + M.get(key) + ", ");
        }
        System.out.println("T1:");
        for (int node : T1) {
            System.out.print(node + " ");
        }
        System.out.println("T2:");
        for (LabeledVertex node : T2) {
            System.out.print(node.getNodeId() + " ");
        }

        System.out.println("N1:");
        for (int node : N1) {
            System.out.print(node + " ");
        }
        System.out.println("N2:");
        for (LabeledVertex node : N2) {
            System.out.print(node.getNodeId() + " ");
        }
    }

}
