import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

import java.util.Set;

public class SingleLabeledVertex implements LabeledVertex {
    private int nodeId;
    private String label;

    public SingleLabeledVertex() {
    }

    public SingleLabeledVertex(int id, String lbl) {
        nodeId = id;
        label = lbl;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public String toString() {
        return "SingleLabeledVertex{" +
                "nodeId=" + nodeId +
                ", label='" + label + '\'' +
                '}';
    }

    public static void main(String[] args) {
        SimpleGraph<SingleLabeledVertex, DefaultEdge> g =
                new SimpleGraph<>(DefaultEdge.class);
        SingleLabeledVertex v1 = new SingleLabeledVertex(1, "N");
        SingleLabeledVertex v2 = new SingleLabeledVertex(2, "R");

        g.addVertex(v1);
        g.addVertex(v2);

        DefaultEdge edge = g.addEdge(v1, v2);
        Set<SingleLabeledVertex> vertices = g.vertexSet();
        for (SingleLabeledVertex vertex : vertices) {
            if ("N".equals(vertex.getLabel())) {
                System.out.println(vertex);
            }
        }
    }
}