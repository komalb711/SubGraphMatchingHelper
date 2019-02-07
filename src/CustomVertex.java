import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

import java.util.Set;

public class CustomVertex {
    private int nodeId;
    private String label;

    public CustomVertex() {
    }

    public CustomVertex(int id, String lbl) {
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
        return "CustomVertex{" +
                "nodeId=" + nodeId +
                ", label='" + label + '\'' +
                '}';
    }

    public static void main(String[] args) {
        SimpleGraph<CustomVertex, DefaultEdge> g =
                new SimpleGraph<>(DefaultEdge.class);
        CustomVertex v1 = new CustomVertex(1, "N");
        CustomVertex v2 = new CustomVertex(2, "R");

        g.addVertex(v1);
        g.addVertex(v2);

        DefaultEdge edge = g.addEdge(v1, v2);
        Set<CustomVertex> vertices = g.vertexSet();
        for (CustomVertex vertex : vertices) {
            if ("N".equals(vertex.getLabel())) {
                System.out.println(vertex);
            }
        }
    }
}