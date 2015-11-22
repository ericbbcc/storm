package backtype.storm.entity;

/**
 * @author float.lu
 */
public class NodePort {
    private String nodeId;
    private int port;

    public String getNodeId() {
        return nodeId;
    }

    public NodePort(String nodeId, int port) {
        this.nodeId = nodeId;
        this.port = port;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public boolean equals(Object obj) {
        NodePort that = (NodePort)obj;
        return this.nodeId.equals(that.getNodeId()) && this.port == that.getPort();
    }
}
