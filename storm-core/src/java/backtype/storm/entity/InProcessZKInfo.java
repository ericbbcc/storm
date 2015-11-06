package backtype.storm.entity;

import org.apache.zookeeper.server.NIOServerCnxnFactory;

/**
 * @author float.lu
 */
public class InProcessZKInfo {
    private int port;
    private NIOServerCnxnFactory factory;

    public InProcessZKInfo(int port, NIOServerCnxnFactory factory) {
        this.port = port;
        this.factory = factory;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public NIOServerCnxnFactory getFactory() {
        return factory;
    }

    public void setFactory(NIOServerCnxnFactory factory) {
        this.factory = factory;
    }
}
