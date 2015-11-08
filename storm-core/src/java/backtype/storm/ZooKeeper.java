package backtype.storm;

import backtype.storm.entity.InProcessZKInfo;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.Properties;

/**
 * @author float.lu
 */
public class ZooKeeper {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(ZooKeeper.class);

    public static InProcessZKInfo makeInprocessZooKeeper(final String localdir, int port) throws IOException,InterruptedException{
        File localFile = new File(localdir);
        ZooKeeperServer zk = new ZooKeeperServer(localFile, localFile, 2000);

        NIOServerCnxnFactory factory = new NIOServerCnxnFactory();
        if(port == 0){
            port = 2000;
        }
        while(port < 65535){
            try {
                factory.configure(new InetSocketAddress(port),0);
            }catch (BindException be){
                port++;
                if(port > 65535){
                    throw new RuntimeException("No port is available to launch an inprocess zookeeper.");
                }
                continue;
            }
            break;
        }
        LOG.info("Starting inprocess zookeeper at port " + port + " and dir " + localdir);
        factory.startup(zk);
        return new InProcessZKInfo(port, factory);
    }

}
