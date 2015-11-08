package backtype.storm;

import org.apache.zookeeper.data.ACL;

import java.util.List;

/**
 * @author float.lu
 */
public class Cluster {

    public interface ClusterState{
        String setEphemeralNode(String path, byte[] data, List<ACL> acls);
        void deleteNode(String path);
        String createSequential(String path, byte[] data, List<ACL> acls);
        void setData(String path, byte[] data, List<ACL> acls);

    }


}
