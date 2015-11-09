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
        byte[] getData(String path, boolean isWatch);
        int getVersion(String path, boolean isWatch);
        byte[] getDataWithVersion(String path, boolean isWatch);
        List<String> getChildren(String path, boolean isWatch);

    }


}
