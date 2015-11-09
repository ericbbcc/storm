package backtype.storm;

import backtype.storm.entity.DataWithVersion;
import backtype.storm.entity.InProcessZKInfo;
import backtype.storm.utils.Utils;
import backtype.storm.utils.ZookeeperAuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * 包装CuratorFramework的工具类
 * @author float.lu
 */
public class ZooKeeper {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(ZooKeeper.class);

    /**
     * ZooKeeper状态
     */
    private static Map<KeeperState,String> zooKeeperStates;
    /**
     * ZooKeeper事件类型
     */
    private static Map<EventType, String> zooKeeperEventTypes;
    /**
     * ZooKeeper节点创建类型
     */
    private static Map<String, CreateMode> zooKeeperCreateModes;



    static {
        zooKeeperStates = new HashMap<>();
        zooKeeperStates.put(KeeperState.Disconnected, "disconnected");
        zooKeeperStates.put(KeeperState.SyncConnected, "connected");
        zooKeeperStates.put(KeeperState.AuthFailed, "auth-failed");
        zooKeeperStates.put(KeeperState.Expired, "expired");

        zooKeeperEventTypes = new HashMap<>();
        zooKeeperEventTypes.put(EventType.None, "none");
        zooKeeperEventTypes.put(EventType.NodeCreated, "node-created");
        zooKeeperEventTypes.put(EventType.NodeDeleted, "node-deleted");
        zooKeeperEventTypes.put(EventType.NodeDataChanged, "node-data-changed");
        zooKeeperEventTypes.put(EventType.NodeChildrenChanged, "node-children-changed");

        zooKeeperCreateModes = new HashMap<>();
        zooKeeperCreateModes.put("ephemeral", CreateMode.EPHEMERAL);
        zooKeeperCreateModes.put("persistent", CreateMode.PERSISTENT);
        zooKeeperCreateModes.put("sequential", CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public static class DefaultWatcher implements Watcher{
        @Override
        public void process(WatchedEvent event) {
            LOG.info("ZooKeeper state update:" + zooKeeperStates.get(event.getState())
                    + zooKeeperEventTypes.get(event.getType()) + event.getPath());
        }
    }

    private static void defaultWatcher(KeeperState state, EventType type, String path){
        LOG.info("Zookeeper state update:" + state + type + path);
    }


    public CuratorFramework makeClient(Map conf, List<String> severs, int port,
                                     String root, final Watcher watcher, Map authConf){
        if(root == null){
            root = "";
        }

        ZookeeperAuthInfo authInfo = null;
        CuratorFramework client = null;

        if(authConf != null && authConf.size() > 0){
            authInfo = new ZookeeperAuthInfo(authConf);
        }
        if(authInfo != null){
            client = Utils.newCurator(conf, severs, port, root, authInfo);
        }else{
            client = Utils.newCurator(conf, severs, port, root);
        }
        client.getCuratorListenable().addListener(new CuratorListener(){
            @Override
            public void eventReceived(CuratorFramework client, CuratorEvent e) throws Exception {
                if(e.getType() == CuratorEventType.WATCHED){
                    if(watcher != null){
                        watcher.process((WatchedEvent)e);
                    }else{
                        new DefaultWatcher().process((WatchedEvent)e);
                    }
                }
            }
        });
        client.start();
        return client;
    }

    public static String createNode(CuratorFramework zk, String path, byte[] data, CreateMode mode, List<ACL> acls){
        CreateMode createMode = zooKeeperCreateModes.get(mode);
        try {
            return zk.create().creatingParentsIfNeeded()
                    .withMode(createMode)
                    .withACL(acls)
                    .forPath(Util.normalizePath(path), data);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static String createNode(CuratorFramework zk, String path, byte[] data, List<ACL> acls)throws Exception{
        return createNode(zk, path, data, CreateMode.PERSISTENT, acls);
    }

    public static boolean isNodeExist(CuratorFramework zk, String path, boolean isWatch){
        try {
            Stat stat = null;
            if(isWatch){
                stat = zk.checkExists().watched().forPath(Util.normalizePath(path));
            }else{
                stat = zk.checkExists().forPath(Util.normalizePath(path));
            }
            if(stat != null){
                return true;
            }
        }catch (Exception e){
            throw new RuntimeException("checkExists failed!");
        }
        return false;
    }

    public static void deleteNode(CuratorFramework zk, String path){
        String nodePath = Util.normalizePath(path);
        if(isNodeExist(zk, nodePath, false)){
            try {
                zk.delete().deletingChildrenIfNeeded().forPath(nodePath);
            }catch (KeeperException.NoNodeException nne){
                //do nothing
            }catch (Exception e){
                throw new RuntimeException(e);
            }
        }
    }

    public static void mkdirs(CuratorFramework zk, String path, List<ACL> acls){
        String nodePath = Util.normalizePath(path);
        if(path != "/" && !isNodeExist(zk, nodePath, false)){
            mkdirs(zk, Util.getParentPath(nodePath), acls);
        }
        try {
            createNode(zk, nodePath, Util.barr(7), CreateMode.PERSISTENT, acls);
        }catch (RuntimeException e){
            //TODO
        }
    }

    public static void syncPath(CuratorFramework zk, String path){
        try {
            zk.sync().forPath(Util.normalizePath(path));
        }catch (Exception e){
            throw new RuntimeException();
        }
    }

    public static void addListener(CuratorFramework zk, ConnectionStateListener listener){
        zk.getConnectionStateListenable().addListener(listener);
    }

    public static byte[] getData(CuratorFramework zk, String path, boolean isWatch){
        String nodePath = Util.normalizePath(path);
        try {
            if(isNodeExist(zk, path, isWatch)){
                if(isWatch){
                    return zk.getData().watched().forPath(nodePath);
                }else{
                    return zk.getData().forPath(nodePath);
                }
            }
            return null;
        }catch (KeeperException.NoNodeException e){
            return null;
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static DataWithVersion getDataWithVersion(CuratorFramework zk, String path, boolean isWatch){
        Stat stat = new Stat();
        String nodePath = Util.normalizePath(path);
        byte[] data = null;
        try {
            if(isNodeExist(zk, nodePath, isWatch)){
                if(isWatch){
                    data = zk.getData().storingStatIn(stat).watched().forPath(nodePath);
                }else{
                    data = zk.getData().storingStatIn(stat).forPath(nodePath);
                }
            }
            return new DataWithVersion(data, stat.getVersion());
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static int getVersion(CuratorFramework zk, String path, boolean isWatch){
        Stat stat = null;
        try {
            if(isNodeExist(zk, path, isWatch)){
                if(isWatch){
                    stat = zk.checkExists().watched().forPath(Util.normalizePath(path));
                }else{
                    stat = zk.checkExists().forPath(Util.normalizePath(path));
                }
                return stat.getVersion();
            }else{
                throw new KeeperException.NoNodeException();
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static List<String> getChildren(CuratorFramework zk, String path, boolean isWatch){
        try{
            if(isWatch){
                return zk.getChildren().watched().forPath(Util.normalizePath(path));
            }else{
                return zk.getChildren().forPath(Util.normalizePath(path));
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static void setData(CuratorFramework zk, String path, byte[] data){
        try {
            zk.setData().forPath(Util.normalizePath(path), data);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static boolean exist(CuratorFramework zk, String path, boolean isWatch){
        return isNodeExist(zk, path, isWatch);
    }

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

    public static void shutdownInprocessZooKeeper(NIOServerCnxnFactory handler){
        handler.shutdown();
    }

    public static void toNimbusInfo(Participant participant)


}
