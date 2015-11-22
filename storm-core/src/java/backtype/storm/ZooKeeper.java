package backtype.storm;

import backtype.storm.entity.DataWithVersion;
import backtype.storm.entity.InProcessZKInfo;
import backtype.storm.nimbus.ILeaderElector;
import backtype.storm.nimbus.NimbusInfo;
import backtype.storm.utils.Utils;
import backtype.storm.utils.ZookeeperAuthInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

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
            LOG.info("ZooKeeper state update:" + event.getState()
                    + event.getType() + event.getPath());
        }
    }

    private static void defaultWatcher(KeeperState state, EventType type, String path){
        LOG.info("Zookeeper state update:" + state + type + path);
    }


    public static CuratorFramework makeClient(Map conf, List<String> severs, int port,
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
        try {
            return zk.create().creatingParentsIfNeeded()
                    .withMode(mode)
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


    public static NimbusInfo toNimbusInfo(Participant participant){
        if(StringUtils.isBlank(participant.getId())){
            throw new RuntimeException("No nimbus leader participant host found, have you started your nimbus hosts?");
        }
        String id = participant.getId();
        NimbusInfo nimbusInfo = NimbusInfo.parse(id);
        nimbusInfo.setLeader(participant.isLeader());
        return nimbusInfo;
    }

    /**
     * LeaderLatchListener当节点失去或者成为领导者的时候会被调用
     */
    public static class LeaderLatchListenerImpl implements LeaderLatchListener {

        private Map conf;
        private CuratorFramework zk;
        private LeaderLatch leaderLatch;
        private String hostName;
        private String STORM_ROOT;

        public LeaderLatchListenerImpl(Map conf, CuratorFramework zk, LeaderLatch leaderLatch) {
            this.conf = conf;
            this.zk = zk;
            this.leaderLatch = leaderLatch;
            try {
                this.hostName = InetAddress.getLocalHost().getCanonicalHostName();
            }catch (UnknownHostException e){
                throw new RuntimeException(e);
            }
            this.STORM_ROOT = conf.get(Config.STORM_ZOOKEEPER_ROOT) + "/storms";

        }

        /**
         * 这里保证只有本地的拓扑集合信息包含ZK上的拓扑集合信息上的时候
         * 当前机器才能够成为Leader
         */
        @Override
        public void isLeader() {
            LOG.info(hostName + " gained leadership, checking if it has all the topology code locally.");
            Set<String> activeTopologyIds = new HashSet<>(getChildren(zk, STORM_ROOT, false));
            String[] localTopologyIds = new File(ConfigUtils.getMasterStormDistRoot(conf)).list();
            Set diffTopology = Util.inFirstNotInSecond(activeTopologyIds, new HashSet(Arrays.asList(localTopologyIds)));

            LOG.info("active-topology-ids [" + StringUtils.join(activeTopologyIds, ",") +
            "] local-topology-ids [" + StringUtils.join(localTopologyIds, ",") +
            "] diff-topology [" + StringUtils.join(diffTopology, ",") + "]");

            if(diffTopology == null || diffTopology.size() == 0){
                LOG.info("Accepting leadership, all active topology found localy.");
            }else{
                LOG.info("code for all active topologies not available locally, giving up leadership.");
                try {
                    leaderLatch.close();
                }catch (IOException e){
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void notLeader() {
            LOG.info(hostName + " lost leadership.");
        }
    }

    public static class ZooKeeperLeaderElector implements ILeaderElector {
        private Map conf;
        private List<String> severs;
        private CuratorFramework zk;
        private String leaderLockPath;
        private String id;
        private LeaderLatch leaderLatch;
        private LeaderLatchListener leaderLatchListener;
        private ReentrantLock lock;

        public ZooKeeperLeaderElector(Map conf) {
            this.conf = conf;
            this.severs = (List<String>)conf.get(Config.STORM_ZOOKEEPER_SERVERS);
            this.zk = makeClient(conf, severs, (Integer)conf.get(Config.STORM_ZOOKEEPER_PORT),
                   null, null, conf);
            this.leaderLockPath = conf.get(Config.STORM_ZOOKEEPER_ROOT) + "/leader-lock";
            this.id = NimbusInfo.fromConf(conf).toHostPortString();
            this.leaderLatch = new LeaderLatch(zk, leaderLockPath, id);
            this.leaderLatchListener = new LeaderLatchListenerImpl(conf, zk, leaderLatch);
        }

        @Override
        public void close() {
            LOG.info("closing zookeeper connection of leader elector.");
        }

        @Override
        public List<NimbusInfo> getAllNimbuses() {
            List<NimbusInfo> allParticipants = new ArrayList<>();
            try {
                Collection<Participant> participants = leaderLatch.getParticipants();
                for(Participant participant : participants){
                    allParticipants.add(toNimbusInfo(participant));
                }
            }catch (Exception e){
                throw new RuntimeException(e);
            }
            return allParticipants;
        }

        @Override
        public NimbusInfo getLeader() {
            try {
                //将leader包装为leaderLatch
                return toNimbusInfo(leaderLatch.getLeader());
            }catch (Exception e){

            }
            return null;
        }

        @Override
        public boolean isLeader() throws Exception {
            return leaderLatch.hasLeadership();
        }

        @Override
        public void removeFromLeaderLockQueue() {
            lock.lock();
            try {
                //只有已经开始的leaderLatch才可以被close掉
                if(LeaderLatch.State.STARTED == leaderLatch.getState()){
                    leaderLatch.close();//放弃Leader选举
                    LOG.info("Removed from leader lock queue.");
                } else{
                    LOG.info("leader latch is not started so no removeFromLeaderLockQueue needed.");
                }
            }catch (Exception e){

            }finally {
                lock.unlock();
            }

        }

        @Override
        public void addToLeaderLockQueue() {
            lock.lock();
            try {
                //如果当前leaderLatch已经被Close了，我们需要创建一个新的实例
                if(leaderLatch.getState() == LeaderLatch.State.CLOSED){
                    leaderLatch = new LeaderLatch(zk, leaderLockPath, id);
                    leaderLatchListener = new LeaderLatchListenerImpl(conf, zk, leaderLatch);
                }else if(leaderLatch.getState() == LeaderLatch.State.LATENT){
                //只有当leaderLatch没有被开始的时候我们才可以调用leaderLatch的start
                    leaderLatch.addListener(leaderLatchListener);
                    leaderLatch.start();//参加leader选举
                    LOG.info("Queued up for leader lock.");
                }else{
                    LOG.info("Node already in queue for leader lock.");
                }
            }catch (Exception e) {
                throw new RuntimeException(e);
            }finally {
                lock.unlock();
            }
        }

        @Override
        public void prepare(Map conf) {
            LOG.info("no-op for zookeeper implementation");
        }
    }


}
