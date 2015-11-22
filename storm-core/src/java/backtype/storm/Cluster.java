package backtype.storm;

import backtype.storm.entity.DataWithVersion;
import backtype.storm.entity.ExecutorBeat;
import backtype.storm.entity.NodePort;
import backtype.storm.generated.*;
import backtype.storm.nimbus.NimbusInfo;
import backtype.storm.utils.AtomicFieldUpdaterUtil;
import backtype.storm.utils.MyCollectionUtils;
import backtype.storm.utils.Time;
import backtype.storm.utils.Utils;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

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
        DataWithVersion getDataWithVersion(String path, boolean isWatch);
        List<String> getChildren(String path, boolean isWatch);
        void mkdirs(String path, List<ACL> acls);
        boolean existsNode(String path, boolean isWatch);
        void close();
        String register(Callback callback);
        void unregister(String id);
        void addListener(ConnectionStateListener listener);
        void syncPath(String path);
    }

    public static class DistributedClusterState implements ClusterState{

        private static final Logger LOG = LoggerFactory.getLogger(DistributedClusterState.class);

        private Map conf;
        private Map authConf;
        private List<ACL> acls;
        private boolean isSperateZkWrite;

        private CuratorFramework zk;
        private Map<String, Callback> callbacks = new ConcurrentHashMap(); //atom
        private AtomicBoolean active;

        private CuratorFramework zkWriter;
        private CuratorFramework zkReader;

        public DistributedClusterState(Map conf, Map authConf, List<ACL> acls, boolean isSperateZkWrite) {
            this.conf = conf;
            this.authConf = authConf;
            this.acls = acls;
            this.isSperateZkWrite = isSperateZkWrite;
            this.zk = ZooKeeper.makeClient(conf,(List<String>)conf.get(Config.STORM_ZOOKEEPER_SERVERS),(Integer)conf.get(Config.STORM_ZOOKEEPER_PORT),
                    null, null, authConf);
            ZooKeeper.mkdirs(zk, (String)conf.get(Config.STORM_ZOOKEEPER_ROOT), acls);
            zk.close();
            zkWriter = ZooKeeper.makeClient(conf, (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS),
                    (Integer) conf.get(Config.STORM_ZOOKEEPER_PORT),
                    (String) conf.get(Config.STORM_ZOOKEEPER_ROOT),
                    new DistributedClusterStateWatcher(),
                    authConf
            );
            if(isSperateZkWrite){
                zkReader = ZooKeeper.makeClient(conf, (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS),
                        (Integer) conf.get(Config.STORM_ZOOKEEPER_PORT),
                        (String) conf.get(Config.STORM_ZOOKEEPER_ROOT),
                        new DistributedClusterStateWatcher(),
                        authConf
                );
            }else{
                zkReader = zkWriter;
            }
        }

        private class DistributedClusterStateWatcher implements Watcher{

            @Override
            public void process(WatchedEvent event) {
                if(DistributedClusterState.this.active.get()){
                    if(Event.KeeperState.SyncConnected != event.getState()){
                        LOG.warn("Received event [" + event.toString() + "] with disconnected Reader Zookeeper.");
                    }
                    if(event.getType() != null){
                        for (String key : DistributedClusterState.this.callbacks.keySet()) {
                            Callback callback = DistributedClusterState.this.callbacks.get(key);
                            callback.setEvent(event).call();
                        }
                    }
                }
            }
        }

        @Override
        public String setEphemeralNode(String path, byte[] data, List<ACL> acls) {
            ZooKeeper.mkdirs(zkWriter, Util.getParentPath(path), acls);//先创建父节点
            if(ZooKeeper.exist(zkWriter, path, false)){
                try {
                    ZooKeeper.setData(zkWriter, path, data);
                }catch (Exception e){
                    LOG.error("Ephemeral node disappeared between checking for existing and setting data");
                    return ZooKeeper.createNode(zkWriter, path, data, CreateMode.EPHEMERAL, acls);
                }
            }else{
                return ZooKeeper.createNode(zkWriter, path, data, CreateMode.EPHEMERAL, acls);
            }
            return path;
        }

        @Override
        public void deleteNode(String path) {
            ZooKeeper.deleteNode(zkWriter, path);
        }

        @Override
        public String createSequential(String path, byte[] data, List<ACL> acls) {
            return ZooKeeper.createNode(zkWriter, path, data, CreateMode.EPHEMERAL, acls);
        }

        @Override
        public void setData(String path, byte[] data, List<ACL> acls) {
            if(ZooKeeper.exist(zkWriter, path, false)){
                ZooKeeper.setData(zkWriter, path, data);
            }else{
                ZooKeeper.mkdirs(zkWriter, Util.getParentPath(path), acls);
                ZooKeeper.createNode(zkWriter, path, data, CreateMode.PERSISTENT, acls);
            }
        }

        @Override
        public byte[] getData(String path, boolean isWatch) {
            return ZooKeeper.getData(zkReader, path, isWatch);
        }

        @Override
        public int getVersion(String path, boolean isWatch) {
            return ZooKeeper.getVersion(zkReader, path, isWatch);
        }

        @Override
        public DataWithVersion getDataWithVersion(String path, boolean isWatch) {
            return ZooKeeper.getDataWithVersion(zkReader, path, isWatch);
        }

        @Override
        public List<String> getChildren(String path, boolean isWatch) {
            return ZooKeeper.getChildren(zkReader, path, isWatch);
        }

        @Override
        public void mkdirs(String path, List<ACL> acls) {
            ZooKeeper.mkdirs(zkWriter, path, acls);
        }

        @Override
        public boolean existsNode(String path, boolean isWatch) {
            return ZooKeeper.isNodeExist(zkReader, path, isWatch);
        }

        @Override
        public void close() {
            active.set(false);
            zkWriter.close();
            if(isSperateZkWrite){
                zkReader.close();
            }
        }

        @Override
        public String register(Callback callback) {
            String id = UUID.randomUUID().toString();
            callbacks.put(id, callback);
            return id;
        }

        @Override
        public void unregister(String id) {
            callbacks.remove(id);
        }

        @Override
        public void addListener(ConnectionStateListener listener) {
            ZooKeeper.addListener(zkReader, listener);
        }

        @Override
        public void syncPath(String path) {
            ZooKeeper.syncPath(zkWriter, path);
        }
    }


    public interface StormClusterState{
        List<String> assignments(Callback callback);
        Assignment getAssignmentInfo(String stormId, Callback callback);
        DataWithVersion getAssignmentInfoWithVersion(String stormId, Callback callback);
        int getAssignmentVersion(String stormId, Callback callback);
        List<String> getCodeDistributor(Callback callback);
        List<NimbusSummary> getNimbuses();
        String addNimbusHost(String nimbusId, NimbusSummary nimbusSummary);
        List<NimbusInfo> getCodeDistributorInfo(String stormId);
        List<String> getActiveStorms();
        List<String> getHeartbeatStorms();
        List<String> getErrorTopologies();
        ClusterWorkerHeartbeat getWorkerHeartbeat(String stormId, String node, int port);
        Map<ExecutorInfo, ExecutorBeat> getExecutorsBeats(String stormId, Map<ExecutorInfo,NodePort> executorToNodePort);
        List<String> getSupervisors(Callback callback);
        SupervisorInfo getSupervisorInfo(String supervisorId);
        LogConfig getTopologyLogConfig(String stormId, Callback callback);
        void setWorkerHeartbeat(String stormId, String node, int port, ClusterWorkerHeartbeat heartbeat);
        void removeWorkerHeartbeat(String stormId, String node, int port);
        void setupHeartbeats(String stormId);
        void teardownHeartbeats(String stormId);
        void setWorkerBackpressure(String stormId, String node, int port, boolean on);
        boolean isTopologyBackpressure(String stormId, Callback callback);
        void setupBackpressure(String stormId);
        void removeWorkBackpressure(String stormId, String node, int port);
        void teardownTopologyErrors(String stormId);
        void setSupervisorHearbeat(String supervisorId, SupervisorInfo info);
        void activeStorm(String stormId, StormBase stormBase);
        StormBase makeStormBase(String stormId, Callback callback);
        void updateStorm(String stormId, Map<String,Integer> newExecutors,Map<String,DebugOptions> newComponentDebug);
        void removeStormBase(String stormId);
        void setAssignment(String stormId, Assignment assignment);
        void setupCodeDistributor(String stormId, NimbusInfo nimbusInfo);
        void removeStorm(String stormId);
        void setCredentials(String stormId, Credentials creds, List<ACL> acls);
        Credentials getCredentials(String stormId, Callback callback);
        void reportError(String stormId, String componentId, String node,int port, Throwable error);
        List<ErrorInfo> getErrors(String stormId, String componentId);
        ErrorInfo getLastError(String stormId, String componentId);
        void disconnect();
    }

    public static final String ASSIGNMENTS_ROOT = "assignments";
    public static final String CODE_ROOT = "code";
    public static final String STORMS_ROOT = "storms";
    public static final String SUPERVISORS_ROOT = "supervisors";
    public static final String WORKERBEATS_ROOT = "workerbeats";
    public static final String BACKPRESSURE_ROOT = "backpressure";
    public static final String ERRORS_ROOT = "errors";
    public static final String CODE_DISTRIBUTOR_ROOT = "code-distributor";
    public static final String NIMBUSES_ROOT = "nimbuses";
    public static final String CREDENTIALS_ROOT = "credentials";
    public static final String LOGCONFIG_ROOT = "logconfigs";

    public static final String ASSIGNMENTS_SUBTREE = "/" + ASSIGNMENTS_ROOT;
    public static final String STORMS_SUBTREE = "/" + STORMS_ROOT;
    public static final String SUPERVISORS_SUBTREE = "/" + SUPERVISORS_ROOT;
    public static final String WORKERBEATS_SUBTREE = "/" + WORKERBEATS_ROOT;
    public static final String BACKPRESSURE_SUBTREE = "/" + BACKPRESSURE_ROOT;
    public static final String ERRORS_SUBTREE = "/" + ERRORS_ROOT;
    public static final String CODE_DISTRIBUTOR_SUBTREE = "/" + CODE_DISTRIBUTOR_ROOT;
    public static final String NIMBUSES_SUBTREE = "/" + NIMBUSES_ROOT;
    public static final String CREDENTIALS_SUBTREE = "/" + CREDENTIALS_ROOT;
    public static final String LOGCONFIG_SUBTREE = "/" + LOGCONFIG_ROOT;

    //supervisor path = /supervisors/id
    public static String makeSupervisorPath(String id){
        return SUPERVISORS_SUBTREE + "/" + id;
    }

    public static String makeAssignmentPath(String id){
        return ASSIGNMENTS_SUBTREE + "/" + id;
    }

    public static String makeCodeDistributorPath(String id){
        return CODE_DISTRIBUTOR_SUBTREE + "/" + id;
    }

    public static String makeNimbusPath(String id){
        return NIMBUSES_SUBTREE + "/" + id;
    }

    public static String makeStormPath(String id){
        return STORMS_SUBTREE + "/" + id;
    }

    public static String makeWorkbeatStormRoot(String stormId){
        return WORKERBEATS_SUBTREE + "/" + stormId;
    }

    public static String makeWorkbeatPath(String stormId, String node, int port){
        return makeWorkbeatStormRoot(stormId) + "/" + node + "-" + port;
    }

    public static String makeBackpressureStormRoot(String stormId){
        return BACKPRESSURE_SUBTREE + "/" + stormId;
    }

    public static String makeBackpressurePath(String stormId, String node, int port){
        return makeBackpressureStormRoot(stormId) + "/" + node + "-" + port;
    }

    public static String makeErrorStormRoot(String stormId){
        return ERRORS_SUBTREE + "/" + stormId;
    }

    public static String makeErrorPath(String stormId, String componentId){
        try {
            return makeErrorStormRoot(stormId) + "/" + URLEncoder.encode(componentId, "UTF-8");
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static String makeLastErrorPathSeg(){
        return "last-error";
    }

    public static String makeLastErrorPath(String stormId, String componentId){
        try {
            return makeErrorStormRoot(stormId) + "/" +
                    URLEncoder.encode(componentId, "UTF-8") +
                    "-" + makeLastErrorPathSeg();
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static Long parseErrorPath(String p){
        return Long.parseLong(p.substring(1));
    }

    public static String makeCredentialsPath(String stormId){
        return CREDENTIALS_SUBTREE + "/" + stormId;
    }

    public static String makeLogConfigPath(String stormId){
        return LOGCONFIG_SUBTREE + "/" + stormId;
    }

    public static Callback issueCallback(Class<?> holdingClazz, Object o,  String callback){
        AtomicReferenceFieldUpdater updater = AtomicFieldUpdaterUtil.newRefUpdater(holdingClazz, o.getClass(), callback);
        Callback oldCallback = (Callback)updater.getAndSet(o, null);
        oldCallback.call();
        return oldCallback;
    }

    public static Callback issueMapCallback(Map<String, Callback> map, String key) {
        Callback callback = map.get(key);
        if(callback != null) {
            map.remove(key);
            callback.call();
        }
        return callback;
    }

    public static <T> T maybeDeserialize(byte[] serialized, Class<T> clazz) {
        if (serialized != null) {
            return Utils.deserialize(serialized, clazz);
        }
        return null;
    }


    public static class DefaultStormClusterState implements StormClusterState{

        private static final Logger LOG = LoggerFactory.getLogger(DefaultStormClusterState.class);

        private Object clusterStateSpec;
        private ClusterState clusterState;
        private List<ACL> acls;
        private AtomicBoolean spearateZkWriter = new AtomicBoolean(false);
        private boolean solo;

        private volatile Callback supervisorsCallback;
        private volatile Callback assignmentsCallback;
        private volatile Callback codeDistributorCallback;

        private AtomicReferenceFieldUpdater supervisorsCallbackUpdater = AtomicFieldUpdaterUtil.newRefUpdater(DefaultStormClusterState.class, Callback.class, "supervisorsCallback");
        private AtomicReferenceFieldUpdater assignmentsCallbackUpdater = AtomicFieldUpdaterUtil.newRefUpdater(DefaultStormClusterState.class, Callback.class, "assignmentsCallback");
        private AtomicReferenceFieldUpdater codeDistributorCallbackUpdater = AtomicFieldUpdaterUtil.newRefUpdater(DefaultStormClusterState.class, Callback.class, "codeDistributorCallback");

        private Map<String, Callback> assignmentInfoCallback = Maps.newConcurrentMap();
        private Map<String, Callback> assignmentInfoWithVersionCallback = Maps.newConcurrentMap();
        private Map<String, Callback> assignmentVersionCallback = Maps.newConcurrentMap();
        private Map<String, Callback> backpressureCallback = Maps.newConcurrentMap();
        private Map<String, Callback> stormBaseCallback = Maps.newConcurrentMap();
        private Map<String, Callback> credentialsCallback = Maps.newConcurrentMap();
        private Map<String, Callback> logConfigCallback = Maps.newConcurrentMap();

        private String stateId;

        public DefaultStormClusterState(Object clusterStateSpec , List<ACL> acls, AtomicBoolean spearateZkWriter) {
            this.clusterStateSpec = clusterStateSpec;
            this.acls = acls;
            this.spearateZkWriter = spearateZkWriter;

            if(clusterStateSpec instanceof ClusterState){
                this.clusterState = (ClusterState)clusterStateSpec;
                this.solo = false;
            }else{
                this.solo = true;
                //[true (mk-distributed-cluster-state cluster-state-spec :auth-conf cluster-state-spec :acls acls :separate-zk-writer? separate-zk-writer?)])
            }
            stateId = clusterState.register(new StormClusterStateCallback());
            makeZookeeperDir();
        }

        //创建ZK目录
        private void makeZookeeperDir(){
            List<String> dirsToMake = new ArrayList<>();
            dirsToMake.add(ASSIGNMENTS_SUBTREE);
            dirsToMake.add(STORMS_SUBTREE);
            dirsToMake.add(SUPERVISORS_SUBTREE);
            dirsToMake.add(WORKERBEATS_SUBTREE);
            dirsToMake.add(ERRORS_SUBTREE);
            dirsToMake.add(CODE_DISTRIBUTOR_SUBTREE);
            dirsToMake.add(NIMBUSES_SUBTREE);
            dirsToMake.add(LOGCONFIG_SUBTREE);
            for(String dir : dirsToMake){
                clusterState.mkdirs(dir, acls);
            }
        }

        //Storm集群元数据信息变化会激活这个回调
        private class StormClusterStateCallback implements Callback{
            private WatchedEvent event;

            @Override
            public Callback setEvent(WatchedEvent event) {
                this.event = event;
                return this;
            }


            @Override
            public void call() {
                List<String> tokenList =  Util.tokenizePath(event.getPath());
                if (MyCollectionUtils.isNotEmpty(tokenList)) {
                    String subTree = tokenList.get(0);
                    List<String> args = MyCollectionUtils.subList(tokenList, 1);
                    switch (subTree) {
                        case ASSIGNMENTS_ROOT :
                            if (MyCollectionUtils.isEmpty(args)) {
                                issueCallback(DefaultStormClusterState.class, DefaultStormClusterState.this, "assignmentsCallback");
                            } else {
                                issueMapCallback(DefaultStormClusterState.this.assignmentInfoCallback, args.get(0));
                                issueMapCallback(DefaultStormClusterState.this.assignmentVersionCallback, args.get(0));
                                issueMapCallback(DefaultStormClusterState.this.assignmentInfoWithVersionCallback, args.get(0));
                            }
                            break;
                        case SUPERVISORS_ROOT :
                            issueCallback(DefaultStormClusterState.class, DefaultStormClusterState.this, "supervisorsCallback");
                            break;
                        case CODE_DISTRIBUTOR_ROOT :
                            issueCallback(DefaultStormClusterState.class, DefaultStormClusterState.this, "codeDistributorCallback");
                            break;
                        case STORMS_ROOT :
                            issueMapCallback(DefaultStormClusterState.this.stormBaseCallback, args.get(0));
                            break;
                        case CREDENTIALS_ROOT :
                            issueMapCallback(DefaultStormClusterState.this.credentialsCallback, args.get(0));
                            break;
                        case LOGCONFIG_SUBTREE :
                            issueMapCallback(DefaultStormClusterState.this.logConfigCallback, args.get(0));
                            break;
                        case BACKPRESSURE_ROOT :
                            issueMapCallback(DefaultStormClusterState.this.backpressureCallback, args.get(0));
                            break;
                        default:
                            //this should never happen :(
                            Util.existProcess(300, "Unknow callback for subtree subtree[" + subTree +
                                    "] args[" + args +
                                    "]");
                    }
                }
            }
        }

        @Override
        public List<String> assignments(Callback callback) {
            if(callback != null){
                assignmentsCallback = callback;
            }
            return clusterState.getChildren(ASSIGNMENTS_SUBTREE, callback != null);
        }

        @Override
        public Assignment getAssignmentInfo(String stormId, Callback callback) {
            if (callback != null) {
                assignmentInfoCallback.put(stormId, callback);
            }

            String assignmentPath = makeAssignmentPath(stormId);
            byte[] serialized = clusterState.getData(assignmentPath, callback != null);
            return maybeDeserialize(serialized, Assignment.class);
        }

        @Override
        public DataWithVersion getAssignmentInfoWithVersion(String stormId, Callback callback) {
            if (callback != null) {
                assignmentInfoWithVersionCallback.put(stormId, callback);
            }

            String assignmentPath = makeAssignmentPath(stormId);
            DataWithVersion dataWithVersion = clusterState.getDataWithVersion(assignmentPath, callback != null);
            Assignment assignment = maybeDeserialize(dataWithVersion.getData(), Assignment.class);
            dataWithVersion.setDeserialized(assignment);
            return dataWithVersion;
        }

        @Override
        public int getAssignmentVersion(String stormId, Callback callback) {
            if (callback != null) {
                assignmentVersionCallback.put(stormId, callback);
            }
            return clusterState.getVersion(makeAssignmentPath(stormId), callback != null);
        }

        @Override
        public List<String> getCodeDistributor(Callback callback) {
            if (callback != null) {
                codeDistributorCallbackUpdater.set(this, callback);
            }
            clusterState.syncPath(CODE_DISTRIBUTOR_SUBTREE);//强制ZooKeeper同步数据
            return clusterState.getChildren(CODE_DISTRIBUTOR_SUBTREE, callback != null);
        }

        @Override
        public List<NimbusSummary> getNimbuses() {
            List<NimbusSummary> nimbuses = new ArrayList<>();
            List<String> nimbusIds = clusterState.getChildren(NIMBUSES_SUBTREE, false);//节点变化没有callback?
            for (String nimbusId : nimbusIds) {
                byte[] serialized = clusterState.getData(makeNimbusPath(nimbusId), false);
                NimbusSummary nimbusSummary = maybeDeserialize(serialized, NimbusSummary.class);
                nimbuses.add(nimbusSummary);
            }
            return nimbuses;
        }

        @Override
        public String addNimbusHost(final String nimbusId, final NimbusSummary nimbusSummary) {
            //删除nimbus,确保nimbus是当前Session创建的
            clusterState.deleteNode(makeNimbusPath(nimbusId));
            clusterState.addListener(new ConnectionStateListener() {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                    LOG.info("Connection state listener invoked, zookeeper connection state has changed to " + newState);
                    if (ConnectionState.RECONNECTED == newState) {
                        LOG.info("Connection state has changed to reconnected so setting nimbuses entry one more time");
                        clusterState.setEphemeralNode(makeNimbusPath(nimbusId), Utils.serialize(nimbusSummary), acls);
                    }
                }

            });
            return clusterState.setEphemeralNode(makeNimbusPath(nimbusId), Utils.serialize(nimbusSummary), acls);
        }

        @Override
        public List<NimbusInfo> getCodeDistributorInfo(String stormId) {
            List<NimbusInfo> nimbusInfos = new ArrayList<>();
            String path = makeCodeDistributorPath(stormId);
            clusterState.syncPath(path);
            List<String> children = clusterState.getChildren(path, false);
            for (String child : children) {
                nimbusInfos.add(NimbusInfo.parse(child));
            }
            return nimbusInfos;
        }

        @Override
        public List<String> getActiveStorms() {
            return clusterState.getChildren(STORMS_SUBTREE, false);
        }

        @Override
        public List<String> getHeartbeatStorms() {
            return clusterState.getChildren(WORKERBEATS_SUBTREE, false);
        }

        @Override
        public List<String> getErrorTopologies() {
            return clusterState.getChildren(ERRORS_SUBTREE, false);
        }

        @Override
        public ClusterWorkerHeartbeat getWorkerHeartbeat(String stormId, String node, int port) {
            byte[] serialized = clusterState.getData(makeWorkbeatPath(stormId, node, port), false);
            if(serialized != null) {
                return maybeDeserialize(serialized, ClusterWorkerHeartbeat.class);
            }
            //省略了原来的java -> clojure的逻辑
            return null;
        }

        @Override
        public Map<ExecutorInfo, ExecutorBeat> getExecutorsBeats(String stormId, Map<ExecutorInfo, NodePort> executorToNodePort) {
            Map<NodePort, List<ExecutorInfo>> nodeportToExecutors = MyCollectionUtils.reverseMap(executorToNodePort);
            Map<ExecutorInfo, ExecutorBeat> executorToExeBeats = new HashMap<>();
            for (NodePort nodePort : nodeportToExecutors.keySet()) {
                List<ExecutorInfo> executors= nodeportToExecutors.get(nodePort);
                ClusterWorkerHeartbeat clusterWorkerHeartbeat = getWorkerHeartbeat(stormId, nodePort.getNodeId(), nodePort.getPort());
                Map<ExecutorInfo,ExecutorStats> executorInfoExecutorStats = clusterWorkerHeartbeat.get_executor_stats();
                for (ExecutorInfo executorInfo : executors) {
                    if (executorInfoExecutorStats.containsKey(executorInfo)) {
                        ExecutorBeat executorBeat = new ExecutorBeat();
                        executorBeat.setExecutorStats(executorInfoExecutorStats.get(executorInfo));
                        executorBeat.setWorkerHeartbeat(clusterWorkerHeartbeat);
                        executorToExeBeats.put(executorInfo, executorBeat);
                    }
                }
            }
            return executorToExeBeats;
        }

        @Override
        public List<String> getSupervisors(Callback callback) {
            if (callback != null) {
                supervisorsCallbackUpdater.set(this, callback);
            }
            return clusterState.getChildren(SUPERVISORS_SUBTREE, callback != null);
        }

        @Override
        public SupervisorInfo getSupervisorInfo(String supervisorId) {
            byte[] serialized = clusterState.getData(makeSupervisorPath(supervisorId), false);
            SupervisorInfo supervisorInfo = maybeDeserialize(serialized, SupervisorInfo.class);
            return supervisorInfo;
        }

        @Override
        public LogConfig getTopologyLogConfig(String stormId, Callback callback) {
            if (callback != null) {
                logConfigCallback.put(stormId, callback);
            }
            byte[] serialized = clusterState.getData(makeLogConfigPath(stormId), callback != null);
            return maybeDeserialize(serialized, LogConfig.class);
        }

        @Override
        public void setWorkerHeartbeat(String stormId, String node, int port, ClusterWorkerHeartbeat heartbeat) {
            if (heartbeat.get_executor_stats() != null && heartbeat.get_executor_stats().size() > 0) {
                clusterState.setData(makeWorkbeatPath(stormId, node, port), Utils.serialize(heartbeat), acls);
            }
        }

        @Override
        public void removeWorkerHeartbeat(String stormId, String node, int port) {
            clusterState.deleteNode(makeWorkbeatPath(stormId, node, port));
        }

        @Override
        public void setupHeartbeats(String stormId) {
            clusterState.mkdirs(makeWorkbeatStormRoot(stormId), acls);
        }

        @Override
        public void teardownHeartbeats(String stormId) {
            try {
                clusterState.deleteNode(makeWorkbeatStormRoot(stormId));
            } catch (Exception e) {
                LOG.error("Could not teardown heatbeats for " + stormId);
            }
        }

        /**
         * if znode exists and to be not on?, delete; if exists and on?, do nothing;
         * if not exists and to be on?, create; if not exists and not on?, do nothing
         * @param stormId
         * @param node
         * @param port
         * @param on
         */
        @Override
        public void setWorkerBackpressure(String stormId, String node, int port, boolean on) {
            String path = makeBackpressurePath(stormId, node, port);
            boolean existed = clusterState.existsNode(path, false);
            if (existed) {
                if (!on) {
                    clusterState.deleteNode(path);// delete the znode since the worker is not congested()[不拥塞]
                } else {
                    clusterState.setEphemeralNode(path, null, acls);//create the znode since worker is congested[拥塞]
                }
            }
        }

        /**
         * if the backpresure/storm-id dir is empty, this topology has throttle-on, otherwise not.
         * @param stormId
         * @param callback
         * @return
         */
        @Override
        public boolean isTopologyBackpressure(String stormId, Callback callback) {
            if (callback != null) {
                backpressureCallback.put(stormId, callback);
            }
            String path = makeBackpressureStormRoot(stormId);
            List<String> children = clusterState.getChildren(path, callback != null);
            return children.size() > 0;
        }

        @Override
        public void setupBackpressure(String stormId) {
            clusterState.mkdirs(makeBackpressureStormRoot(stormId), acls);
        }

        @Override
        public void removeWorkBackpressure(String stormId, String node, int port) {
            clusterState.deleteNode(makeBackpressurePath(stormId, node, port));
        }

        @Override
        public void teardownTopologyErrors(String stormId) {
            try {
              clusterState.deleteNode(makeErrorStormRoot(stormId));
            } catch (Exception e) {
                LOG.error("Could not teardown errors for " + stormId);
            }
        }

        @Override
        public void setSupervisorHearbeat(String supervisorId, SupervisorInfo info) {
            clusterState.setEphemeralNode(makeSupervisorPath(supervisorId), Utils.serialize(info), acls);
        }

        @Override
        public void activeStorm(String stormId, StormBase stormBase) {
            clusterState.setData(makeStormPath(stormId), Utils.serialize(stormBase), acls);
        }


        @Override
        public StormBase makeStormBase(String stormId, Callback callback) {
            if (callback != null) {
                stormBaseCallback.put(stormId, callback);
            }
            byte[] serialized = clusterState.getData(makeStormPath(stormId), callback != null);
            StormBase stormBase = maybeDeserialize(serialized, StormBase.class);
            return stormBase;
        }

        @Override
        public void updateStorm(String stormId, Map<String,Integer> newExecutors,Map<String,DebugOptions> newComponentDebug) {
            StormBase base = makeStormBase(stormId, null);
            Map<String,Integer> executors = base.get_component_executors();
            Map<String,DebugOptions> componentDebug = base.get_component_debug();
            executors.putAll(newExecutors);
            for (String key : newComponentDebug.keySet()) {
                if (!componentDebug.containsKey(key)) {
                    componentDebug.put(key, newComponentDebug.get(key));
                } else {
                    DebugOptions newDebugOptions = newComponentDebug.get(key);
                    DebugOptions oldDebugOptions = componentDebug.get(key);
                    oldDebugOptions.set_enable(newDebugOptions.is_enable());
                    oldDebugOptions.set_samplingpct(newDebugOptions.get_samplingpct());
                    //merge
                    componentDebug.put(key, oldDebugOptions);
                }
            }
            base.set_component_executors(executors);
            base.set_component_debug(componentDebug);
            clusterState.setData(makeStormPath(stormId), Utils.serialize(base), acls);
        }

        @Override
        public void removeStormBase(String stormId) {
            clusterState.deleteNode(makeStormPath(stormId));
        }

        @Override
        public void setAssignment(String stormId, Assignment assignment) {
            clusterState.setData(makeAssignmentPath(stormId), Utils.serialize(assignment), acls);
        }

        @Override
        public void setupCodeDistributor(String stormId, NimbusInfo nimbusInfo) {
            String path = makeCodeDistributorPath(stormId) + "/" + nimbusInfo.toHostPortString();
            clusterState.mkdirs(makeCodeDistributorPath(stormId), acls);
            clusterState.deleteNode(path);
            clusterState.setEphemeralNode(path, null, acls);
        }

        @Override
        public void removeStorm(String stormId) {
            clusterState.deleteNode(makeAssignmentPath(stormId));
            clusterState.deleteNode(makeCodeDistributorPath(stormId));
            clusterState.deleteNode(makeCredentialsPath(stormId));
            clusterState.deleteNode(makeLogConfigPath(stormId));
            removeStormBase(stormId);
        }

        @Override
        public void setCredentials(String stormId, Credentials creds, List<ACL> acls) {
            String path = makeCredentialsPath(stormId);
            clusterState.setData(path, Utils.serialize(creds), acls);
        }

        @Override
        public Credentials getCredentials(String stormId, Callback callback) {
            if (callback != null) {
                credentialsCallback.put(stormId, callback);
            }
            return maybeDeserialize(clusterState.getData(makeCredentialsPath(stormId), callback != null), Credentials.class);
        }

        @Override
        public void reportError(String stormId, String componentId, String node,int port, Throwable error) {
            String path = makeErrorPath(stormId, componentId);
            String lastErrorPath = makeLastErrorPath(stormId, componentId);
            ErrorInfo data = new ErrorInfo();
            data.set_error_time_secs(Time.currentTimeSecs());
            data.set_error(ConfigUtils.stringifyError(error));
            data.set_host(node);
            data.set_port(port);
            clusterState.mkdirs(path, acls);
            byte[] serialized = Utils.serialize(data);
            clusterState.mkdirs(path, acls);
            clusterState.createSequential(path + "/e", serialized, acls);
            clusterState.setData(lastErrorPath, serialized, acls);
            List<String> children = clusterState.getChildren(path, false);
            Collections.sort(children, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return parseErrorPath(o1).compareTo(parseErrorPath(o2));
                }
            });
            Collections.reverse(children);
            List<String> toKill = MyCollectionUtils.subList(children, 10);
            for (String k : toKill) {
                clusterState.deleteNode(path + "/" + k);
            }
        }

        @Override
        public List<ErrorInfo> getErrors(String stormId, String componentId) {
            String errorPath = makeErrorPath(stormId, componentId);
            List<ErrorInfo> errorInfos = new ArrayList<>();
            if (clusterState.existsNode(errorPath, false)) {
                List<String> children = clusterState.getChildren(errorPath, false);
                for (String errPath : children) {
                    byte[] serialized = clusterState.getData(errorPath + "/" + errPath, false);
                    ErrorInfo errorInfo = maybeDeserialize(serialized, ErrorInfo.class);
                    if (errorInfo != null) {
                        errorInfos.add(errorInfo);
                    }
                    //(map->TaskError data)
                }
            }
            Collections.sort(errorInfos, new Comparator<ErrorInfo>() {
                @Override
                public int compare(ErrorInfo o1, ErrorInfo o2) {
                    return o1.get_error_time_secs() - o2.get_error_time_secs();
                }
            });
            return errorInfos;
        }


        @Override
        public ErrorInfo getLastError(String stormId, String componentId) {
            String lastErrorPath = makeLastErrorPath(stormId, componentId);
            if (clusterState.existsNode(lastErrorPath, false)) {
                ErrorInfo errorInfo = maybeDeserialize(clusterState.getData(lastErrorPath, false), ErrorInfo.class);
                return errorInfo;
            }
            //(map->TaskError data)
            return null;
        }

        @Override
        public void disconnect() {
            clusterState.unregister(stateId);
        }
    }


}
