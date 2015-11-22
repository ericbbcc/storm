package backtype.storm.daemon;

import backtype.storm.Config;
import backtype.storm.ConfigUtils;
import backtype.storm.Util;
import backtype.storm.generated.*;
import backtype.storm.nimbus.NimbusInfo;
import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.IScheduler;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.*;

/**
 * @author float.lu
 */
public class ServiceHandler implements Nimbus.Iface, Shutdownable, DaemonCommon {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceHandler.class);

    private Map conf;
    private INimbus iNimbus;

    private Properties nimbus;

    public ServiceHandler(Map conf, INimbus iNimbus) {
        this.conf = conf;
        this.iNimbus = iNimbus;
        prepare();
        LOG.info("Starting Nimbus with conf" + conf);
        initProperties();
    }

    private void prepare(){
        try {
            iNimbus.prepare(conf, ConfigUtils.getMasterInimbusDir(conf));
        }catch (Exception e){
            LOG.error("Error on initialization of server " + ServiceHandler.class.getName());
            Util.existProcess(13, "Error on initialization");
        }

    }

    private void initProperties(){

    }

    public static Map getNimbusData(Map conf, INimbus iNimbus) throws Exception{
        IScheduler forcedScheduler = iNimbus.getForcedScheduler();

        Map data = new HashMap();
        data.put("conf", conf);
        data.put("nimbus-host-port-info", NimbusInfo.fromConf(conf));
        data.put("inimbus", iNimbus);
        data.put("authorization-handler", Common.mkAuthorizationHandler((String)conf.get(Config.NIMBUS_AUTHORIZER), conf));
        data.put("impersonation-authorization-handler", Common.mkAuthorizationHandler((String)conf.get(Config.NIMBUS_IMPERSONATION_AUTHORIZER), conf))
        data.put("submitted-count", new AtomicInteger(0));
        data.put("storm-cluster-state", )

        return null;
    }


    @Override
    public void submitTopology(String name, String uploadedJarLocation, String jsonConf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, TException {

    }

    @Override
    public void submitTopologyWithOpts(String name, String uploadedJarLocation, String jsonConf, StormTopology topology, SubmitOptions options) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, TException {

    }

    @Override
    public void killTopology(String name) throws NotAliveException, AuthorizationException, TException {

    }

    @Override
    public void killTopologyWithOpts(String name, KillOptions options) throws NotAliveException, AuthorizationException, TException {

    }

    @Override
    public void activate(String name) throws NotAliveException, AuthorizationException, TException {

    }

    @Override
    public void deactivate(String name) throws NotAliveException, AuthorizationException, TException {

    }

    @Override
    public void rebalance(String name, RebalanceOptions options) throws NotAliveException, InvalidTopologyException, AuthorizationException, TException {

    }

    @Override
    public void setLogConfig(String name, LogConfig config) throws TException {

    }

    @Override
    public LogConfig getLogConfig(String name) throws TException {
        return null;
    }

    @Override
    public void debug(String name, String component, boolean enable, double samplingPercentage) throws NotAliveException, AuthorizationException, TException {

    }

    @Override
    public void uploadNewCredentials(String name, Credentials creds) throws NotAliveException, InvalidTopologyException, AuthorizationException, TException {

    }

    @Override
    public String beginFileUpload() throws AuthorizationException, TException {
        return null;
    }

    @Override
    public void uploadChunk(String location, ByteBuffer chunk) throws AuthorizationException, TException {

    }

    @Override
    public void finishFileUpload(String location) throws AuthorizationException, TException {

    }

    @Override
    public String beginFileDownload(String file) throws AuthorizationException, TException {
        return null;
    }

    @Override
    public ByteBuffer downloadChunk(String id) throws AuthorizationException, TException {
        return null;
    }

    @Override
    public String getNimbusConf() throws AuthorizationException, TException {
        return null;
    }

    @Override
    public ClusterSummary getClusterInfo() throws AuthorizationException, TException {
        return null;
    }

    @Override
    public TopologyInfo getTopologyInfo(String id) throws NotAliveException, AuthorizationException, TException {
        return null;
    }

    @Override
    public TopologyInfo getTopologyInfoWithOpts(String id, GetInfoOptions options) throws NotAliveException, AuthorizationException, TException {
        return null;
    }

    @Override
    public TopologyPageInfo getTopologyPageInfo(String id, String window, boolean is_include_sys) throws NotAliveException, AuthorizationException, TException {
        return null;
    }

    @Override
    public ComponentPageInfo getComponentPageInfo(String topology_id, String component_id, String window, boolean is_include_sys) throws NotAliveException, AuthorizationException, TException {
        return null;
    }

    @Override
    public String getTopologyConf(String id) throws NotAliveException, AuthorizationException, TException {
        return null;
    }

    @Override
    public StormTopology getTopology(String id) throws NotAliveException, AuthorizationException, TException {
        return null;
    }

    @Override
    public StormTopology getUserTopology(String id) throws NotAliveException, AuthorizationException, TException {
        return null;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean isWaiting() {
        return false;
    }
}
