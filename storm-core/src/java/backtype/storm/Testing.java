package backtype.storm;

import backtype.storm.entity.InProcessZKInfo;
import backtype.storm.scheduler.INimbus;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.*;

/**
 * @author float.lu
 */
public class Testing {

    private static final Logger LOG = LoggerFactory.getLogger(Testing.class);

    public static String OS_NAME = "os.name";
    public static String OS_WIN = "win";
    public static String OS_TMPDIR = "java.io.tmpdir";

    public static String STORM_ZOOKEEPER_SERVERS = "STORM_ZOOKEEPER_SERVERS";
    public static int TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS = 3;

    public static int DEFAULT_SUPERVISOR = 2;
    public static int DEFAULT_PORTSPRESUPERVISOR = 3;
    public static int DEFAULT_SUPERVISORSLORTPORTMIN = 1024;

    public Properties mkLocalStormCluster(int supervisor, int portsPreSupervisor,
                                          Map deamenConf, INimbus iNimbus,
                                          int supervisorSlotPortMin)throws IOException, InterruptedException {
        int _supervisor            =  supervisor == 0 ? DEFAULT_SUPERVISOR : supervisor;
        int _portsPreSupervisor    =  portsPreSupervisor == 0 ? DEFAULT_PORTSPRESUPERVISOR : portsPreSupervisor;
        int _supervisorSlotPortMin =  supervisorSlotPortMin == 0 ? DEFAULT_SUPERVISORSLORTPORTMIN : supervisorSlotPortMin;
        String zkTmp               =  localTempPath();
        InProcessZKInfo zkInfo = null;

        if(deamenConf == null || !deamenConf.containsKey(STORM_ZOOKEEPER_SERVERS)){
            zkInfo = ZooKeeper.makeInprocessZooKeeper(zkTmp, 0);
        }
        Map stormConfig = ConfigUtils.readStormConfig();




    }

    public static String localTempPath(){
        String os = System.getProperty(OS_NAME);
        if(StringUtils.startsWithIgnoreCase(os, OS_WIN)){
            return System.getProperty(OS_TMPDIR) + "/" + UUID.randomUUID();
        }else{
            return System.getProperty(OS_TMPDIR) + UUID.randomUUID();
        }
    }
}
