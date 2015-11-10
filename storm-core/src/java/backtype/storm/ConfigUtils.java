package backtype.storm;

import backtype.storm.utils.Utils;
import backtype.storm.validation.ConfigValidation;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * @author float.lu
 */
public class ConfigUtils {

    public static Map readStormConfig()  throws IllegalAccessException, InstantiationException, NoSuchFieldException, NoSuchMethodException, InvocationTargetException {
        Map conf = Utils.readStormConfig();
        ConfigValidation.validateFields(conf);
        return conf;
    }

    public static String getMasterInimbusDir(Map conf){
        StringBuilder sb = new StringBuilder(getMasterLocalDir(conf));
        sb.append(getFilePathSeparator());
        sb.append("inimbus");
        return sb.toString();
    }

    public static String getMasterLocalDir(Map conf){
        StringBuilder sb = new StringBuilder(getAbsoluteStormLocalDir(conf));
        sb.append(getFilePathSeparator());
        sb.append("nimbus");
        try {
            FileUtils.forceMkdir(new File(sb.toString()));
        }catch (IOException e){
            throw new RuntimeException(e);
        }
        return sb.toString();
    }

    public static String getAbsoluteStormLocalDir(Map conf){
        String stormHome = System.getProperty("storm.home");
        String path = (String)conf.get(Config.STORM_LOCAL_DIR);
        if(path != null && path.trim() != ""){
            if(Util.isAbsolutePath(path)){
                return path;
            }else{
                StringBuilder sb = new StringBuilder(stormHome);
                sb.append(getFilePathSeparator());
                sb.append(path);
                return sb.toString();
            }
        }else {
            StringBuilder sb = new StringBuilder(stormHome);
            sb.append(getFilePathSeparator());
            sb.append("storm-local");
            return sb.toString();
        }
    }

    public static String getMasterStormDistRoot(Map conf){
        return getMasterLocalDir(conf) + getFilePathSeparator() + "stormdist";
    }

    public static String getMasterStormDistRoot(Properties conf, String stormId){
        return getMasterLocalDir(conf) + getFilePathSeparator() + stormId;
    }

    public static String getFilePathSeparator(){
        return System.getProperty("file.separator");
    }
}
