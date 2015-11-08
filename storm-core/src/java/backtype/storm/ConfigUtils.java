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

    public static String getMasterInimbusDir(Properties conf)throws IOException{
        StringBuilder sb = new StringBuilder(getMasterLocalDir(conf));
        sb.append(getFilePathSeparator());
        sb.append("inimbus");
        return sb.toString();
    }

    public static String getMasterLocalDir(Properties conf)throws IOException{
        StringBuilder sb = new StringBuilder(getAbsoluteStormLocalDir(conf));
        sb.append(getFilePathSeparator());
        sb.append("nimbus");
        FileUtils.forceMkdir(new File(sb.toString()));
        return sb.toString();
    }

    public static String getAbsoluteStormLocalDir(Properties conf){
        String stormHome = System.getProperty("storm.home");
        String path = conf.getProperty(Config.STORM_LOCAL_DIR);
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

    public static String getFilePathSeparator(){
        return System.getProperty("file.separator");
    }
}
