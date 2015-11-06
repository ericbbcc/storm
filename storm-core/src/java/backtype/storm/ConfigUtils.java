package backtype.storm;

import backtype.storm.utils.Utils;

import java.util.*;

/**
 * @author float.lu
 */
public class ConfigUtils {

    public static Map readStormConfig(){
        return Utils.readStormConfig();
    }
}
