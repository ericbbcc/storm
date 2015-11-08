package backtype.storm;


import backtype.storm.utils.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

/**
 * @author float.lu
 */
public class Util {

    private static final Logger LOG = LoggerFactory.getLogger(Util.class);

    public static Counter mkCounter(){
        return mkCounter(1);
    }

    public static Counter mkCounter(int startVal){
        return new Counter(startVal);
    }

    public static boolean isAbsolutePath(String path){
        return Paths.get(path, new String[0]).isAbsolute();
    }

    public static void existProcess(int code, String ...msg){
        //TODO log error
        Runtime.getRuntime().exit(13);
    }

}
