package backtype.storm;


import backtype.storm.utils.Counter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

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

    public static String normalizePath(String path){
            return toksToPath(tokenizePath(path));
    }

    public static String toksToPath(List<String> toks){
        return "/" + StringUtils.join(toks, "/");
    }

    public static List<String> tokenizePath(String path){
        List<String> ret = new ArrayList<>();
        String[] toks = path.split("/");
        for(String tok : toks){
            if(tok == null || tok.trim() == ""){
                continue;
            }
            ret.add(tok);
        }
        return ret;
    }

    public static String getParentPath(String path){
        List<String> toks = tokenizePath(path);
        toks.remove(toks.size() - 1);
        return "/" + StringUtils.join(toks, "/");
    }

    public static byte[] barr(int ...val){
        ByteBuffer byteBuffer = ByteBuffer.allocate(val.length);
        for(int v : val){
            byteBuffer.putInt(v);
        }
        return byteBuffer.array();
    }

}
