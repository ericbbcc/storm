package backtype.storm.daemon;

import backtype.storm.security.auth.IAuthorizer;

import java.util.*;

/**
 * @author float.lu
 */
public class Common {


    public static IAuthorizer mkAuthorizationHandler(String klassName, Map conf)throws ClassNotFoundException,
            IllegalAccessException, InstantiationException{
        Class clazz = Class.forName(klassName);
        if(clazz != null){
            IAuthorizer aznHandler = (IAuthorizer)clazz.newInstance();
            if(aznHandler != null){
                aznHandler.prepare(conf);
                return aznHandler;
            }
        }
        return null;
    }
}