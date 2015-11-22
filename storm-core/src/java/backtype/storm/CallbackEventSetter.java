package backtype.storm;

import org.apache.zookeeper.WatchedEvent;

/**
 * @author float.lu
 */
public interface CallbackEventSetter {
    Callback setEvent(WatchedEvent event);
}
