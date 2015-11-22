package backtype.storm;


/**
 * @author float.lu
 */
public interface Callback extends CallbackEventSetter {
    void call();
}
