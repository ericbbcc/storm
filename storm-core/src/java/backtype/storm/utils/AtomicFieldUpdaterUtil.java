package backtype.storm.utils;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * @author float.lu
 */
final public class AtomicFieldUpdaterUtil {
    private static final boolean AVAILABLE;

    public static <T, V> AtomicReferenceFieldUpdater<T, V> newRefUpdater(Class<T> holdingClazz, Class<V> fieldClazz, String fieldName) {
        return AVAILABLE ? AtomicReferenceFieldUpdater.newUpdater(holdingClazz, fieldClazz, fieldName):null;
    }

    public static <T> AtomicIntegerFieldUpdater<T> newIntUpdater(Class<T> holdingClazz, String fieldName) {
        return AVAILABLE ? AtomicIntegerFieldUpdater.newUpdater(holdingClazz, fieldName) : null;
    }

    static boolean isAvailable() {
        return AVAILABLE;
    }

    static {
        boolean available = false;

        try {
            AtomicReferenceFieldUpdater testUpdater = AtomicReferenceFieldUpdater.newUpdater(AtomicFieldUpdaterUtil.Node.class, AtomicFieldUpdaterUtil.Node.class, "next");
            AtomicFieldUpdaterUtil.Node testNode = new AtomicFieldUpdaterUtil.Node();
            testUpdater.set(testNode, testNode);
            if(testNode.next != testNode) {
                throw new Exception();
            }
            available = true;
        } catch (Throwable throwable) {

        }

        AVAILABLE = available;
    }

    static final class Node {
        volatile AtomicFieldUpdaterUtil.Node next;
    }
}
