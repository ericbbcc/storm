package backtype.storm.utils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 计数器
 * @author float.lu
 */
public class Counter {

    private AtomicInteger value;

    public Counter(int value) {
        this.value = new AtomicInteger(value - 1);
    }

    public int getValue(){
        return value.incrementAndGet();
    }
}
