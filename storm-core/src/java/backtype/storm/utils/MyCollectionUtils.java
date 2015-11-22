package backtype.storm.utils;

import java.util.*;

/**
 * @author float.lu
 */
final public class MyCollectionUtils {

    public static boolean isEmpty(Collection<?> collection) {
        if (collection == null || collection.size() == 0) {
            return true;
        }
        return false;
    }

    public static boolean isNotEmpty(Collection<?> collection) {
        return !isEmpty(collection);
    }

    public static <E> List<E> subList(List<E> list, int start, int step) {
        if(start < 0) {
            throw new IllegalArgumentException();
        }
        if (isEmpty(list)){
            if (start != 0 || step != 0) {
                throw new RuntimeException("Collection must not be null, while start=[" + start +
                        "] and end=[" + step +
                        "]");
            }
            return list;
        } else {
            if (start + step > list.size() - 1) {
                IllegalArgumentException e = new IllegalArgumentException();
                throw new RuntimeException("Collection size=[" + list.size() +
                        "], start=[" + start +
                        "], step=[" + step +
                        "]", e);
            }
            try {
                List subList = list.getClass().newInstance();
                Iterator iterator = list.iterator();
                int i = 0;
                while (iterator.hasNext()){
                    if(i >= start + step) {
                        break;
                    }
                    if(i >= start) {
                        subList.add(iterator.next());
                    }
                    i++;
                }
                return list;
            } catch (IllegalAccessException | InstantiationException e) {
                throw new RuntimeException(e);
            }
        }
    }
    public static <E> List<E> subList(List<E> collection, int start) {
        if (isEmpty(collection)) {
            throw new IllegalArgumentException("collection must not be empty");
        }
        return subList(collection, start, collection.size());
    }

    public static <E, K> Map<K, List<E>> reverseMap(Map<E, K> mapper) {
        Map<K, List<E>> map = new HashMap<>();
        for (E e : mapper.keySet()) {
            K k = mapper.get(e);
            if (map.get(k) == null) {
               map.put(k, new ArrayList<E>());
            }
            map.get(k).add(e);
        }
        return map;
    }
}
