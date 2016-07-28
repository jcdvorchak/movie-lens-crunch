package movielens.transformations;

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;

/**
 * Change the value to the key
 * The new value will be a long of 1
 *
 * Created by dvorcjc on 7/27/2016.
 */
public class ValueToKey<K,V> extends MapFn<Pair<K,V>, Pair<V, Long>> {

    @Override
    public Pair<V, Long> map(Pair<K, V> input) {
        return Pair.of(input.second(), 1L);
    }
}