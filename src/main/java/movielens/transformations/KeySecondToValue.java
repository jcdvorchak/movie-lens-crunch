package movielens.transformations;

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;

/**
 * Move the second of the key pair to be the first of the new value pair
 *
 * Created by dvorcjc on 7/27/2016.
 */
public class KeySecondToValue<K1,K2,V> extends MapFn<Pair<Pair<K1, K2>, V>, Pair<K1, Pair<K2, V>>> {

    @Override
    public Pair<K1, Pair<K2, V>> map(Pair<Pair<K1, K2>, V> input) {
        return Pair.of(input.first().first(), Pair.of(input.first().second(), input.second()));
    }
}