package movielens.transformations;

import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;

/**
 * Move the second of the key pair to be the first of the new value pair
 *
 * Created by dvorcjc on 7/27/2016.
 */
public class PartialKeyToValue<K,V> extends MapFn<Pair<Pair<K, K>, V>, Pair<K, Pair<K, V>>> {
    private int keyToMove;

    public PartialKeyToValue(int keyToMove) {
        this.keyToMove = keyToMove;
    }
    
    @Override
    public Pair<K, Pair<K, V>> map(Pair<Pair<K, K>, V> input) {
        if (keyToMove == 1) {
            return Pair.of(input.first().second(), Pair.of(input.first().first(), input.second()));
        } else if (keyToMove == 2) {
            return Pair.of(input.first().first(), Pair.of(input.first().second(), input.second()));
        } else {
            return null;
        }
    }
}