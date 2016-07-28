package movielens.transformations;

import org.apache.crunch.Pair;
import org.apache.crunch.MapFn;
import org.apache.crunch.Tuple3;

import java.util.Iterator;

/**
 * Find the most frequent count (the second field of the value)
 * If there are two or more that have the a
 *
 * Created by dvorcjc on 7/27/2016.
 */
public class FindMostFrequent extends MapFn<Pair<String, Iterable<Pair<String,Long>>>, Tuple3<String,String,Long>> {

    @Override
    public Tuple3<String, String, Long> map(Pair<String, Iterable<Pair<String, Long>>> input) {
        Iterator<Pair<String, Long>> it = input.second().iterator();
        Pair<String, Long> maxPair = it.next();
        Pair<String, Long> currPair;

        while (it.hasNext()) {
            currPair = it.next();
            if (currPair.second() > maxPair.second()) {
                maxPair = currPair;
            } else if (currPair.second().equals(maxPair.second())) {
                // if they are the same add the strings together
                maxPair = Pair.of(maxPair.first()+"|"+currPair.first(),maxPair.second());
            }
        }

        return Tuple3.of(input.first(), maxPair.first(), maxPair.second());
    }
}
