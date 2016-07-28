package movielens.transformations;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

/**
 * Convert a String to a Pair of Strings
 * Splits based on separator, verifies length based on length,
 *   gets key based on keyIndex, gets value based on valueIndex
 *
 * Created by dvorcjc on 7/21/2016.
 */
public class LineToPair extends DoFn<String, Pair<String, String>> {
    private int keyIndex, valueIndex, length;
    private String separator;

    public LineToPair(int keyIndex, int valueIndex, int length, String separator) {
        this.keyIndex = keyIndex;
        this.valueIndex = valueIndex;
        this.length = length;
        this.separator = separator;
    }

    public void process(String line, Emitter<Pair<String, String>> emitter) {
        String[] lineArr = line.split(this.separator);

        if (lineArr.length==length) {
            emitter.emit( Pair.of(lineArr[keyIndex], lineArr[valueIndex]));
        }
    }
}
