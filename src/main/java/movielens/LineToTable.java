package movielens;

import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.Pair;
import org.eclipse.jetty.io.UncheckedIOException;

/**
 * Created by dvorcjc on 7/21/2016.
 */
public class LineToTable extends MapFn<String, Pair<String, String>> {
    private int keyIndex, valueIndex;
    private String separator;

    public LineToTable(int keyIndex, int valueIndex, String separator) {
        this.keyIndex = keyIndex;
        this.valueIndex = valueIndex;
        this.separator = separator;
    }

    public Pair<String,String> map(String line) {
        String[] lineArr = line.split(this.separator);

        return new Pair<String,String>(lineArr[keyIndex], lineArr[valueIndex]);
    }
}
