package demo;

import com.google.common.base.Splitter;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

/**
 * Created by dvorcjc on 7/21/2016.
 */
public class Tokenizer extends DoFn<String, String> {
//    private final Splitter SPLITTER = Splitter.onPattern("\\s+").omitEmptyStrings();

    @Override
    public void process(String line, Emitter<String> emitter) {
        for (String word : line.split(" ")) {//SPLITTER.split(line)) {
            emitter.emit(word);
        }
    }
}
