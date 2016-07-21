package demo;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import org.apache.crunch.*;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Set;

/**
 * WordCount example to familiarize myself with Crunch
 *
 * Created by jcdvorchak on 7/20/2016.
 */
public class WordCount extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Only two arguments are accepted.");
            System.err.println("Usage: WordCount <inputPath> <outputPath>");
            return 1;
        }
        String inputPath = args[0];
        String outputPath = args[1];

        Pipeline pipeline = new MRPipeline(WordCount.class, getConf());

        PCollection<String> lines = pipeline.readTextFile(inputPath);

        PCollection<String> words = lines.parallelDo(new Tokenizer(), Writables.strings());

        PCollection<String> noStopWords = words.filter(new StopWordFilter());

        PTable<String, Long> counts = noStopWords.count();

        pipeline.writeTextFile(counts, outputPath);

        PipelineResult result = pipeline.done();
        return result.succeeded() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new WordCount(), args);
    }

    private class Tokenizer extends DoFn<String, String> {
        private final Splitter SPLITTER = Splitter.onPattern("\\s+").omitEmptyStrings();

        @Override
        public void process(String line, Emitter<String> emitter) {
            for (String word : SPLITTER.split(line)) {
                emitter.emit(word);
            }
        }
    }

    private class StopWordFilter extends FilterFn<String> {
        // English stop words, borrowed from Lucene.
        private final Set<String> STOP_WORDS = ImmutableSet.copyOf(new String[] {
                "a", "and", "are", "as", "at", "be", "but", "by",
                "for", "if", "in", "into", "is", "it",
                "no", "not", "of", "on", "or", "s", "such",
                "t", "that", "the", "their", "then", "there", "these",
                "they", "this", "to", "was", "will", "with"
        });

        @Override
        public boolean accept(String word) {
            return !STOP_WORDS.contains(word);
        }
    }
}
