package demo;

import org.apache.crunch.*;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * WordCount example to familiarize myself with Crunch
 * <p/>
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

//        JavaSparkContext sc = new JavaSparkContext(new SparkConf());
//        Pipeline pipeline = new SparkPipeline(sc, "WordCount");

        Pipeline pipeline = new MRPipeline(WordCount.class, getConf());

        PCollection<String> lines = pipeline.readTextFile(inputPath);

        PCollection<String> words = lines.parallelDo(new Tokenizer(), Writables.strings());

        PCollection<String> noStopWords = words.filter(new StopWordFilter());

        PTable<String, Long> counts = noStopWords.count();

        //pipeline.writeTextFile(counts, outputPath);
        counts.write(At.textFile(outputPath), Target.WriteMode.OVERWRITE);
        PipelineResult result = pipeline.done();
        return result.succeeded() ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new WordCount(), args);
    }
}
