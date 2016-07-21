package movielens;

import org.apache.crunch.*;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Iterator;

/**
 * Created by dvorcjc on 7/21/2016.
 */
public class MovieTagCount extends Configured implements Tool {

    public int run(String[] args) {
        if (args.length!=2) {
            System.err.println("Incorrect arguments.");
            System.err.println("Usage: MovieTagCount <inputPath> <outputPath>");
            return 1;
        }
        String inputPath = args[0];
        String outputPath = args[1];

        Pipeline pipeline = new MRPipeline(MovieTagCount.class, getConf());

        // read in movies
        PCollection<String> movies = pipeline.read(From.textFile(inputPath+"/movies.dat"));
        // to kv pair
        // todo doesn't need to be a string,string
        PTable<String,String> movieTable = movies.parallelDo(
                new LineToTable(0,1,"::"),
                Writables.tableOf(Writables.strings(),Writables.strings())
        );

        // read in tags
        PCollection<String> tags = pipeline.read(From.textFile(inputPath+"/tags.dat"));
        // to kv pair
        PTable<String,String> tagTable = tags.parallelDo(
                new LineToTable(1,2,"::"),
                Writables.tableOf(Writables.strings(),Writables.strings())
        );

        // join on movieid
        PTable<String, Pair<String,String>> movieTags = movieTable.join(tagTable);

//        movieTags = movieTags.parallelDo(
//                new MapFn<Pair<String, Pair<String,String>>, Pair<String, Pair<String,Long>>>() {
//                    Pair<String, Pair<String,Long>> map(Pair<String, Pair<String,String>> in) {
//                        return Pair.of(in.second().first(),Pair.of(in.second().second(),1L));
//                    }
//                },
//                Writables.tableOf(Writables.strings(),Writables.pairs(Writables.strings(),Writables.longs()))
//        );
//
//        PTable<String, Pair<String,Long>> movieTagCount = movieTags.groupByKey().combineValues(
//                new CombineFn< Pair<String,Iterable<Pair<String,Long>>>, Pair<String,Pair<String,Long>> >() {
//                    void process(Pair<String,Iterable<Pair<String,Long>>> in, Emitter<Pair<String,Pair<String,Long>>> emmitter) {
//                        long size = 0;
//                        for (Object o : in.second()) {
//                            size++;
//                        }
//
//                        emmitter.emit(Pair.of(in.first(),Pair.of(in.second().iterator().next().first(),size)));
//                    }
//                }
//        );

//        movieTagCount.write(To.textFile(outputPath), Target.WriteMode.OVERWRITE);

        PipelineResult result = pipeline.done();
        return result.succeeded() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new MovieTagCount(), args);
    }

    // this is only going to return one per thing -- MapFn
    private class MovieToTable extends DoFn<String, Pair<Integer, String>> {

        @Override
        public void process(String line, Emitter<Pair<Integer,String>> emitter) {
            String[] lineArr = line.split("::");
            emitter.emit(new Pair<Integer, String>(Integer.parseInt(lineArr[0]), lineArr[1]));
        }
    }
}
