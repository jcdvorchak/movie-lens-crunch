package movielens;

import movielens.transformations.LineToPair;
import org.apache.crunch.*;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.Join;
import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Iterator;

/**
 * Find the most frequent tag for a movie
 *
 * Created by dvorcjc on 7/21/2016.
 */
public class MovieTagCount extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Incorrect arguments.");
            System.err.println("Usage: MovieTagCount <inputPath> <outputPath>");
            System.err.println("(where inputPath is the parent directory of movielens *.dat files)");
            return 1;
        }
        String inputPath = args[0];
        String outputPath = args[1];

        Pipeline pipeline = new MRPipeline(MovieTagCount.class, getConf());

        // read in movies
        PCollection<String> movies = pipeline.read(From.textFile(inputPath + "/movies.dat"));
        // to kv pair
        PTable<String, String> movieTable = movies.parallelDo(
                new LineToPair(0, 1, 3, "::"),
                Writables.tableOf(Writables.strings(), Writables.strings())
        );

        // read in tags
        PCollection<String> tags = pipeline.read(From.textFile(inputPath + "/tags.dat"));
        // to kv pair
        PTable<String, String> tagTable = tags.parallelDo(
                new LineToPair(1, 2, 4, "::"),
                Writables.tableOf(Writables.strings(), Writables.strings())
        );

        // join on movieid
        PTable<String, Pair<String, String>> movieTagJoin = Join.innerJoin(movieTable, tagTable);

        // reformat for a composite key of movie+tag
        PTable<Pair<String, String>, Long> movieTagKey = movieTagJoin.parallelDo(
                new MovieTagCountPrep(),
                Writables.tableOf(Writables.pairs(Writables.strings(), Writables.strings()), Writables.longs())
        );

        // aggregate on the value (1) for each movie+tag pair
        PTable<Pair<String, String>, Long> movieTagCount = movieTagKey.groupByKey()
                .combineValues(Aggregators.SUM_LONGS()
        );

        // format for secondary sort
        PTable<String, Pair<String,Long>> userGenreCountSecondary = movieTagCount.parallelDo(
                new MovieTagCountPrepSec(),
                Writables.tableOf(Writables.strings(),Writables.pairs(Writables.strings(),Writables.longs()))
        );

        // secondary sort to find the max genre count for a rater
        PCollection<Tuple3<String,String,Long>> userGenreCountCol = SecondarySort.sortAndApply(userGenreCountSecondary,
                new MovieTagCountMax(),
                Writables.triples(Writables.strings(),Writables.strings(),Writables.longs())
        );

        userGenreCountCol.write(To.textFile(outputPath), Target.WriteMode.OVERWRITE);

        PipelineResult result = pipeline.done();
        return result.succeeded() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new MovieTagCount(), args);
    }


    // map to a composite key of movie name and tag with a value of 1 to prepare for the count
    public static class MovieTagCountPrep extends MapFn<Pair<String, Pair<String, String>>, Pair<Pair<String, String>, Long>> {

        @Override
        public Pair<Pair<String, String>, Long> map(Pair<String, Pair<String, String>> input) {
            return Pair.of(Pair.of(input.second().first(), input.second().second()), 1L);
        }

    }

    public static class MovieTagCountPrepSec extends MapFn<Pair<Pair<String,String>,Long>, Pair<String,Pair<String,Long>>> {

        @Override
        public Pair<String,Pair<String,Long>> map(Pair<Pair<String,String>,Long> input) {
            return Pair.of(input.first().first(),Pair.of(input.first().second(),input.second()));
        }
    }

    public static class MovieTagCountMax extends MapFn<Pair<String,Iterable<Pair<String,Long>>>, Tuple3<String,String,Long>> {

        @Override
        public Tuple3<String, String, Long> map(Pair<String, Iterable<Pair<String, Long>>> input) {
            Iterator<Pair<String,Long>> it = input.second().iterator();
            Pair<String,Long> maxPair = it.next();
            Pair<String,Long> currPair = null;

            while (it.hasNext()) {
                currPair = it.next();
                if (currPair.second()>maxPair.second()) {
                    maxPair = currPair;
                }
            }

            return Tuple3.of(input.first(), maxPair.first(), maxPair.second());
        }
    }

}
