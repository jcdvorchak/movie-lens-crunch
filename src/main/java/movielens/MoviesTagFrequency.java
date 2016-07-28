package movielens;

import movielens.transformations.FindMostFrequent;
import movielens.transformations.PartialKeyToValue;
import movielens.transformations.LineToPair;
import movielens.transformations.ValueToKey;
import org.apache.commons.lang.ArrayUtils;
import org.apache.crunch.*;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.join.JoinType;
import org.apache.crunch.lib.join.MapsideJoinStrategy;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Find the most frequent tag for a movie
 * <p/>
 * Created by dvorcjc on 7/21/2016.
 */
public class MoviesTagFrequency extends Configured implements Tool {
    protected static String SEPARATOR = ",";

    public int run(String[] args) throws Exception {
        if (args.length !=2 ) {
            System.err.println("Incorrect arguments " + ArrayUtils.toString(args));
            System.err.println("Usage: MoviesTagFrequency <inputPath> <outputPath>");
            System.err.println("where inputPath is the parent dir for movielens files");
            return 1;
        }
        String inputPath = args[0];
        String outputPath = args[1];

        Pipeline pipeline = new MRPipeline(MoviesTagFrequency.class, getConf());

        PCollection<String> movies = pipeline.read(From.textFile(inputPath + "/movies*"));

        PCollection<String> tags = pipeline.read(From.textFile(inputPath + "/tags*"));

        // count the number of tag occurrences per movie
        PTable<Pair<String, String>, Long> moviesTagCount = moviesTagCount(movies, tags);

        // find the most frequent tag per movie
        PCollection<Tuple3<String, String, Long>> userGenreCountCol = moviesMostFreqTag(moviesTagCount);

        userGenreCountCol.write(To.textFile(outputPath), Target.WriteMode.OVERWRITE);

        PipelineResult result = pipeline.done();
        return result.succeeded() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new MoviesTagFrequency(), args);
    }

    public PTable<Pair<String, String>, Long> moviesTagCount(PCollection<String> movies, PCollection<String> tags) {
        // to kv pair -- MovieId,MovieName
        PTable<String, String> movieTable = movies.parallelDo(
                new LineToPair(0, 1, 3, SEPARATOR),
                Avros.tableOf(Avros.strings(), Avros.strings())
        );

        // to kv pair -- MovieId,Tag
        PTable<String, String> tagTable = tags.parallelDo(
                new LineToPair(1, 2, 4, SEPARATOR),
                Avros.tableOf(Avros.strings(), Avros.strings())
        );

        // join on MovieId
        // do a mapside join since movieTable is pretty small
        MapsideJoinStrategy<String,String,String> mapsideJoinStrategy = MapsideJoinStrategy.create();
        PTable<String, Pair<String, String>> movieTagJoin = mapsideJoinStrategy.join(movieTable,tagTable, JoinType.INNER_JOIN);
        //Join.innerJoin(movieTable, tagTable);

        // reformat for a key of (movie,tag)
        PTable<Pair<String, String>, Long> movieTagKey = movieTagJoin.parallelDo(
                new ValueToKey<String,Pair<String,String>>(),
                Avros.tableOf(Avros.pairs(Avros.strings(), Avros.strings()), Avros.longs())
        );

        // sum the value (1) for each key (movie,tag)
        return movieTagKey.groupByKey()
                .combineValues(Aggregators.SUM_LONGS()
                );
    }

    public PCollection<Tuple3<String, String, Long>> moviesMostFreqTag(PTable<Pair<String, String>, Long> moviesTagCount) {
        // format for a group by movie
        PTable<String, Pair<String, Long>> movieKey = moviesTagCount.parallelDo(
                new PartialKeyToValue<String,Long>(2),
                Avros.tableOf(Avros.strings(), Avros.pairs(Avros.strings(), Avros.longs()))
        );

        // find the max genre count for a rater
        return movieKey.groupByKey().parallelDo(
                new FindMostFrequent(),
                Avros.triples(Avros.strings(), Avros.strings(), Avros.longs())
        );
    }
}
