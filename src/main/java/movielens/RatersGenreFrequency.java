package movielens;

import com.google.common.base.Splitter;
import movielens.transformations.FindMostFrequent;
import movielens.transformations.PartialKeyToValue;
import movielens.transformations.LineToPair;
import movielens.transformations.ValueToKey;
import org.apache.crunch.*;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.Join;
import org.apache.crunch.lib.join.JoinType;
import org.apache.crunch.lib.join.MapsideJoinStrategy;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Find the most frequent genre for a rater
 * <p/>
 * Created by dvorcjc on 7/25/2016.
 */
public class RatersGenreFrequency extends Configured implements Tool {
    private static final String SEPARATOR = "::";

    public int run(String[] args) {
        if (args.length != 2) {
            System.err.println("Incorrect usage");
            System.err.println("Usage: RatersGenreFrequency <inputPath> <outputPath>");
            System.err.println("where inputPath is the parent dir for movielens files");
            return 1;
        }
        String inputPath = args[0];
        String outputPath = args[1];

        Pipeline pipeline = new MRPipeline(RatersGenreFrequency.class, getConf());

        // read in ratings -- UserID::MovieID::Rating::Timestamp
        PCollection<String> ratings = pipeline.read(From.textFile(inputPath + "/ratings.dat"));

        // read in movies -- MovieID::Title::Genre|Genre|Genre|...
        PCollection<String> movies = pipeline.read(From.textFile(inputPath + "/movies.dat"));

        // count the number of genre occurrences per rater
        PTable<Pair<String,String>,Long> ratersGenreCount = ratersGenreCount(movies, ratings);

        // find the most frequent genre per rater
        PCollection<Tuple3<String, String, Long>> userGenreCountCol = ratersMostFreqGenre(ratersGenreCount);

        userGenreCountCol.write(To.textFile(outputPath), Target.WriteMode.OVERWRITE);

        PipelineResult result = pipeline.done();
        return result.succeeded() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new RatersGenreFrequency(), args);
    }

    public PTable<Pair<String, String>, Long> ratersGenreCount(PCollection<String> movies, PCollection<String> ratings) {
        // map to MovieID,UserID
        PTable<String, String> ratingsTable = ratings.parallelDo(
                new LineToPair(1, 0, 4, SEPARATOR),
                Avros.tableOf(Avros.strings(), Avros.strings())
        );

        // flatmap to -- MovieID,Genre
        PTable<String, String> moviesTable = movies.parallelDo(
                new FlattenMovieGenres(),
                Avros.tableOf(Avros.strings(), Avros.strings())
        );

        // join on movie ID -- MovieID,Genre::UserID
        // do a mapside join since movieTable is pretty small
        MapsideJoinStrategy<String,String,String> mapsideJoinStrategy = MapsideJoinStrategy.create();
        PTable<String, Pair<String, String>> moviesRatingsJoin = mapsideJoinStrategy.join(moviesTable,ratingsTable, JoinType.INNER_JOIN);
//        PTable<String, Pair<String, String>> moviesRatingsJoin = Join.innerJoin(moviesTable, ratingsTable);

        // map to UserID,Genre
        PTable<Pair<String, String>, Long> userGenreKey = moviesRatingsJoin.parallelDo(
                new ValueToKey<String,Pair<String,String>>(),
                Avros.tableOf(Avros.pairs(Avros.strings(), Avros.strings()), Avros.longs())
        );

        // count
        return userGenreKey.groupByKey().combineValues(
                Aggregators.SUM_LONGS()
        );
    }

    public PCollection<Tuple3<String, String, Long>> ratersMostFreqGenre(PTable<Pair<String,String>,Long> raterGenreCount) {
        // format for group by rater
        PTable<String, Pair<String, Long>> raterKey = raterGenreCount.parallelDo(
                new PartialKeyToValue<String,Long>(1),
                Avros.tableOf(Avros.strings(), Avros.pairs(Avros.strings(), Avros.longs()))
        );

        // secondary sort to find the max genre count for a rater
        return raterKey.groupByKey().parallelDo(
                new FindMostFrequent(),
                Avros.triples(Avros.strings(), Avros.strings(), Avros.longs())
        );
    }

    /****************
     * Custom DoFns *
     ****************/

    /*
     * Flatten movies.dat records into one record per genre
     */
    public static class FlattenMovieGenres extends DoFn<String, Pair<String, String>> {
        private static final char GENRE_SEPARATOR = '|';
        private static final Splitter GENRE_SPLITTER = Splitter.on(GENRE_SEPARATOR).trimResults().omitEmptyStrings();

        @Override
        public void process(String input, Emitter<Pair<String, String>> emitter) {
            String[] lineArr = input.split(SEPARATOR);

            if (lineArr.length == 3) {
                String movieId = lineArr[0];

                for (String genre : GENRE_SPLITTER.split(lineArr[2])) {
                    emitter.emit(Pair.of(movieId, genre));
                }
            }
        }
    }

//    public class FindMax<T extends Comparable> extends DoFn<T, T> {
//
//        private PType<T> ptype;
//        private T maxValue;
//
//        public FindMax(PType<T> ptype) {
//            this.ptype = ptype;
//        }
//
//        public void initialize() {
//            this.ptype.initialize(getConfiguration());
//        }
//
//        public void process(T input, Emitter<T> emitter) {
//            if (maxValue == null || maxValue.compareTo(input) > 0) {
//                // We need to call getDetachedValue here, otherwise the internal
//                // state of maxValue might change with each call to process()
//                // and we won't hold on to the max value
//                maxValue = ptype.getDetachedValue(input);
//            }
//        }
//
//        public void cleanup(Emitter<T> emitter) {
//            if (maxValue != null) {
//                emitter.emit(maxValue);
//            }
//        }
//    }
}
