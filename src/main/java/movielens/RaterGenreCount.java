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
 * Find the most frequent genre for a rater
 *
 * Created by dvorcjc on 7/25/2016.
 */
public class RaterGenreCount extends Configured implements Tool {

    public int run(String[] args) {
        if (args.length != 2) {
            System.err.println("Incorrect usage");
            System.err.println("Usage: <inputPath> <outputPath>");
            System.err.println("where inputPath is the parent dir for *.dat files");
            return 1;
        }
        String inputPath = args[0];
        String outputPath = args[1];

        Pipeline pipeline = new MRPipeline(RaterGenreCount.class, getConf());

        // read in ratings -- UserID::MovieID::Rating::Timestamp
        PCollection<String> ratings = pipeline.read(From.textFile(inputPath+"/ratings.dat"));
        // map to MovieID,UserID
        PTable<String,String> ratingsTable = ratings.parallelDo(
                new LineToPair(1,0,4,"::"),
                Writables.tableOf(Writables.strings(),Writables.strings())
        );

        // read in movies -- MovieID::Title::Genre|Genre|Genre|...
        PCollection<String> movies = pipeline.read(From.textFile(inputPath+"/movies.dat"));
        // flatmap to -- MovieID,Genre
        PTable<String,String> moviesTable = movies.parallelDo(
                new FlattenMovieGenres(),
                Writables.tableOf(Writables.strings(),Writables.strings())
        );

        // join on movie ID -- MovieID,Genre::UserID
        PTable<String,Pair<String,String>> moviesRatingsJoin = Join.innerJoin(ratingsTable,moviesTable);
        // map to UserID,Genre
        PTable<Pair<String,String>,Long> userGenreKey = moviesRatingsJoin.parallelDo(
                new UserGenreCountPrep(),
                Writables.tableOf(Writables.pairs(Writables.strings(),Writables.strings()),Writables.longs())
        );

        // count
        PTable<Pair<String,String>,Long> userGenreCount = userGenreKey.groupByKey().combineValues(
                Aggregators.SUM_LONGS()
        );

        // format for secondary sort
        PTable<String, Pair<String,Long>> userGenreCountSecondary = userGenreCount.parallelDo(
                new UserGenreCountPrepSec(),
                Writables.tableOf(Writables.strings(),Writables.pairs(Writables.strings(),Writables.longs()))
        );

        // secondary sort to find the max genre count for a rater
        PCollection<Tuple3<String,String,Long>> userGenreCountCol = SecondarySort.sortAndApply(userGenreCountSecondary,
                new UserGenreCountMax(),
                Writables.triples(Writables.strings(),Writables.strings(),Writables.longs())
        );



        userGenreCountCol.write(To.textFile(outputPath), Target.WriteMode.OVERWRITE);

        PipelineResult result = pipeline.done();
        return result.succeeded() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new RaterGenreCount(), args);
    }

    // flatten movings.dat to one record per genre
    public static class FlattenMovieGenres extends DoFn<String, Pair<String,String>> {

        @Override
        public void process(String input, Emitter<Pair<String,String>> emitter) {
            String[] lineArr = input.split("::");

            if (lineArr.length==3) {
                String movieId = lineArr[0];
                String[] genreArr = lineArr[2].split("\\|");

                for (String genre : genreArr) {
                    emitter.emit(Pair.of(movieId, genre));
                }
            }
        }
    }

    public static class UserGenreCountPrep extends MapFn<Pair<String,Pair<String,String>>,Pair<Pair<String,String>,Long>> {

        @Override
        public Pair<Pair<String,String>,Long> map(Pair<String,Pair<String,String>> input) {
            return Pair.of(Pair.of(input.second().first(),input.second().second()),1L);
        }
    }

    public static class UserGenreCountPrepSec extends MapFn<Pair<Pair<String,String>,Long>, Pair<String,Pair<String,Long>>> {

        @Override
        public Pair<String,Pair<String,Long>> map(Pair<Pair<String,String>,Long> input) {
            return Pair.of(input.first().first(),Pair.of(input.first().second(),input.second()));
        }
    }

    public static class UserGenreCountMax extends MapFn<Pair<String,Iterable<Pair<String,Long>>>, Tuple3<String,String,Long>> {

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
