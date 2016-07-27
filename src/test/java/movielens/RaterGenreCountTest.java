package movielens;

import com.google.common.collect.ImmutableList;
import movielens.transformations.LineToPair;
import org.apache.crunch.*;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.lib.Join;
import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.types.writable.Writables;
import org.junit.Test;

public class RaterGenreCountTest {
    private PCollection<String> movies = MemPipeline.typedCollectionOf(
            Writables.strings(),
            ImmutableList.of(
                    "1::Toy Story (1995)::Adventure|Animation|Children|Comedy|Fantasy",
                    "2::Jumanji (1995)::Adventure|Children|Fantasy",
                    "3::Grumpier Old Men (1995)::Comedy|Romance",
                    "4::Waiting to Exhale (1995)::Comedy|Drama|Romance",
                    "5::Father of the Bride Part II (1995)::Comedy",
                    "6::Heat (1995)::Action|Crime|Thriller",
                    "7::Sabrina (1995)::Comedy|Romance",
                    "8::Tom and Huck (1995)::Adventure|Children",
                    "9::Sudden Death (1995)::Action",
                    "10::GoldenEye (1995)::Action|Adventure|Thriller"
            ));

    private PCollection<String> ratings = MemPipeline.typedCollectionOf(
            Writables.strings(),
            ImmutableList.of(
                    "1::1::5::838985046",
                    "1::2::5::838983525",
                    "1::3::5::838983392",
                    "1::3::5::838983421",
                    "1::3::5::838983392",
                    "2::4::5::838983392",
                    "2::5::5::838984474",
                    "2::5::5::838983653",
                    "2::6::5::838984885",
                    "2::6::5::838983707",
                    "2::6::5::838984596",
                    "2::6::5::838983834",
                    "3::8::5::838983834",
                    "3::10::5::838984679",
                    "3::9::5::838983653",
                    "3::9::5::838984679"
            ));


    @Test
    public void testRun() {

        PTable<String,String> ratingsTable = ratings.parallelDo(
                new LineToPair(1,0,4,"::"),
                Writables.tableOf(Writables.strings(),Writables.strings())
        );

        PTable<String,String> moviesTable = movies.parallelDo(
                new RaterGenreCount.FlattenMovieGenres(),
                Writables.tableOf(Writables.strings(),Writables.strings())
        );

        // join on movie ID -- MovieID,Genre::UserID
        PTable<String,Pair<String,String>> moviesRatingsJoin = Join.innerJoin(ratingsTable,moviesTable);

        // map to UserID,Genre
        PTable<Pair<String,String>,Long> userGenreKey = moviesRatingsJoin.parallelDo(
                new RaterGenreCount.UserGenreCountPrep(),
                Writables.tableOf(Writables.pairs(Writables.strings(),Writables.strings()),Writables.longs())
        );
        // count
        PTable<Pair<String,String>,Long> userGenreCount = userGenreKey.groupByKey().combineValues(
                Aggregators.SUM_LONGS()
        );

        PTable<String, Pair<String,Long>> userGenreCountSecondary = userGenreCount.parallelDo(
                new RaterGenreCount.UserGenreCountPrepSec(),
                Writables.tableOf(Writables.strings(),Writables.pairs(Writables.strings(),Writables.longs()))
        );

        PCollection<Tuple3<String,String,Long>> userGenreCountCol = SecondarySort.sortAndApply(userGenreCountSecondary,
                new RaterGenreCount.UserGenreCountMax(),
                Writables.triples(Writables.strings(),Writables.strings(),Writables.longs())
        );

        Iterable<Tuple3<String,String,Long>> it = userGenreCountCol.materialize();
        for (Tuple3<String,String,Long> record : it) {
            System.out.println(record.toString());
        }
    }
}
