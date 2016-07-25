package movielens;

import com.google.common.collect.ImmutableList;
import movielens.transformations.LineToTable;
import org.apache.crunch.*;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.Join;
import org.apache.crunch.lib.Sort;
import org.apache.crunch.types.writable.Writables;
import org.junit.Test;

/**
 * Created by dvorcjc on 7/25/2016.
 */
public class MovieTagCountTest {
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

    private PCollection<String> tags = MemPipeline.typedCollectionOf(
            Writables.strings(),
            ImmutableList.of(
                    "15::1::excellent!::1215184630",
                    "20::1::excellent!::1188263867",
                    "20::2::satire::1188263867",
                    "20::2::chick flick 212::1188263835",
                    "20::5::hanks::1188263835",
                    "20::5::ryan::1188263835",
                    "20::5::hanks::1188263755",
                    "20::3::bond::1188263756",
                    "20::4::spoof::1188263880",
                    "20::10::star wars::1188263880",
                    "20::9::bloody::1188263801",
                    "20::8::kung fu::1188263801",
                    "20::8::kung fu::1188263801"
            ));

    @Test
    public void runTest() {
        Pipeline pipeline = MemPipeline.getInstance();

        PTable<String,String> movieTable = movies.parallelDo(
                new LineToTable(0,1,3,"::"),
                Writables.tableOf(Writables.strings(),Writables.strings())
        );

        PTable<String,String> tagTable = tags.parallelDo(
                new LineToTable(1,2,4,"::"),
                Writables.tableOf(Writables.strings(),Writables.strings())
        );

        // join on movieid
        PTable<String, Pair<String,String>> movieTagJoin = Join.innerJoin(movieTable,tagTable);

        PTable<Pair<String, String>, Long> movieTagKey = movieTagJoin.parallelDo(
                new MovieTagCount.MovieTagCountPrep(),
                Writables.tableOf(Writables.pairs(Writables.strings(), Writables.strings()), Writables.longs())
        );

        PTable<Pair<String, String>, Long> movieTagCount = movieTagKey.groupByKey().combineValues(Aggregators.SUM_LONGS());

//        PCollection<Tuple3<String,String,Long>>  movieTagCol = movieTagCount.parallelDo(
//                new MovieTagCount.MovieTagCountColumns(),
//                Writables.triples(Writables.strings(),Writables.strings(),Writables.longs())
//        );
//
//        movieTagCol = Sort.sortTriples(movieTagCol,
//                Sort.ColumnOrder.by(1, Sort.Order.ASCENDING),
//                Sort.ColumnOrder.by(3, Sort.Order.DESCENDING)
//        );

        pipeline.done();

//        Iterable<Tuple3<String,String,Long>> it = movieTagCol.materialize();
//        for (Tuple3<String,String,Long> record : it) {
//            System.out.println(record.toString());
//        }

        Iterable<Pair<Pair<String,String>,Long>> it = movieTagCount.materialize();
        for (Pair<Pair<String,String>,Long> record : it) {
            System.out.println(record.toString());
        }
    }
}
