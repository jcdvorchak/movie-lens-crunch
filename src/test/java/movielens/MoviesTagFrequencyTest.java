package movielens;

import com.google.common.collect.ImmutableList;
import org.apache.crunch.*;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.junit.Assert;
import org.junit.Test;

public class MoviesTagFrequencyTest {
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

    private PTable<Pair<String,String>,Long> moviesTagCountExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.pairs(Writables.strings(), Writables.strings()), Writables.longs()),
            ImmutableList.of(
                    Pair.of(Pair.of("Father of the Bride Part II (1995)","hanks"),2L),
                    Pair.of(Pair.of("Father of the Bride Part II (1995)","ryan"),1L),
                    Pair.of(Pair.of("GoldenEye (1995)","star wars"),1L),
                    Pair.of(Pair.of("Grumpier Old Men (1995)","bond"),1L),
                    Pair.of(Pair.of("Jumanji (1995)","chick flick 212"),1L),
                    Pair.of(Pair.of("Jumanji (1995)","satire"),1L),
                    Pair.of(Pair.of("Sudden Death (1995)","bloody"),1L),
                    Pair.of(Pair.of("Tom and Huck (1995)","kung fu"),2L),
                    Pair.of(Pair.of("Toy Story (1995)","excellent!"),2L),
                    Pair.of(Pair.of("Waiting to Exhale (1995)","spoof"),1L)
            ));

    private PCollection<Tuple3<String,String,Long>> moviesMostFreqTagExpected = MemPipeline.typedCollectionOf(
            Writables.triples(Writables.strings(),Writables.strings(),Writables.longs()),
            ImmutableList.of(
                    Tuple3.of("Father of the Bride Part II (1995)","hanks",2L),
                    Tuple3.of("GoldenEye (1995)","star wars",1L),
                    Tuple3.of("Grumpier Old Men (1995)","bond",1L),
                    Tuple3.of("Jumanji (1995)","chick flick 212|satire",1L),
                    Tuple3.of("Sudden Death (1995)","bloody",1L),
                    Tuple3.of("Tom and Huck (1995)","kung fu",2L),
                    Tuple3.of("Toy Story (1995)","excellent!",2L),
                    Tuple3.of("Waiting to Exhale (1995)","spoof",1L)
            ));

    private MoviesTagFrequency moviesTagFrequency = new MoviesTagFrequency();

    // for manual verification
    //@Test
    public void runTest() {
        Pipeline pipeline = MemPipeline.getInstance();

        // secondary sort to find the max genre count for a rater
        PCollection<Tuple3<String,String,Long>> moviesMostFreqTag = moviesTagFrequency.moviesMostFreqTag(moviesTagCountExpected);

        Iterable<Tuple3<String,String,Long>> it = moviesMostFreqTag.materialize();
        for (Tuple3<String,String,Long> record : it) {
            System.out.println(record.toString());
        }
    }

    @Test
    public void moviesTagCountTest() {
        PTable<Pair<String,String>,Long> moviesTagCount = moviesTagFrequency.moviesTagCount(movies, tags);

        Assert.assertEquals(moviesTagCountExpected.toString(),moviesTagCount.toString());
    }

    @Test
    public void moviesMostFreqTagTest() {
        PCollection<Tuple3<String,String,Long>> moviesMostFreqTag = moviesTagFrequency.moviesMostFreqTag(moviesTagCountExpected);

        Assert.assertEquals(moviesMostFreqTagExpected.toString(),moviesMostFreqTag.toString());
    }
}
