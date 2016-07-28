package movielens;

import com.google.common.collect.ImmutableList;
import junit.framework.Assert;
import org.apache.crunch.*;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.junit.Test;

public class RatersGenreFrequencyTest {
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

    private PTable<Pair<String, String>, Long> ratersGenreCountExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.pairs(Writables.strings(), Writables.strings()), Writables.longs()),
            ImmutableList.of(
                    Pair.of(Pair.of("Action","2"),4L),
                    Pair.of(Pair.of("Action","3"),3L),
                    Pair.of(Pair.of("Adventure","1"),2L),
                    Pair.of(Pair.of("Adventure","3"),2L),
                    Pair.of(Pair.of("Animation","1"),1L),
                    Pair.of(Pair.of("Children","1"),2L),
                    Pair.of(Pair.of("Children","3"),1L),
                    Pair.of(Pair.of("Comedy","1"),4L),
                    Pair.of(Pair.of("Comedy","2"),3L),
                    Pair.of(Pair.of("Crime","2"),4L),
                    Pair.of(Pair.of("Drama","2"),1L),
                    Pair.of(Pair.of("Fantasy","1"),2L),
                    Pair.of(Pair.of("Romance","1"),3L),
                    Pair.of(Pair.of("Romance","2"),1L),
                    Pair.of(Pair.of("Thriller","2"),4L),
                    Pair.of(Pair.of("Thriller","3"),1L)
            ));

    private PCollection<Tuple3<String,String,Long>> ratersMostFreqGenreExpected = MemPipeline.typedCollectionOf(
            Writables.triples(Writables.strings(),Writables.strings(),Writables.longs()),
            ImmutableList.of(
                    Tuple3.of("1","Comedy",4L),
                    Tuple3.of("2","Action|Crime|Thriller",4L),
                    Tuple3.of("3","Action",3L)
            )
    );

    private RatersGenreFrequency ratersGenreFrequency = new RatersGenreFrequency();


    // for manual verification
    //@Test
    public void testRun() {
        PCollection<Tuple3<String,String,Long>> ratersMostFreqGenre = ratersGenreFrequency.ratersMostFreqGenre(ratersGenreCountExpected);

        Iterable<Tuple3<String,String,Long>> it = ratersMostFreqGenre.materialize();
        for (Tuple3<String,String,Long> record : it) {
            System.out.println(record.toString());
        }
    }

    @Test
    public void ratersGenreCountTest() {
        PTable<Pair<String, String>, Long> ratersGenreCount = ratersGenreFrequency.ratersGenreCount(movies, ratings);

        Assert.assertEquals(ratersGenreCountExpected.toString(),ratersGenreCount.toString());
    }

    @Test
    public void ratersMostFreqGenreTest() {
        PCollection<Tuple3<String,String,Long>> ratersMostFreqGenre = ratersGenreFrequency.ratersMostFreqGenre(ratersGenreCountExpected);

        Assert.assertEquals(ratersMostFreqGenreExpected.toString(),ratersMostFreqGenre.toString());
    }
}
