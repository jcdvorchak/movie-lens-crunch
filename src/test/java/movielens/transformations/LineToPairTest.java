package movielens.transformations;

import com.google.common.collect.ImmutableList;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.junit.Assert;
import org.junit.Test;

public class LineToPairTest {
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

    private PTable<String,String> movieTableExpected = MemPipeline.typedTableOf(
            Writables.tableOf(Writables.strings(),Writables.strings()),
            ImmutableList.of(
                    Pair.of("1","Toy Story (1995)"),
                    Pair.of("2","Jumanji (1995)"),
                    Pair.of("3","Grumpier Old Men (1995)"),
                    Pair.of("4","Waiting to Exhale (1995)"),
                    Pair.of("5","Father of the Bride Part II (1995)"),
                    Pair.of("6","Heat (1995)"),
                    Pair.of("7","Sabrina (1995)"),
                    Pair.of("8","Tom and Huck (1995)"),
                    Pair.of("9","Sudden Death (1995)"),
                    Pair.of("10","GoldenEye (1995)")
            ));

    @Test
    public void testProcess() throws Exception {

        MemPipeline.getInstance();

        PTable<String,String> movieTable = movies.parallelDo(
                new LineToPair(0,1,3,"::"),
                Writables.tableOf(Writables.strings(), Writables.strings())
        );

        Assert.assertEquals(movieTableExpected.toString(),movieTable.toString());
    }
}