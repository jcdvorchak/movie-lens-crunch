package movielens.transformations;

import com.google.common.collect.ImmutableList;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.writable.Writables;
import org.junit.Test;

/**
 * Created by dvorcjc on 7/21/2016.
 */
public class LineToTableTest {
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

    @Test
    public void testProcess() throws Exception {

        MemPipeline.getInstance();

        PTable<String,String> movieTable = movies.parallelDo(
                new LineToTable(0,1,3,"::"),
                Writables.tableOf(Writables.strings(), Writables.strings())
        );

        Iterable<Pair<String,String>> it = movieTable.materialize();
        for (Pair<String,String> i : it) {
            System.out.println(i.first()+"::"+i.second());
        }
    }
}