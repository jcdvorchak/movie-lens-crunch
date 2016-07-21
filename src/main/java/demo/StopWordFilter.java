package demo;

import com.google.common.collect.ImmutableSet;
import org.apache.crunch.FilterFn;

import java.util.Set;

/**
 * Created by dvorcjc on 7/21/2016.
 */
public class StopWordFilter extends FilterFn<String> {
    // English stop words, borrowed from Lucene.
    private final Set<String> STOP_WORDS = ImmutableSet.copyOf(new String[] {
            "a", "and", "are", "as", "at", "be", "but", "by",
            "for", "if", "in", "into", "is", "it",
            "no", "not", "of", "on", "or", "s", "such",
            "t", "that", "the", "their", "then", "there", "these",
            "they", "this", "to", "was", "will", "with"
    });

    @Override
    public boolean accept(String word) {
        return !STOP_WORDS.contains(word);
    }
}