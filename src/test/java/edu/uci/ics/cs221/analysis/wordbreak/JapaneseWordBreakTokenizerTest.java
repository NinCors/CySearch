package edu.uci.ics.cs221.analysis.wordbreak;

import edu.uci.ics.cs221.analysis.WordBreakTokenizer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JapaneseWordBreakTokenizerTest {
    @Test
    public void locationTest() {
        String text = "関西国際空港";
        List<String> expected = Arrays.asList("関西","国際","空港");

        WordBreakTokenizer tokenizer = new WordBreakTokenizer("jp");

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void OnePieceTest() {
        String text = "俺は海贼王になる男だ";
        List<String> expected = Arrays.asList("俺", "海贼王", "男");

        WordBreakTokenizer tokenizer = new WordBreakTokenizer("jp");

        assertEquals(expected, tokenizer.tokenize(text));
    }


    @Test
    public void ConanTest() {
        String text = "真実はいつも一つ";
        List<String> expected = Arrays.asList("真実", "いつも", "一つ");

        WordBreakTokenizer tokenizer = new WordBreakTokenizer("jp");

        assertEquals(expected, tokenizer.tokenize(text));
    }

    @Test
    public void SlamDunkTest() {
        String text = "私は天才です";
        List<String> expected = Arrays.asList("天才");

        WordBreakTokenizer tokenizer = new WordBreakTokenizer("jp");

        assertEquals(expected, tokenizer.tokenize(text));
    }

}
