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

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text,"jp"));
    }

    @Test
    public void OnePieceTest() {
        String text = "俺は海贼王になる男だ";
        List<String> expected = Arrays.asList("俺", "は", "海贼王", "に", "なる", "男", "だ");

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text,"jp"));
    }


    @Test
    public void ConanTest() {
        String text = "真実はいつも一つ";
        List<String> expected = Arrays.asList("真実", "は", "いつも", "一つ");

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text,"jp"));
    }

    @Test
    public void SlamDunkTest() {
        String text = "私は天才です";
        List<String> expected = Arrays.asList("私", "は", "天才", "です");

        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text,"jp"));
    }

}
