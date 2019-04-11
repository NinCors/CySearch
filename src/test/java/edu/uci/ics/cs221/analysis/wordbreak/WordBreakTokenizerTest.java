package edu.uci.ics.cs221.analysis.wordbreak;

import edu.uci.ics.cs221.analysis.WordBreakTokenizer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class WordBreakTokenizerTest {

    @Test
    public void test1() {
        String text = "catdog";
        List<String> expected = Arrays.asList("cat", "dog");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));

    }

    @Test
    public void testx() {
        String text = "ofthe";
        List<String> expected = new ArrayList<>();
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));

    }

    @Test
    public void test() {
        String text = "lordofthering";
        List<String> expected = Arrays.asList("lord", "ring");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));

    }
    //this test case checks if word is capitalized, should be lowered and also checks removal of stop
    //words as well as returning peanut butter instead of pea, nut, but, ter which are also in the
    //dictionary
    @Test
    public void test2()
    {
        String text = "IWANTtohavepeanutbuttersandwich";
        List<String> expected = Arrays.asList("want", "peanut", "butter", "sandwich");
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();

        assertEquals(expected, tokenizer.tokenize(text));
    }
    //this test checks that when text has a word not in dictionary and has spaces and question mark,
    //then it should throw an exception
    @Test(expected = UnsupportedOperationException.class)
    public void test3()
    {
        String text = "Where did Ghada go?";
        WordBreakTokenizer tokenizer = new WordBreakTokenizer();
        tokenizer.tokenize(text);
    }

}
