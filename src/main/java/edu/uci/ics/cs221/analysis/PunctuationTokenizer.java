package edu.uci.ics.cs221.analysis;

import java.util.*;

/**
 * Project 1, task 1: Implement a simple tokenizer based on punctuations and white spaces.
 *
 * For example: the text "I am Happy Today!" should be tokenized to ["happy", "today"].
 *
 * Requirements:
 *  - White spaces (space, tab, newline, etc..) and punctuations provided below should be used to tokenize the text.
 *  - White spaces and punctuations should be removed from the result tokens.
 *  - All tokens should be converted to lower case.
 *  - Stop words should be filtered out. Use the stop word list provided in `StopWords.java`
 *
 */

/**
 * Program logic:
 *     1. Scan the input string. Sliding windows, Start with a letter, end with spaces/punctuations.
 *     2. Get the scanned string in the current window, convert it to lowercase
 *     3. Check whether it is a stop word
 *     4. If not a stop word, append it to the result list.
 *     5. Change start to end+1, back to step 1.
 */

public class PunctuationTokenizer implements Tokenizer {

    public static Set<String> punctuations = new HashSet<>();
    static {
        punctuations.addAll(Arrays.asList(",", ".", ";", "?", "!"));
    }

    public PunctuationTokenizer() {}

    private boolean isEnd(char cur){
        //System.out.println("Testing char is " + cur);
        if(cur == ' ' || cur == '\t' || cur == '\r' || cur == '\n' || punctuations.contains(Character.toString(cur))){
            return true;
        }
        return false;
    }

    public List<String> tokenize(String text) {

        List<String> res = new ArrayList<>();
        if(text == null){return res;}
        StopWords sw = new StopWords();

        int start = 0;
        int end = start;
        int size = text.length();
        String tmp;

        while(end < size){
            //System.out.println("The end is "+text.charAt(end) +" with start : " + start + " end : "+end);

            //Deal with the situation that the string ends with letter. eg "happy"
            if(end == size-1 && !isEnd(text.charAt(end))){
                tmp = text.substring(start,end+1);
                //System.out.println("End string is " + tmp);
                if(tmp.length() >0 && !sw.stopWords.contains(tmp.toLowerCase())){
                    res.add(tmp.toLowerCase());
                }
                break;
            }

            //regular case, if find the end char, stop it and analyze it.
            //Otherwise, move the end to next char.
            if(isEnd(text.charAt(end))){
                tmp =text.substring(start,end);
                //System.out.println("Catch string "+ tmp);
                if(tmp.length()>0 && !sw.stopWords.contains(tmp.toLowerCase())){
                    res.add(tmp.toLowerCase());
                }
                start = end+1;
                end = start;
            }
            else{
                end++;
            }
        }

        return res;
    }

}
