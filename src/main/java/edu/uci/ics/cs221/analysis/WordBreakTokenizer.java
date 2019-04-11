package edu.uci.ics.cs221.analysis;

import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.lang.reflect.Array;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;

/**
 * Project 1, task 2: Implement a Dynamic-Programming based Word-Break Tokenizer.
 *
 * Word-break is a problem where given a dictionary and a string (text with all white spaces removed),
 * determine how to break the string into sequence of words.
 * For example:
 * input string "catanddog" is broken to tokens ["cat", "and", "dog"]
 *
 * We provide an English dictionary corpus with frequency information in "resources/cs221_frequency_dictionary_en.txt".
 * Use frequency statistics to choose the optimal way when there are many alternatives to break a string.
 * For example,
 * input string is "ai",
 * dictionary and probability is: "a": 0.1, "i": 0.1, and "ai": "0.05".
 *
 * Alternative 1: ["a", "i"], with probability p("a") * p("i") = 0.01
 * Alternative 2: ["ai"], with probability p("ai") = 0.05
 * Finally, ["ai"] is chosen as result because it has higher probability.
 *
 * Requirements:
 *  - Use Dynamic Programming for efficiency purposes.
 *  - Use the the given dictionary corpus and frequency statistics to determine optimal alternative.
 *      The probability is calculated as the product of each token's probability, assuming the tokens are independent.
 *  - A match in dictionary is case insensitive. Output tokens should all be in lower case.
 *  - Stop words should be removed.
 *  - If there's no possible way to break the string, throw an exception.
 *
 */


/**
 * Program logic:
 *      1. Convert the dictionary to the hashmap with probability
 *
 */
public class WordBreakTokenizer implements Tokenizer {

    private List<String> dictLines;
    public WordBreakTokenizer() {
        try {
            // load the dictionary corpus
            URL dictResource = WordBreakTokenizer.class.getClassLoader().getResource("cs221_frequency_dictionary_en.txt");
            this.dictLines = Files.readAllLines(Paths.get(dictResource.toURI()));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the hashmap from dictionary
     * @return A Hashmap contain <word, probability>
     */
    public HashMap<String, Double> getHashMap(){
        HashMap<String, Double> map = new HashMap<>();

        //Get Sum of all counts
        long sum = 0;
        for(String cur:this.dictLines){
            String[] tmp = cur.split(" ");
            sum += Long.parseLong(tmp[1]);
        }
        //System.out.println("Sum is : " + sum );

        //Put the information into hashmap
        for(String cur:this.dictLines){
            if (cur.startsWith("\uFEFF")) {
                cur = cur.substring(1);
            }
            String[] tmp = cur.split(" ");
            map.put(tmp[0],Math.log(Double.parseDouble(tmp[1])/sum));
        }

        return map;
    }

    public List<String> tokenize(String text) {
        text = text.toLowerCase();
        HashMap<String, Double> map = getHashMap();
        //System.out.println(map.get("undermining"));
        int size = text.length();
        double[][] prob = new double[size][size];
        StopWords sw = new StopWords();

        // <INDEX, List of string>
        HashMap<String, List<String>> res= new HashMap<String,List<String>>();


        for(int len = 1; len <= size; ++len){
            for(int start = 0; start <= size - len; ++start){
                int end = start + len -1;

                if(start == end){
                    String letter = Character.toString(text.charAt(start));
                    if(map.containsKey(letter)) {
                        prob[start][start] = map.get(letter);
                        List<String> tmp = new ArrayList<>();
                        if(!sw.stopWords.contains(letter)){
                            tmp.add(letter);
                        }
                        res.put(start+""+start, tmp);
                    }
                    continue;
                }

                double highest_prob = -Double.MAX_VALUE;
                List<String> tmp= new ArrayList<>();

                //System.out.println(start + " : " + end + " : " + text.substring(start,end+1));


                if(map.containsKey(text.substring(start,end+1))){
                    highest_prob = map.get(text.substring(start,end+1));
                    //System.out.println("Map prob : " + highest_prob);
                    if(!sw.stopWords.contains(text.substring(start, end + 1))){
                        tmp.add(text.substring(start, end + 1));
                    }
                }

                else {
                    for (int k = start; k <= end; ++k) {
                        if (prob[start][k] != 0 && prob[k + 1][end] != 0) {
                            if (prob[start][k] + prob[k + 1][end] > highest_prob) {
                                highest_prob = prob[start][k] + prob[k + 1][end];
                                //System.out.println(start + " : " + k + " : " + end + "Change prob to : " + highest_prob);
                                tmp.clear();
                                tmp.addAll(res.get(start + "" + k));
                                tmp.addAll(res.get((k + 1) + "" + end));
                            }
                        }
                    }
                }

                if(highest_prob != -Double.MAX_VALUE){
                    prob[start][end] =highest_prob;
                }
                res.put(start+""+end,tmp);
            }
        }

        //System.out.println(prob[1][2]);
        //System.out.println(res);



        return res.get("0"+(size-1));
    }

}
