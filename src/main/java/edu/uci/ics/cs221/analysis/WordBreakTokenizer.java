package edu.uci.ics.cs221.analysis;


import javax.management.RuntimeErrorException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

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
 *      2. Calculate the prob array where prob[i][j] means the highest probability for string(i to j)
 *      3. Calculate the res hashmap with key format "row-col" and value format "List<String> of words with highest probability"
 *      4. Follow the DP algorithm in the slides
 *      5. Get the result from res hashmap with key "0-(len-1) : List<String> "
 */
public class WordBreakTokenizer implements Tokenizer {

    private List<String> dictLines;
    private HashMap<String, Double> map;
    public  Set<String> stopWords;


    public WordBreakTokenizer() {
        try {
            // load the dictionary corpus
            URL dictResource = WordBreakTokenizer.class.getClassLoader().getResource("cs221_frequency_dictionary_en.txt");
            List<String> dl = Files.readAllLines(Paths.get(dictResource.toURI()));
            this.dictLines = dl;
            StopWords sw = new StopWords();
            this.stopWords = new HashSet<>(sw.stopWords);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public WordBreakTokenizer(String language) {
        String dict_name = "";

        if(language == "jp"){
            dict_name = "freq_dict_jp.txt";
            //change stop words
            try {
                URL stopword_url = WordBreakTokenizer.class.getClassLoader().getResource("stop_words_jp.txt");
                List<String> st = Files.readAllLines(Paths.get(stopword_url.toURI()));
                this.stopWords = new HashSet<>(st);
            }
            catch (Exception e){
                throw new RuntimeException(e);
            }
        }
        try {
            // load the dictionary corpus
            URL dictResource = WordBreakTokenizer.class.getClassLoader().getResource(dict_name);
            List<String> dl = Files.readAllLines(Paths.get(dictResource.toURI()));
            this.dictLines = dl;

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
            sum += Double.parseDouble(tmp[1]);
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

    /**
     * @param text : the input string
     * @return List<String> : the tokenized words
     */

    public List<String> tokenize(String text) {
        if(text == null || text.length() == 0){return new ArrayList<>();}
        map = getHashMap();
        text = text.toLowerCase();
        //System.out.println(map.get("undermining"));
        int size = text.length();
        double[][] prob = new double[size][size];

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
                        if(!stopWords.contains(letter)){
                            tmp.add(letter);
                        }
                        res.put(start+"-"+start, tmp);
                    }
                    continue;
                }

                double highest_prob = -Double.MAX_VALUE;
                List<String> tmp= new ArrayList<>();

                //System.out.println(start + " : " + end + " : " + text.substring(start,end+1));


                if(map.containsKey(text.substring(start,end+1))){
                    highest_prob = map.get(text.substring(start,end+1));
                    //System.out.println("Map prob : " + highest_prob);
                    if(!stopWords.contains(text.substring(start, end + 1))){
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
                                tmp.addAll(res.get(start + "-" + k));
                                tmp.addAll(res.get((k + 1) + "-" + end));
                            }
                        }
                    }
                }

                if(highest_prob != -Double.MAX_VALUE){
                    prob[start][end] =highest_prob;
                }
                res.put(start+"-"+end,tmp);
            }
        }


        // If word break can't find a way to divide the input string
        if(res.get("0-"+(size-1)).size() == 1 && res.get("0-"+(size-1)).get(0) == text){
            throw new RuntimeException();
        }

        // If word break can't do anything about the string due to some special character
        if(res.get("0-"+(size-1)).size() == 0 && prob[0][size-1] == 0){
            throw new RuntimeException();
        }

        return res.get("0-"+(size-1));
    }
}
