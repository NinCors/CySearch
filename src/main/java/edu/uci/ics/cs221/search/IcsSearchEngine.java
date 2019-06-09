package edu.uci.ics.cs221.search;

import edu.uci.ics.cs221.index.inverted.InvertedIndexManager;
import edu.uci.ics.cs221.index.inverted.Pair;
import edu.uci.ics.cs221.storage.Document;

import java.io.File;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


/**
 * Page Rank Logic:
 *      1. Load the connection graph between different nodes into memory
 *      2. Use the score list to record the score of each document with initial value 1
 *      3. For each literation:
 *
 *
 */


public class IcsSearchEngine {

    private InvertedIndexManager indexManager;
    private Path documentDirectory;
    private double d;
    private int totalDocNum;
    private List<Double> pageRankScores;


    /**
     * Initializes an IcsSearchEngine from the directory containing the documents and the
     *
     */
    public static IcsSearchEngine createSearchEngine(Path documentDirectory, InvertedIndexManager indexManager) {
        return new IcsSearchEngine(documentDirectory, indexManager);
    }

    private IcsSearchEngine(Path documentDirectory, InvertedIndexManager indexManager) {
        this.indexManager = indexManager;
        this.d = 0.85;
        this.documentDirectory = documentDirectory;
        this.totalDocNum = new File(documentDirectory + "/cleaned").listFiles().length;
        this.pageRankScores = new ArrayList<>(Collections.nCopies(totalDocNum,0.0));
    }

    /**
     * Writes all ICS web page documents in the document directory to the inverted index.
     */
    public void writeIndex() {
        try {
            for(int i =0; i< totalDocNum; i++){
                List<String> lines= Files.readAllLines(Paths.get(documentDirectory + "/cleaned/"+Integer.toString(i)));
                String doc = "";
                for(String line:lines){
                    doc+=line+"\n";
                }
                this.indexManager.addDocument(new Document(doc));
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * Computes the page rank score from the "id-graph.tsv" file in the document directory.
     * The results of the computation can be saved in a class variable and will be later retrieved by `getPageRankScores`.
     */
    public void computePageRank(int numIterations) {

        // Load the graph from id-graph file
        HashMap<Integer, List<Integer>> in_graph = new HashMap<>(); // <docId, the docIds that points to the key>
        HashMap<Integer, Integer> out_graph = new HashMap<>(); // <docId>, number of docIds that this docID points to.


        try{
            List<String> lines= Files.readAllLines(Paths.get(documentDirectory + "/id-graph.tsv"));
            for(String line:lines){
                String [] docs = line.split("\\s+"); // sourceDocId, destDocId
                int sourceDocId = Integer.parseInt(docs[0]);
                int destDocId = Integer.parseInt(docs[1]);

                if(!in_graph.containsKey(destDocId)){
                    in_graph.put(destDocId,new ArrayList<>());
                }

                if(!out_graph.containsKey(sourceDocId)){
                    out_graph.put(sourceDocId,0);
                }

                in_graph.get(destDocId).add(sourceDocId);
                out_graph.put(sourceDocId, out_graph.get(sourceDocId)+1);
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }

        /*
        System.out.println("Total doc nums is "+ totalDocNum);
        System.out.println(in_graph.size());
        System.out.println(out_graph.size());
         */

        // Compute the page rank

        for(int i = 0; i < numIterations; i++){
            List<Double> curScores = new ArrayList<>(Collections.nCopies(totalDocNum,0.0));

            for(int docId = 0; docId< totalDocNum;docId++){
                if(in_graph.containsKey(docId)){
                    List<Integer> in_links = in_graph.get(docId);
                    double PrAccumulator = 0.0;

                    for (int link : in_links) {
                        PrAccumulator += this.pageRankScores.get(link) / out_graph.get(link);
                    }

                    curScores.set(docId, 1-this.d+PrAccumulator*this.d);
                }
                else{
                    curScores.set(docId, 1-this.d);
                    continue;
                }
            }
            this.pageRankScores = curScores;
        }

    }

    /**
     * Gets the page rank score of all documents previously computed. Must be called after `cmoputePageRank`.
     * Returns an list of <DocumentID - Score> Pairs that is sorted by score in descending order (high scores first).
     */
    public List<Pair<Integer, Double>> getPageRankScores() {
        List<Pair<Integer, Double>> res = new ArrayList<>();

        for(int i = 0; i < totalDocNum; i++){
            res.add(new Pair<>(i, this.pageRankScores.get(i)));
        }

        //sort the result
        Collections.sort(res, new Comparator<Pair<Integer, Double>>() {
            @Override
            public int compare( Pair<Integer, Double> p1,  Pair<Integer, Double> p2) {
                return Double.compare(p2.getRight(),p1.getRight());
            }
        });

        return res;
    }

    /**
     * Searches the ICS document corpus and returns the top K documents ranked by combining TF-IDF and PageRank.
     *
     * The search process should first retrieve ALL the top documents from the InvertedIndex by TF-IDF rank,
     * by calling `searchTfIdf(query, null)`.
     *
     * Then the corresponding PageRank score of each document should be retrieved. (`computePageRank` will be called beforehand)
     * For each document, the combined score is  tfIdfScore + pageRankWeight * pageRankScore.
     *
     * Finally, the top K documents of the combined score are returned. Each element is a pair of <Document, combinedScore>
     *
     *
     * Note: We could get the Document ID by reading the first line of the document.
     * This is a workaround because our project doesn't support multiple fields. We cannot keep the documentID in a separate column.
     */
    public Iterator<Pair<Document, Double>> searchQuery(List<String> query, int topK, double pageRankWeight) {
        throw new UnsupportedOperationException();
    }

}
