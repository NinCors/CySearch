package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;

import java.io.File;
import java.nio.ByteBuffer;
import org.checkerframework.checker.units.qual.A;

import javax.swing.plaf.synth.SynthTextAreaUI;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Program Logic for document operation:
 *      Keep an in-memory hashmap buffer to store all the keys and posting list
 *      While adding document:
 *          1. insert document to document store
 *          2. Tokenize the document and analyze it to get a list of word
 *          3. For each word in the list:
 *              append it to hashmap with the format <word, current docID> if not exist.
 *              Otherwise, just append the current DocId to it.
 *          4. Increase DocID.
 *      When the DocID reaches the DEFAULT_FLUSH_THRESHOLD -> flush():
 *          1. DocID = 0;
 *          2. Create the segment<x> file
 *          3. Flush it to disk:
 *              Format: sizeOfDictionary +sizeOfDocument + dictionary(wordLength+word+offset+length) + eachList
 *          4. Segment number++
 *          5. Create new document store file based on Segment
 *          6. Clear hashmap
 *      When the number of segment is even -> merge() all:
 *          1. For segment i from segment 1 to the number of segment
 *          2. Merge segment i with segment i-1:
 *              1. Fetch the dictionaries from both segment:
 *              2. Use two pointers to access key words from dictionaries in order.
 *              3. If the keywords not equal:
 *                  Fetch the larger keywords lists to memory, and insert it to the new merged file
 *              4. If the keywords are equal:
 *                  Fetch both list and merge them to one, then insert it to the new merged file
 *              5. Decrease the segment number when finish one pair
 *
 * Program logic for search operation:
 *       TBC
 *
 *
 */



public class InvertedIndexManager {

    /**
     * The default flush threshold, in terms of number of documents.
     * For example, a new Segment should be automatically created whenever there's 1000 documents in the buffer.
     *
     * In test cases, the default flush threshold could possibly be set to any number.
     */
    public static int DEFAULT_FLUSH_THRESHOLD = 1000;

    /**
     * The default merge threshold, in terms of number of segments in the inverted index.
     * When the number of segments reaches the threshold, a merge should be automatically triggered.
     *
     * In test cases, the default merge threshold could possibly be set to any number.
     */
    public static int DEFAULT_MERGE_THRESHOLD = 8;

    private TreeMap<String,List<Integer>> SEGMENT_BUFFER;
    private TreeMap<Integer,Document> DOCSTORE_BUFFER;
    private Analyzer analyzer;
    private int docCounter;
    private int segmentCounter;
    protected String indexFolder;



    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        docCounter = 0;
        segmentCounter =0;
        if(indexFolder.charAt(indexFolder.length()-1) != '/'){
            indexFolder += '/';
        }
        this.indexFolder = indexFolder;
        this.DOCSTORE_BUFFER = new TreeMap<>();
        this.SEGMENT_BUFFER = new TreeMap<>();
        this.analyzer = analyzer;
    }

    /**
     * Creates an inverted index manager with the folder and an analyzer
     */
    public static InvertedIndexManager createOrOpen(String indexFolder, Analyzer analyzer) {
        try {
            Path indexFolderPath = Paths.get(indexFolder);
            if (Files.exists(indexFolderPath) && Files.isDirectory(indexFolderPath)) {
                if (Files.isDirectory(indexFolderPath)) {
                    return new InvertedIndexManager(indexFolder, analyzer);
                } else {
                    throw new RuntimeException(indexFolderPath + " already exists and is not a directory");
                }
            } else {
                Files.createDirectories(indexFolderPath);
                return new InvertedIndexManager(indexFolder, analyzer);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Adds a document to the inverted index.
     *     1.Insert document to document store
     *     2.Tokenize the document and analyze it to get a list of word
     *     3.For each word in the list:
     *         Append it to hashmap with the format <word, current docID> if not exist.
     *         Otherwise, just append the current DocId to it.
     *     4.Increase DocID.
     * Document should live in a in-memory buffer until `flush()` is called to write the segment to disk.
     * @param document
     */
    public void addDocument(Document document) {
        if(this.docCounter == DEFAULT_FLUSH_THRESHOLD){
            flush();
            return;
        }
        List<String> words = this.analyzer.analyze(document.getText());
        DOCSTORE_BUFFER.put(this.docCounter, document);
        for(String word:words){
            if(this.SEGMENT_BUFFER.containsKey(word)){
                this.SEGMENT_BUFFER.get(word).add(this.docCounter);
            }
            else{
                List<Integer> tmp = new ArrayList<>();
                tmp.add(this.docCounter);
                this.SEGMENT_BUFFER.put(word, tmp);
            }
        }
        this.docCounter++;
    }

    /**
     * Flushes all the documents in the in-memory segment buffer to disk. If the buffer is empty, it should not do anything.
     * flush() writes the segment to disk containing the posting list and the corresponding document store.
     * When the DocID reaches the DEFAULT_FLUSH_THRESHOLD -> flush():
     *          1. DocID = 0;
     *          2. Create the segment<x> file
     *          3. Flush it to disk:
     *              Format: sizeOfDictionary +sizeOfDocument + dictionary(wordLength+word+offset+length) + eachList
     *          4. Segment number++
     *          5. Create new document store file based on Segment
     */
    public void flush() {
        System.out.println(this.SEGMENT_BUFFER);
        System.out.println(this.DOCSTORE_BUFFER);

        //Open segment file
        Path indexFilePath = Paths.get(this.indexFolder+"segment"+segmentCounter+".seg");
        PageFileChannel segment = PageFileChannel.createOrOpen(indexFilePath);
        //get sorted key from the segment buffer
        Set<String> keys = this.SEGMENT_BUFFER.keySet();

        //calculate the estimated size of the dictionary part
        int dic_size = 8;// sizeofDictionary + DocumentOffset
        for(String key:keys){
            dic_size += 12+key.length();//offset,listLength,keyLength+real key;
        }
        ByteBuffer dict_part = ByteBuffer.allocate(dic_size);
        dict_part.putInt(keys.size());
        dict_part.putInt(dic_size);
        System.out.println("Size of dict is : " + dic_size);

        dic_size += (4096 - dic_size%4096);

        //build the dictionary part
        for(String key:keys){
            dict_part.putInt(key.length());
            dict_part.put(key.getBytes());
            dict_part.putInt(dic_size);
            dict_part.putInt(this.SEGMENT_BUFFER.get(key).size()*4);
            dic_size+=this.SEGMENT_BUFFER.get(key).size()*4;
        }
        segment.appendAllBytes(dict_part);
        System.out.println("Size of whole dictionary is : " + dic_size);

        ByteBuffer page_tmp = ByteBuffer.allocate(4096);
        //Append all the real posting list into disk
        for(String key:keys){
            List<Integer> tmp = this.SEGMENT_BUFFER.get(key);
            int list_size = tmp.size()*4;
            ByteBuffer postingList = ByteBuffer.allocate(list_size);
            for(int i: tmp){
                postingList.putInt(i);
            }
            if(page_tmp.position()+list_size < page_tmp.capacity()){
                page_tmp.put(postingList);
            }
            else if(page_tmp.position()+list_size == page_tmp.capacity()){
                page_tmp.put(postingList);
                segment.appendPage(page_tmp);
                page_tmp = ByteBuffer.allocate(4096);
            }
            else{
                /*
                    eg. Current page has 4000 bytes, need add list with 1000 bytes
                        sizeForCurPage = 4096 - 4000%4096 = 96
                        sizeForNextPage = 1000 - 96;
                 */
                //append the leftside of list into page
                int sizeForCurPage = (4096 - page_tmp.position());
                postingList.position(0);
                postingList.limit(sizeForCurPage);
                page_tmp.put(postingList);
                segment.appendPage(page_tmp);

                //re-adjust the posting list
                postingList.rewind();
                postingList.position(sizeForCurPage);

                int sizeForNextPage = list_size -sizeForCurPage;
                while(sizeForNextPage > 0){
                    int write_size = 4096;
                    if(sizeForNextPage < write_size){
                        write_size = sizeForCurPage;
                    }
                    postingList.limit(postingList.position()+write_size);
                    page_tmp = ByteBuffer.allocate(4096);
                    page_tmp.put(postingList);
                    postingList.position(postingList.limit());
                    sizeForNextPage -= write_size;
                }

            }
        }

        //write the document store file
        DocumentStore ds = MapdbDocStore.createWithBulkLoad(this.indexFolder+"doc"+segmentCounter+".db",this.DOCSTORE_BUFFER.entrySet().iterator());

        //Ready for next segment
        segment.close();
        ds.close();
        this.segmentCounter++;
        this.DOCSTORE_BUFFER.clear();
        this.SEGMENT_BUFFER.clear();
        this.docCounter = 0;

    }

    /**
     * Merges all the disk segments of the inverted index pair-wise.
     *      When the number of segment is even -> merge() all:
     *          1. For segment i from segment 1 to the number of segment
     *          2. Merge segment i with segment i-1:
     *              1. Fetch the dictionaries from both segment:
     *              2. Use two pointers to access key words from dictionaries in order.
     *              3. If the keywords not equal:
     *                  Fetch the larger keywords lists to memory, and insert it to the new merged file
     *                4. If the keywords are equal:
     *                    Fetch both list and merge them to one, then insert it to the new merged file
     *                5. Decrease the segment number when finish one pair
     */

    public void mergeAllSegments() {
        // merge only happens at even number of segments
        Preconditions.checkArgument(getNumSegments() % 2 == 0);
        for(int i = 1; i<this.segmentCounter; i++){

        }

    }

    /**
     * Performs a single keyword search on the inverted index.
     * You could assume the analyzer won't convert the keyword into multiple tokens.
     * If the keyword is empty, it should not return anything.
     *
     * @param keyword keyword, cannot be null.
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchQuery(String keyword) {
        Preconditions.checkNotNull(keyword);

        throw new UnsupportedOperationException();
    }

    /**
     * Performs an AND boolean search on the inverted index.
     *
     * @param keywords a list of keywords in the AND query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchAndQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);

        throw new UnsupportedOperationException();
    }

    /**
     * Performs an OR boolean search on the inverted index.
     *
     * @param keywords a list of keywords in the OR query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchOrQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);

        throw new UnsupportedOperationException();
    }

    /**
     * Iterates through all the documents in all disk segments.
     * Program logic:
     *      1. Scan all the doc file and keep read
     *
     */
    public Iterator<Document> documentIterator() {
        Iterator<Document> it = new Iterator<Document>() {
            boolean first = true;
            Iterator<Integer> key = null;
            int cur_seg_num = 0;
            DocumentStore ds = null;

            private boolean openDoc(){
                File doc = new File(indexFolder+"doc"+cur_seg_num+".db");
                if (!doc.exists()) {return false;}
                this.ds = MapdbDocStore.createOrOpen(indexFolder+"doc"+cur_seg_num+".db");
                key = ds.keyIterator();
                this.cur_seg_num++;
                return true;
            }

            @Override
            public boolean hasNext() {
                if(key == null || !key.hasNext()){
                    if(!openDoc()){return false;}
                }
                return key.hasNext();
            }

            @Override
            public Document next() {
                if(hasNext()){
                    return ds.getDocument(key.next());
                }
                return null;
            }
        };
        return it;
    }

    /**
     * Deletes all documents in all disk segments of the inverted index that match the query.
     * @param keyword
     */
    public void deleteDocuments(String keyword) {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets the total number of segments in the inverted index.
     * This function is used for checking correctness in test cases.
     *
     * @return number of index segments.
     */
    public int getNumSegments() {
        File file = new File(this.indexFolder);
        String[] filelist = file.list();
        if(this.segmentCounter != (filelist.length/2)){
            System.out.println("get segment wrong!");
            return -1;
        }
        return this.segmentCounter;
    }

    /**
     * Reads a disk segment into memory based on segmentNum.
     * This function is mainly used for checking correctness in test cases.
     *
     * @param segmentNum n-th segment in the inverted index (start from 0).
     * @return in-memory data structure with all contents in the index segment, null if segmentNum don't exist.
     */
    public InvertedIndexSegmentForTest getIndexSegment(int segmentNum) {
        File seg = new File(indexFolder+"segment"+segmentNum+".seg");
        File doc = new File(indexFolder+"doc"+segmentNum+".db");
        if (!doc.exists()||!seg.exists()) {return null;}

        TreeMap<String,List<Integer>> invertedList = new TreeMap<>();
        TreeMap<Integer,Document> documents = new TreeMap<>();

        Path indexFilePath = Paths.get(this.indexFolder+"segment"+segmentNum+".seg");
        PageFileChannel segment = PageFileChannel.createOrOpen(indexFilePath);

        TreeMap<String,int[]> dict = indexDicDecoder(segment);

        for(Map.Entry<String,int[]>entry:dict.entrySet()) {
            invertedList.put(entry.getKey(), indexListDecoder(entry.getValue(), segment));
        }

        segment.close();

        DocumentStore ds = MapdbDocStore.createOrOpen(indexFolder+"doc"+segmentNum+".db");
        Iterator<Map.Entry<Integer,Document>> it = ds.iterator();

        while(it.hasNext()){
            Map.Entry<Integer,Document> entry =it.next();
            documents.put(entry.getKey(),entry.getValue());
        }

        InvertedIndexSegmentForTest test = new InvertedIndexSegmentForTest(invertedList,documents);
        return test;
    }


    /**
     * Decode one posting list from segment file based on the dictionary information <offset, length>
     * Program logic:
     *
     * @param keyInfo -> [offset, length]
     * @param segment
     * @return
     */


    public List<Integer> indexListDecoder(int[] keyInfo, PageFileChannel segment){
        /*
            eg. Offset 5200 length 1000
            list_buffer = allocate(1000);
            5200/4096 = 1 -> open page 1
            5200%4096 = 1100-> in page 1
         */
        int startPageNum = keyInfo[0]/4096;
        int pageOffset = keyInfo[0]/4096;
        int finishPageNum = startPageNum + (pageOffset + keyInfo[0])/4096;
        ByteBuffer list_buffer = ByteBuffer.allocate((finishPageNum-startPageNum+1)*4096);

        for(int i = startPageNum; i<=finishPageNum;i++){
            list_buffer.put(segment.readPage(i));
        }
        list_buffer.position(pageOffset);
        List<Integer>res = new ArrayList<>();
        for(int i = 0; i<=keyInfo[1]-4;i+=4){
            res.add(list_buffer.getInt());
        }

        return res;
    }

    /**
     * Decode the content of dictionary from segment
     * Program Logic:
     *      1. read first page of seg to get segment information
     *      2. Load all pages that contains the dictionary content into one bytebuffer
     *      3. Keep extract the key from this bytebuffer until reaches the size of dictionary
     * @param segment
     * @return in-memory data structure of dictionary <Key, [offset, length]>
     */

    public TreeMap<String, int[]> indexDicDecoder(PageFileChannel segment){
        /*
        File seg = new File(this.indexFolder+"segment"+segmentNum+".seg");
        if(!seg.exists()){return null;}
        Path indexFilePath = Paths.get(this.indexFolder+"segment"+segmentNum+".seg");
        PageFileChannel segment = PageFileChannel.createOrOpen(indexFilePath);
        */

        TreeMap<String, int[]> dict = new TreeMap<>();
        int[] key_info = new int[2];
        ByteBuffer segInfo = segment.readPage(0);

        int key_num = segInfo.getInt();
        int doc_offset = segInfo.getInt();
        int page_num = doc_offset/4096;
        System.out.println("KeyNum: "+key_num+" docOffset: "+ doc_offset + " pageNum: "+page_num);

        ByteBuffer dic_content = ByteBuffer.allocate((page_num+1)*4096).put(segInfo);

        //read all the content of dictionary from disk
        for(int i =1;i<=page_num;i++){
            dic_content.put(segment.readPage(i));
        }
        dic_content.rewind();
        //loop through the dic_content to extract key
        //Format -> <key_length, key, offset, length>
        while(key_num > 0){
            int key_length =dic_content.getInt();
            System.out.println("Read: keyLength: "+ key_length);
            byte[] str = new byte[key_length];
            dic_content.get(str);
            String tmp_key = new String(str);
            System.out.println("Read: Key: "+ tmp_key);
            key_info[0] = dic_content.getInt();
            System.out.println("Read: Offset: "+ key_info[0]);
            key_info[1] = dic_content.getInt();
            System.out.println("Read: Length: "+ key_info[1]);
            dict.put(tmp_key,key_info);
            key_num--;
        }

        return dict;
    }

    /**
     * Test Functions---------------------------------------------------------
     */

    public void hashMapTest(){
        //Hashmap test
        TreeMap<String, Integer> mt = new TreeMap<>();
        mt.put("a",1);
        mt.put("c",4);
        mt.put("b",2);
        mt.put("d",3);

        TreeMap<String, Integer> mt1 = mt;

        Iterator<Map.Entry<String,Integer>> it = mt.entrySet().iterator();
        while(it.hasNext()){
            System.out.println(it.next().toString());
        }


        Set<String>set = mt1.keySet();
        for(String i: set){
            //System.out.println(i);
        }
        System.out.println(mt);
    }

    public void readAddBytefferTest(){
        //read from byte buffer test
        ByteBuffer tmp = ByteBuffer.allocate(17);
        tmp.putInt(5);
        tmp.put("hello".getBytes());
        tmp.putInt(10);
        tmp.putInt(10);

        ByteBuffer tmp1 = ByteBuffer.allocate(17);
        tmp1.putInt(5);
        tmp1.put("fucke".getBytes());
        tmp1.putInt(11);
        tmp1.putInt(12);

        tmp.rewind();
        //tmp.getInt();
        tmp1.rewind();

        //combine byte buffer test
        ByteBuffer tmp2 = ByteBuffer.allocate(34);
        tmp2.put(tmp1);
        tmp2.put(tmp);

        System.out.println("position"+tmp2.position());
        System.out.println("limit"+tmp2.limit());
        tmp2.rewind();
        System.out.println("position"+tmp2.position());
        System.out.println("limit"+tmp2.limit());
        tmp2.position(17);
        System.out.println(tmp2.getInt());
        byte[] str = new byte[5];
        tmp2.get(str);
        String s = new String(str);
        System.out.println(s);
        System.out.println(tmp2.getInt());
        System.out.println(tmp2.getInt());
        System.out.println(tmp2.capacity());

        System.out.println("position"+tmp2.position());
        System.out.println("limit"+tmp2.limit());

        tmp2.position(0);

        System.out.println(tmp2.getInt());
        byte[] str1 = new byte[5];
        tmp2.get(str1);
        String s1 = new String(str1);

        System.out.println(s1);
        System.out.println(tmp2.getInt());
        System.out.println(tmp2.getInt());
        System.out.println(tmp2.capacity());
        System.out.println("position"+tmp2.position());
        System.out.println("limit"+tmp2.limit());

        // Merge two byte buffer
        // bb = ByteBuffer.allocate(300).put(bb).put(bb2);

    }

    public void loopBytebufferTest(){
        ByteBuffer tmp = ByteBuffer.allocate(16);
        tmp.putInt(5);
        tmp.putInt(6);
        tmp.putInt(7);
        tmp.putInt(8);
    }


    public static void main(String[] args) throws Exception {






    }

}
