package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import com.sun.jdi.request.MethodEntryRequest;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;

import java.io.File;
import java.nio.ByteBuffer;
import org.checkerframework.checker.units.qual.A;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import javax.print.Doc;
import javax.swing.plaf.synth.SynthTextAreaUI;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
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

        List<String> words = this.analyzer.analyze(document.getText());
        DOCSTORE_BUFFER.put(this.docCounter, document);
        for(String word:words){
            if(word!="") {
                if (this.SEGMENT_BUFFER.containsKey(word)) {
                    this.SEGMENT_BUFFER.get(word).add(this.docCounter);
                } else {
                    List<Integer> tmp = new ArrayList<>();
                    tmp.add(this.docCounter);
                    this.SEGMENT_BUFFER.put(word, tmp);
                }
            }
        }
        this.docCounter++;
        if(this.docCounter == DEFAULT_FLUSH_THRESHOLD){
            flush();
            return;
        }
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
        //System.out.println(this.SEGMENT_BUFFER);
        //System.out.println(this.DOCSTORE_BUFFER);

        //Open segment file
        if(this.docCounter == 0){
            return;
        }
        Path indexFilePath = Paths.get(this.indexFolder+"segment"+segmentCounter+".seg");
        PageFileChannel segment = PageFileChannel.createOrOpen(indexFilePath);
        //get sorted key from the segment buffer
        Set<String> keys = this.SEGMENT_BUFFER.keySet();

        //calculate the estimated size of the dictionary part
        int dic_size = 8;// sizeofDictionary + DocumentOffset
        for(String key:keys){
            dic_size += (12+key.getBytes(StandardCharsets.UTF_8).length);//offset,listLength,keyLength+real key;
        }
        ByteBuffer dict_part = ByteBuffer.allocate(dic_size+PageFileChannel.PAGE_SIZE - dic_size%PageFileChannel.PAGE_SIZE);
        dict_part.putInt(keys.size());
        dict_part.putInt(dic_size);
        //System.out.println("Size of dict is : " + dic_size);

        dic_size += (PageFileChannel.PAGE_SIZE - dic_size%PageFileChannel.PAGE_SIZE);

        //build the dictionary part
        for(String key:keys){
            dict_part.putInt(key.getBytes(StandardCharsets.UTF_8).length);
            dict_part.put(key.getBytes(StandardCharsets.UTF_8));
            dict_part.putInt(dic_size);
            dict_part.putInt(this.SEGMENT_BUFFER.get(key).size()*4);
            dic_size+=this.SEGMENT_BUFFER.get(key).size()*4;
        }
        segment.appendAllBytes(dict_part);
        //System.out.println("Disk has page : " + segment.getNumPages());
        //System.out.println("Size of whole dictionary is : " + dic_size);

        ByteBuffer page_tmp = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);

        //Append all the real posting list into disk
        for(String key:keys){
            List<Integer> tmp = this.SEGMENT_BUFFER.get(key);
            int list_size = tmp.size()*4;
            ByteBuffer postingList = ByteBuffer.allocate(list_size);
            for(int i: tmp){
                postingList.putInt(i);
            }
            postingList.rewind();
            //System.out.println("Append key "+key);
            //System.out.println("Position: "+page_tmp.position());
            //System.out.println("Capacity: "+page_tmp.capacity());
            //System.out.println("ListSize: "+list_size);
            if(page_tmp.position()+list_size < page_tmp.capacity()){
                page_tmp.put(postingList);
            }
            else if(page_tmp.position()+list_size == page_tmp.capacity()){
                page_tmp.put(postingList);
                segment.appendPage(page_tmp);
                page_tmp = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
            }
            else{
                /*
                    eg. Current page has 4000 bytes, need add list with 1000 bytes
                        sizeForCurPage = 4096 - 4000%4096 = 96
                        sizeForNextPage = 1000 - 96;
                 */
                //append the leftside of list into page


                int sizeForCurPage = (PageFileChannel.PAGE_SIZE - page_tmp.position());
                postingList.position(0);
                postingList.limit(sizeForCurPage);
                page_tmp.put(postingList);

                //System.out.println("Special case: sizeForCur->"+sizeForCurPage);

                segment.appendPage(page_tmp);
                page_tmp = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);


                //re-adjust the posting list
                postingList.rewind();
                postingList.position(sizeForCurPage);

                int sizeForNextPage = list_size -sizeForCurPage;
                //System.out.println("Special case: sizeForNext->"+sizeForNextPage);

                while(sizeForNextPage > 0){
                    int write_size = PageFileChannel.PAGE_SIZE;
                    if(sizeForNextPage < write_size){
                        write_size = sizeForNextPage;
                    }
                    if(postingList.position()+write_size<postingList.capacity()) {
                        postingList.limit(postingList.position() + write_size);
                    }
                    else{
                        postingList.limit(postingList.capacity());
                    }
                    //System.out.println("PostingList: position "+postingList.position()+" Limit "+postingList.limit());
                    page_tmp.put(postingList);
                    if(page_tmp.position()==page_tmp.capacity()){
                        segment.appendPage(page_tmp);
                        page_tmp = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
                    }
                    postingList.position(postingList.limit());
                    sizeForNextPage -= write_size;
                }
            }
        }
        //If there is any remaining bytes not add into disk
        if(page_tmp.position()>0){
            segment.appendPage(page_tmp);
        }

        //System.out.println("Disk has page : "+ segment.getNumPages());

        //write the document store file
        DocumentStore ds = MapdbDocStore.createWithBulkLoad(this.indexFolder+"doc"+this.segmentCounter+".db",this.DOCSTORE_BUFFER.entrySet().iterator());

        //Ready for next segment
        segment.close();
        ds.close();
        this.segmentCounter++;
        this.DOCSTORE_BUFFER.clear();
        this.SEGMENT_BUFFER.clear();
        this.docCounter = 0;

        if(this.segmentCounter == this.DEFAULT_MERGE_THRESHOLD){
            mergeAllSegments();
        }

    }


    public void updateSegementDocFile(int segNum, int mergedSegId){
        File doc_f1 = new File(this.indexFolder+"doc"+segNum+".db");
        File doc_f2 = new File(this.indexFolder+"doc"+(segNum+1)+".db");
        if(!doc_f1.delete() || !doc_f2.delete()){
            throw new UnsupportedOperationException();
            //System.out.println("Can't delete old doc!");
        }
        File doc_f3 = new File(this.indexFolder+"doc"+segNum+"_tmp"+".db");
        File doc_f4 = new File(this.indexFolder+"doc"+mergedSegId+".db");
        if(!doc_f3.renameTo(doc_f4)){
            throw new UnsupportedOperationException();
            //System.out.println("Can't rename the new doc!");
        }

        File f1 = new File(this.indexFolder+"segment"+segNum+".seg");
        File f2 = new File(this.indexFolder+"segment"+(segNum+1)+".seg");
        if(!f1.delete() || !f2.delete()){
            throw new UnsupportedOperationException();
            //System.out.println("Can't delete old segment!");
        }
        File f3 = new File(this.indexFolder+"segment"+segNum+"_tmp"+".seg");
        File f4 = new File(this.indexFolder+"segment"+mergedSegId+".seg");
        if(!f3.renameTo(f4)){
            throw new UnsupportedOperationException();
            //System.out.println("Can't rename the new segment!");
        }
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

    public void mergeAndflush(int segNum1,int segNum2,int mergedSegId){
        //System.out.println("Start merge------------------------------------------");
        //Open two local Doc file to merge
        DocumentStore ds1 = MapdbDocStore.createOrOpen(this.indexFolder+"doc"+segNum1+".db");
        DocumentStore ds2 = MapdbDocStore.createOrOpen(this.indexFolder+"doc"+segNum2+".db");
        DocumentStore merged_ds = MapdbDocStore.createOrOpen(this.indexFolder+"doc"+segNum1+"_tmp"+".db");
        int doc_counter = 0;

        Iterator<Map.Entry<Integer,Document>> it = ds1.iterator();
        while(it.hasNext()){
            //System.out.println("ds1");
            Map.Entry<Integer,Document> tmp = it.next();
            merged_ds.addDocument(tmp.getKey(),tmp.getValue());
            doc_counter++;
        }
        Iterator<Map.Entry<Integer,Document>> it2 = ds2.iterator();
        while(it2.hasNext()){
            //System.out.println("ds2");
            Map.Entry<Integer,Document> tmp = it2.next();
            merged_ds.addDocument(tmp.getKey()+doc_counter,tmp.getValue());
        }

        ds1.close();
        ds2.close();
        merged_ds.close();

        //Opem two segement file to merge
        Path indexFilePath = Paths.get(this.indexFolder+"segment"+segNum1+".seg");
        PageFileChannel segment1 = PageFileChannel.createOrOpen(indexFilePath);
        indexFilePath = Paths.get(this.indexFolder+"segment"+segNum2+".seg");
        PageFileChannel segment2 = PageFileChannel.createOrOpen(indexFilePath);
        //create the mergedSegment
        indexFilePath = Paths.get(this.indexFolder+"segment"+segNum1+"_tmp"+".seg");
        PageFileChannel merged_segement = PageFileChannel.createOrOpen(indexFilePath);

        TreeMap<String,int[]> dict1 = indexDicDecoder(segment1);
        TreeMap<String,int[]> dict2 = indexDicDecoder(segment2);
        TreeMap<String,List<int[]>> merged_dict = new TreeMap<>();

        //get the information of two segement
        ByteBuffer segInfo = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        segInfo.put(segment1.readPage(0));
        segInfo.rewind();
        int seg1_sizeOfDictionary = segInfo.getInt();
        int seg1_offset = segInfo.getInt();
        segInfo = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        segInfo.put(segment1.readPage(0));
        segInfo.rewind();
        int seg2_sizeOfDictionary = segInfo.getInt();
        int seg2_offset = segInfo.getInt();

        //Build the dictionary part
        int sizeOfDictionary = 0;
        int offset = seg1_offset+seg2_offset-8;
        ByteBuffer dict_part = ByteBuffer.allocate(offset+PageFileChannel.PAGE_SIZE - offset%PageFileChannel.PAGE_SIZE);
        //System.out.println("Budild head_part");

        //build merged map
        for(Map.Entry<String,int[]>entry:dict1.entrySet()) {
            entry.getValue()[2] = 0;

            if(merged_dict.containsKey(entry.getKey())){
                merged_dict.get(entry.getKey()).add(entry.getValue());
            }
            else{
                sizeOfDictionary++;
                List<int[]> tmp = new ArrayList<>();
                tmp.add(entry.getValue());
                merged_dict.put(entry.getKey(),tmp);
            }
        }

        for(Map.Entry<String,int[]>entry:dict2.entrySet()) {
            entry.getValue()[2] = 1;

            if(merged_dict.containsKey(entry.getKey())){
                merged_dict.get(entry.getKey()).add(entry.getValue());
            }
            else{
                sizeOfDictionary++;
                List<int[]> tmp = new ArrayList<>();
                tmp.add(entry.getValue());
                merged_dict.put(entry.getKey(),tmp);
            }
        }
        dict_part.putInt(sizeOfDictionary);
        dict_part.putInt(offset);

        offset += (PageFileChannel.PAGE_SIZE - offset%PageFileChannel.PAGE_SIZE);
        //System.out.println("nUM KEY"+sizeOfDictionary+"!!!!!!!!!!!merged map is : " +merged_dict.toString());
        for(Map.Entry<String,List<int[]>> entry:merged_dict.entrySet()){
            dict_part.putInt(entry.getKey().getBytes(StandardCharsets.UTF_8).length);
            dict_part.put(entry.getKey().getBytes(StandardCharsets.UTF_8));
            dict_part.putInt(offset);
            //compute the length of new list
            int len = 0;
            for(int[] info:merged_dict.get(entry.getKey())){
                len+= info[1];
            }
            dict_part.putInt(len);
            offset += len;
        }
        merged_segement.appendAllBytes(dict_part);
        //System.out.println("Finish head_part");

        //keep write all the posting list to the disk
        ByteBuffer page_tmp = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);

        for(Map.Entry<String,List<int[]>> entry:merged_dict.entrySet()){
            //Construct the list to insert
            List<Integer> tmp_list = new ArrayList<Integer>();
            for(int[] keyInfo:entry.getValue()){
                if(keyInfo[2] == 0){
                    tmp_list.addAll(indexListDecoder(keyInfo,segment1));
                }
                else{
                    List<Integer> list2 = indexListDecoder(keyInfo,segment2);
                    for(int i: list2){
                        tmp_list.add(i+doc_counter);
                    }
                }
            }

            int list_size = tmp_list.size()*4;
            ByteBuffer postingList = ByteBuffer.allocate(list_size);
            for(int i:tmp_list){
                postingList.putInt(i);
            }
            postingList.rewind();
            if(page_tmp.position()+list_size < page_tmp.capacity()){
                page_tmp.put(postingList);
            }
            else {
                int sizeForCurPage = (PageFileChannel.PAGE_SIZE - page_tmp.position());
                postingList.position(0);
                postingList.limit(sizeForCurPage);
                page_tmp.put(postingList);

                //System.out.println("Special case: sizeForCur->" + sizeForCurPage);

                merged_segement.appendPage(page_tmp);
                page_tmp = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);


                //re-adjust the posting list
                postingList.rewind();
                postingList.position(sizeForCurPage);

                int sizeForNextPage = list_size - sizeForCurPage;
                //System.out.println("Special case: sizeForNext->" + sizeForNextPage);

                while (sizeForNextPage > 0) {
                    int write_size = PageFileChannel.PAGE_SIZE;
                    if (sizeForNextPage < write_size) {
                        write_size = sizeForNextPage;
                    }
                    if (postingList.position() + write_size < postingList.capacity()) {
                        postingList.limit(postingList.position() + write_size);
                    } else {
                        postingList.limit(postingList.capacity());
                    }
                    //System.out.println("PostingList: position " + postingList.position() + " Limit " + postingList.limit());
                    page_tmp.put(postingList);
                    if (page_tmp.position() == page_tmp.capacity()) {
                        merged_segement.appendPage(page_tmp);
                        page_tmp = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
                    }
                    postingList.position(postingList.limit());
                    sizeForNextPage -= write_size;
                }
            }
        }
        //System.out.println("Finish listpart");

        //If there is any remaining bytes not add into disk
        if(page_tmp.position()>0){
            merged_segement.appendPage(page_tmp);
        }

        updateSegementDocFile(segNum1,mergedSegId);
        this.segmentCounter--;
        //System.out.println("Finish merge");

    }


    public void mergeAllSegments() {
        // merge only happens at even number of segments
        Preconditions.checkArgument(getNumSegments() % 2 == 0);
        int mergedSegId = 0;
        //System.out.println("!!!!!!!Total segemetns :" + this.segmentCounter);
        int totalSeg = this.segmentCounter;
        for(int i = 1; i<totalSeg; i+=2){
            mergeAndflush(i-1,i,mergedSegId);
            mergedSegId++;
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
        String key = analyzer.analyze(keyword).get(0);
        //System.out.println("Search key :" + key);
        Iterator<Document> it = new Iterator<Document>() {
            private int cur_seg_num = 0;
            Iterator<Integer> list = null;
            DocumentStore ds = null;

            private boolean openDoc(){
                while(cur_seg_num < getNumSegments()){
                    //System.out.println("Open seg : "+cur_seg_num);
                    if(this.ds != null){
                        this.ds.close();
                    }
                    this.ds = MapdbDocStore.createOrOpenReadOnly(indexFolder+"doc"+cur_seg_num+".db");
                    Path indexFilePath = Paths.get(indexFolder+"segment"+cur_seg_num+".seg");
                    PageFileChannel segment = PageFileChannel.createOrOpen(indexFilePath);
                    TreeMap<String,int[]> dict = indexDicDecoder(segment);

                    if (!dict.containsKey(key)) {
                        segment.close();
                        cur_seg_num++;
                        continue;
                    }
                    //System.out.println("Find keys!" + indexListDecoder(dict.get(key),segment).toString());
                    TreeSet<Integer> list_set = new TreeSet<>(indexListDecoder(dict.get(key),segment));
                    list = list_set.iterator();
                    segment.close();
                    cur_seg_num++;
                    return true;
                }
                return false;
            }
            @Override
            public boolean hasNext() {
                if(list==null||!list.hasNext()){
                    if(!openDoc()){return false;};
                }
                return list.hasNext();
            }

            @Override
            public Document next() {
                if(hasNext()){
                    return ds.getDocument(list.next());
                }
                throw new NoSuchElementException();
            }
        };

        return it;
    }

    /**
     * Performs an AND boolean search on the inverted index.
     * Program logic:
     *      For each segment:
     *          Get the list for each keyword
     *          Get the common docID that
     *
     * @param keywords a list of keywords in the AND query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchAndQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);
        Iterator<Document> it = new Iterator<Document>() {
            private int cur_seg_num = 0;
            Iterator<Integer> list = null;
            DocumentStore ds = null;

            private boolean openDoc(){
                while(cur_seg_num < getNumSegments()){
                    if(this.ds != null){
                        this.ds.close();
                    }
                    this.ds = MapdbDocStore.createOrOpenReadOnly(indexFolder+"doc"+cur_seg_num+".db");
                    Path indexFilePath = Paths.get(indexFolder+"segment"+cur_seg_num+".seg");
                    PageFileChannel segment = PageFileChannel.createOrOpen(indexFilePath);

                    TreeMap<String,int[]> dict = indexDicDecoder(segment);
                    List<Set<Integer>> searchID = new ArrayList<>();
                    boolean find = false;

                    for(String key:keywords){
                        if(key.length()==0){continue;}
                        key = analyzer.analyze(key).get(0);
                        if(!dict.containsKey(key)){
                            find = false;
                            break;
                        }
                        if(dict.containsKey(key)){
                            Set<Integer>tmp_set = new HashSet<>(indexListDecoder(dict.get(key),segment));
                            searchID.add(tmp_set);
                            find = true;
                        }
                    }
                    if(!find) {
                        cur_seg_num++;
                        segment.close();
                        continue;
                    }

                    TreeSet<Integer> remove = new TreeSet<>();
                    for(Integer docId:searchID.get(0)) {
                        for (int i = 1; i < searchID.size(); i++) {
                            if(!searchID.get(i).contains(docId)){
                                remove.add(docId);
                                break;
                            }
                        }
                    }
                    searchID.get(0).removeAll(remove);
                    list = searchID.get(0).iterator();
                    cur_seg_num++;
                    segment.close();
                    return true;
                }
                return false;
            }

            @Override
            public boolean hasNext() {
                if(list==null||!list.hasNext()){
                    if(!openDoc()){return false;};
                }
                return list.hasNext();
            }

            @Override
            public Document next() {
                if(hasNext()){
                    return ds.getDocument(list.next());
                }
                throw new NoSuchElementException();
            }
        };
        return it;
    }

    /**
     * Performs an OR boolean search on the inverted index.
     *
     * @param keywords a list of keywords in the OR query
     * @return a iterator of documents matching the query
     */
    public Iterator<Document> searchOrQuery(List<String> keywords) {
        Preconditions.checkNotNull(keywords);

        Iterator<Document> it = new Iterator<Document>() {
            private int cur_seg_num = 0;
            Iterator<Integer> list = null;
            DocumentStore ds = null;

            private boolean openDoc(){
                while(cur_seg_num < getNumSegments()){
                    //System.out.println("Open SEG "+cur_seg_num);
                    if(this.ds != null){
                        this.ds.close();
                    }
                    this.ds = MapdbDocStore.createOrOpenReadOnly(indexFolder+"doc"+cur_seg_num+".db");
                    Path indexFilePath = Paths.get(indexFolder+"segment"+cur_seg_num+".seg");
                    PageFileChannel segment = PageFileChannel.createOrOpen(indexFilePath);

                    TreeMap<String,int[]> dict = indexDicDecoder(segment);
                    //System.out.println("Keys "+keywords.toString());
                    //System.out.println("Dict "+ dict.toString());
                    TreeSet<Integer> searchID = new TreeSet<>();
                    boolean find = false;

                    for(String key:keywords){
                        key = analyzer.analyze(key).get(0);
                        if(dict.containsKey(key)){
                            searchID.addAll(indexListDecoder(dict.get(key),segment));
                            find = true;
                        }
                    }
                    if(!find) {
                        //System.out.println("Not find it");
                        cur_seg_num++;
                        segment.close();
                        continue;
                    }
                    //System.out.println("Find it "+searchID.toString());
                    list = searchID.iterator();
                    cur_seg_num++;
                    segment.close();
                    return true;
                }
                return false;
            }

            @Override
            public boolean hasNext() {
                if(list==null||!list.hasNext()){
                    if(!openDoc()){return false;};
                }
                return list.hasNext();
            }

            @Override
            public Document next() {
                if(hasNext()){
                    return ds.getDocument(list.next());
                }
                throw new NoSuchElementException();
            }
        };
        return it;

    }

    /**
     * Iterates through all the documents in all disk segments.
     * Program logic:
     *      1. Scan all the doc file and keep read
     *
     */
    public Iterator<Document> documentIterator() {
        Iterator<Document> it = new Iterator<Document>() {
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
                throw new NoSuchElementException();
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
            //System.out.println("get segment wrong!");
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
        ds.close();
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
        int startPageNum = keyInfo[0]/PageFileChannel.PAGE_SIZE;
        int pageOffset = keyInfo[0]%PageFileChannel.PAGE_SIZE;
        int finishPageNum = startPageNum + (pageOffset + keyInfo[1])/PageFileChannel.PAGE_SIZE;
        //System.out.println("List: Offset: "+ keyInfo[0] + " Length : "+keyInfo[1]);
        //System.out.println("List: StartPage: "+ startPageNum + " pageOffset: "+pageOffset + " finishPageNum" + finishPageNum);
        ByteBuffer list_buffer = ByteBuffer.allocate((finishPageNum-startPageNum+1)*PageFileChannel.PAGE_SIZE);


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
     * @return in-memory data structure of dictionary <Key, [offset, length, which segment]>
     */

    public TreeMap<String, int[]> indexDicDecoder(PageFileChannel segment){
        /*
        File seg = new File(this.indexFolder+"segment"+segmentNum+".seg");
        if(!seg.exists()){return null;}
        Path indexFilePath = Paths.get(this.indexFolder+"segment"+segmentNum+".seg");
        PageFileChannel segment = PageFileChannel.createOrOpen(indexFilePath);
        */

        TreeMap<String, int[]> dict = new TreeMap<>();
        ByteBuffer segInfo = segment.readPage(0);

        int key_num = segInfo.getInt();
        int doc_offset = segInfo.getInt();
        int page_num = doc_offset/PageFileChannel.PAGE_SIZE;
        //System.out.println("KeyNum: "+key_num+" docOffset: "+ doc_offset + " pageNum: "+page_num);

        ByteBuffer dic_content = ByteBuffer.allocate((page_num+1)*PageFileChannel.PAGE_SIZE).put(segInfo);

        //read all the content of dictionary from disk
        for(int i =1;i<=page_num;i++){
            dic_content.put(segment.readPage(i));
        }
        dic_content.rewind();
        //loop through the dic_content to extract key
        //Format -> <key_length, key, offset, length>
        while(key_num > 0){
            int key_length =dic_content.getInt();
            //System.out.println("Read: keyLength: "+ key_length);
            byte[] str = new byte[key_length];
            dic_content.get(str);
            String tmp_key = new String(str,StandardCharsets.UTF_8);
            //System.out.println("Read: Key: "+ tmp_key);
            int[] key_info = new int[3];
            key_info[0] = dic_content.getInt();
            //System.out.println("Read: Offset: "+ key_info[0]);
            key_info[1] = dic_content.getInt();
            //System.out.println("Read: Length: "+ key_info[1]);
            dict.put(tmp_key,key_info);
            key_num--;
        }

        return dict;
    }

    /**
     * Test Functions---------------------------------------------------------
     */

    public static void setTest(){
        List<Integer> tmp = Arrays.asList(1,3,4,5,6,7,8,9,9,2,10,11,2,3,56);
        List<Integer> tmp1 = Arrays.asList(1,3,4,5,6,7,8,9,9,2,10,11,2,3,56);
        List<Integer> tmp2 = Arrays.asList(3,4,199);

        TreeSet<Integer> s = new TreeSet<>(tmp);
        s.addAll(tmp);
        s.addAll(tmp1);
        s.addAll(tmp2);
        System.out.println(s);
        s.removeAll(tmp2);
        System.out.println(s);
    }

    public static void hashMapTest(){
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

    public static void readAddBytefferTest(){
        //read from byte buffer test
        ByteBuffer tmp = ByteBuffer.allocate(17);
        tmp.putInt(5);
        tmp.put("hello".getBytes());
        tmp.putInt(10);
        tmp.putInt(10);

        String asd = "fuck’";
        System.out.println("size of asd "+ asd.getBytes(StandardCharsets.UTF_8).length);
        ByteBuffer tmp1 = ByteBuffer.allocate(12 + asd.getBytes(StandardCharsets.UTF_8).length);

        tmp1.putInt(asd.length());
        tmp1.put(asd.getBytes(StandardCharsets.UTF_8));
        tmp1.putInt(11);
        tmp1.putInt(12);

        tmp.rewind();
        //tmp.getInt();
        tmp1.rewind();

        //combine byte buffer test
        ByteBuffer tmp2 = ByteBuffer.allocate(4096);
        tmp2.put(tmp1);
        tmp2.put(tmp);

        System.out.println("position"+tmp2.position());
        System.out.println("limit"+tmp2.limit());
        tmp2.rewind();
        System.out.println("position"+tmp2.position());
        System.out.println("limit"+tmp2.limit());

        System.out.println(tmp2.getInt());
        byte[] str = new byte[asd.getBytes(StandardCharsets.UTF_8).length];
        tmp2.get(str);
        String s = new String(str, StandardCharsets.UTF_8);;
        System.out.println(s);
        System.out.println(tmp2.getInt());
        System.out.println(tmp2.getInt());
        System.out.println(tmp2.capacity());

        System.out.println("position"+tmp2.position());
        System.out.println("limit"+tmp2.limit());

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

    public static void loopBytebufferTest(){
        //ByteBuffer all = ByteBuffer.allocate(16);
        ByteBuffer tmp = ByteBuffer.allocate(16);
        tmp.putInt(5);
        tmp.putInt(6);
        tmp.putInt(7);
        tmp.putInt(8);
        tmp.rewind();
        ByteBuffer i = ByteBuffer.allocate(16);
        int size = 16;
        while(size > 0){
            int write = 1;
            if(size<1){
                write = size;
            }
            tmp.limit(tmp.position()+write);
            i.put(tmp);
            tmp.position(tmp.limit());
            size -= write;
        }
        i.rewind();
        System.out.println(i.getInt());
        System.out.println(i.getInt());

        System.out.println(i.getInt());

        System.out.println(i.getInt());

    }


    public static void main(String[] args) throws Exception {
        //setTest();
        List<String> test = Arrays.asList("hellp","world");
        String s = "";
        System.out.println(s.length());
    }

}
