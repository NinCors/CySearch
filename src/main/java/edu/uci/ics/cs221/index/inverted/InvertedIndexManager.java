package edu.uci.ics.cs221.index.inverted;

import com.google.common.base.Preconditions;
import com.google.common.collect.TreeBasedTable;
import edu.uci.ics.cs221.analysis.Analyzer;
import edu.uci.ics.cs221.storage.Document;
import edu.uci.ics.cs221.storage.DocumentStore;
import edu.uci.ics.cs221.storage.MapdbDocStore;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;

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
 *              1. Append it to hashmap with the format <word, current docID> if not exist.
 *              Otherwise, just append the current DocId to it.
 *              2. Append it to table with the format <word,docId,list<Integer>> if not exist.
 *              Otherwise, just append the current Position to it.
 *          4. Increase DocID.
 *      When the DocID reaches the DEFAULT_FLUSH_THRESHOLD -> flush():
 *          1. check if DocID = 0;
 *          2. Create the segment<x> file
 *          3. Flush it to disk:
 *              Format: dictPart: numOfKeyWord + sizeOfDictPart + dictionary(wordLength+word+offset+length) + endOffset
 *                      ListParts: invertedList + Position_Offset_list
 *             Also flush the table in to position index
 *          4. Segment number++
 *          5. Create new document store file based on Segment
 *          6. Clear hashmap
 *          7. Clear table
 *      When the number of segment is even -> merge() all:
 *          1. For segment i from segment 1 to the number of segment
 *          2. Merge segment i with segment i-1:
 *              1. Fetch the dictionaries from both segment:
 *              2. Combine two dictionaries together.
 *              3. If the keywords not equal:
 *                  Use iterator to get the data chunk:
 *                  Fetch the smaller keywords inverted and offset lists to memory,
 *                  and insert the it to the new merged segment file.
 *                  For each offset in offset list:
 *                      extract it from positional list
 *                      write it into new merged positional file
 *
 *              4. If the keywords are equal:
 *                    Use iterator to get the data chunk:
 *                      Based on the offset, length, Fetch both inverted list and merge them to one,
 *                      Based on the offset+length, next_offset,Fetch both offset list, convert the offsetnumber, and merge them to one
 *                      Use iterator to ge the data chunk of each positional index:
 *                          keep merge them into one segment
 *
 *                      then insert it to the new merged file
 *              5. Decrease the segment number when finish one pair
 *
 *      Phase search:
 *          1. get a list of common document ID from AND search
 *          2. For each common docID:
 *                  For each search key k1,k2,k3:
 *                      extract the position index of k1 in positional index:
 *                      extract the position index of k2 in positional index:
 *                          for each positional index of k1:
 *                              check if there exists an +1 in the positional index of k2
 *                              if there is
 *
 *
 * Todo:
 *      1. Compress Inverted Index
 *      2. Create Positional Index
 *      3. Compress positional Index
 *      4. Phase Search
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

    protected TreeMap<String,Integer> DocumentFrequency;

    protected boolean hasPosIndex;
    protected TreeMap<String,List<Integer>> SEGMENT_BUFFER;
    protected TreeMap<Integer,Document> DOCSTORE_BUFFER;
    protected TreeBasedTable<String, Integer, List<Integer>> POS_BUFFER;

    protected Analyzer analyzer;
    protected Compressor compressor;
    protected int docCounter;
    protected int segmentCounter;
    protected String indexFolder;

    protected InvertedIndexManager(){}

    private void init(String indexFolder, Analyzer analyzer){
        docCounter = 0;
        segmentCounter =0;
        if(indexFolder.charAt(indexFolder.length()-1) != '/'){
            indexFolder += '/';
        }
        this.indexFolder = indexFolder;
        this.DOCSTORE_BUFFER = new TreeMap<>();
        this.SEGMENT_BUFFER = new TreeMap<>();
        this.analyzer = analyzer;
        this.DocumentFrequency = new TreeMap<>();
        this.POS_BUFFER = TreeBasedTable.create();
    }

    private InvertedIndexManager(String indexFolder, Analyzer analyzer) {
        init(indexFolder,analyzer);
        this.compressor = new NaiveCompressor();
        this.hasPosIndex = false;
    }

    protected InvertedIndexManager(String indexFolder, Analyzer analyzer, Compressor compressor) {
        init(indexFolder,analyzer);
        this.compressor = compressor;
        this.hasPosIndex = true;
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
     * Creates a positional index with the given folder, analyzer, and the compressor.
     * Compressor must be used to compress the inverted lists and the position lists.
     *
     *
     *
     */
    public static InvertedIndexManager createOrOpenPositional(String indexFolder, Analyzer analyzer, Compressor compressor) {
        try {
            Path indexFolderPath = Paths.get(indexFolder);
            if (Files.exists(indexFolderPath) && Files.isDirectory(indexFolderPath)) {
                if (Files.isDirectory(indexFolderPath)) {
                    return new InvertedIndexManager(indexFolder, analyzer,compressor);
                } else {
                    throw new RuntimeException(indexFolderPath + " already exists and is not a directory");
                }
            } else {
                Files.createDirectories(indexFolderPath);
                return new InvertedIndexManager(indexFolder, analyzer,compressor);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Adds a document to the inverted index.
     *      While adding document:
     *          1. insert document to document store
     *          2. Tokenize the document and analyze it to get a list of word
     *          3. For each word in the list:
     *              1. Append it to hashmap with the format <word, current docID> if not exist.
     *              Otherwise, just append the current DocId to it.
     *              2. Append it to table with the format <word,docId,list<Integer>> if not exist.
     *              Otherwise, just append the current Position to it.
     *          4. Increase DocID.
     * Document should live in a in-memory buffer until `flush()` is called to write the segment to disk.
     * @param document
     */
    public void addDocument(Document document) {

        List<String> words = this.analyzer.analyze(document.getText());

        //System.out.println("Add document with key words " + words.toString());

        DOCSTORE_BUFFER.put(this.docCounter, document);
        HashSet<String> set = new HashSet<>();
        for(int i =0;i<words.size();i++){
            String word = words.get(i);
            if(word!="") {
                if (this.SEGMENT_BUFFER.containsKey(word)) {
                    if(!set.contains(word)) {
                        this.SEGMENT_BUFFER.get(word).add(this.docCounter);
                    }
                } else {
                    List<Integer> tmp = new ArrayList<>();
                    tmp.add(this.docCounter);
                    this.SEGMENT_BUFFER.put(word, tmp);
                }

                if(!set.contains(word)){
                    if(!this.DocumentFrequency.containsKey(word)){
                        this.DocumentFrequency.put(word,0);
                    }
                    this.DocumentFrequency.put(word,this.DocumentFrequency.get(word)+1);
                }


                if(this.POS_BUFFER.contains(word,this.docCounter)){
                    this.POS_BUFFER.get(word,this.docCounter).add(i);
                }
                else{
                    List<Integer> tmp = new ArrayList<>();
                    tmp.add(i);
                    this.POS_BUFFER.put(word,this.docCounter,tmp);
                }


            }
            set.add(word);

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
     *      When the DocID reaches the DEFAULT_FLUSH_THRESHOLD -> flush():
     *          1. check if DocID = 0;
     *          2. Create the segment<x> file
     *          3. Flush it to disk:
     *              Format: dictPart: numOfKeyWord + sizeOfDictPart + dictionary(wordLength+word+offset+length) + endOffset
     *                      ListParts: invertedList + Position_Offset_list
     *             Also flush the table in to position index
     *          4. Segment number++
     *          5. Create new document store file based on Segment
     *          6. Clear hashmap
     *          7. Clear table
     */

    public void flush(){

        if(this.docCounter == 0){
            return;
        }

        /*
        System.out.println("Current segment buffer is " + this.SEGMENT_BUFFER.toString());
        System.out.println("Current position buffer is " + this.POS_BUFFER.toString());
        System.out.println("Current Term Frequency is " + this.DocumentFrequency.toString());
        System.out.println("Current doc num is " + this.docCounter);
        */

        ByteArrayOutputStream invertedListBuffer = new ByteArrayOutputStream();
        ByteArrayOutputStream posListBuffer = new ByteArrayOutputStream();


        //get sorted key from the segment buffer
        Set<String> keys = this.SEGMENT_BUFFER.keySet();

        //calculate the estimated size of the dictionary part
        int dic_size = 12;// numberOfKey + SizeOfDictPart + numberOfDocument

        for(String key:keys){
            //KeyLength + Key + offset+ InvertedListLength + TermFrequencyListLength + DocumentFrequency
            dic_size += (20+key.getBytes(StandardCharsets.UTF_8).length);
        }

        dic_size += 4; // the end of all the file

        ByteBuffer dict_part = ByteBuffer.allocate(dic_size+PageFileChannel.PAGE_SIZE - dic_size%PageFileChannel.PAGE_SIZE);
        dict_part.putInt(keys.size());
        dict_part.putInt(dic_size);
        dict_part.putInt(this.docCounter);

        dic_size += (PageFileChannel.PAGE_SIZE - dic_size%PageFileChannel.PAGE_SIZE);
        //System.out.println("Size of dict is : " + dic_size);
        //System.out.println("Size of list start is : " + dic_size);

        int posIndexOffset = 0;

        //build the dictionary part
        //For each key, add its docID to inverted list
        //For each posID of one key and docID, add it to the positional list, and record the offset
        for(String key:keys){
            //KeyLength + Key + offset+ InvertedListLength + TermFrequencyListLength + DocumentFrequency

            try {

                List<Integer> offsetList = new ArrayList<>();
                List<Integer> docIds = this.SEGMENT_BUFFER.get(key);
                List<Integer> termFrequencyList = new ArrayList<>();
                int length = 0;


                for(Integer docId:docIds){
                    if(hasPosIndex) {
                        offsetList.add(posIndexOffset);
                        byte[] compressed_posId = this.compressor.encode(this.POS_BUFFER.get(key, docId));
                        posListBuffer.write(compressed_posId);
                        posIndexOffset += compressed_posId.length;

                        offsetList.add(posIndexOffset);
                    }
                    termFrequencyList.add(this.POS_BUFFER.get(key,docId).size());
                }

                //offsetList.add(posIndexOffset); //add the listEndOffset

                byte[] compressed_docId = this.compressor.encode(docIds);
                byte[] compressed_tfList = this.compressor.encode(termFrequencyList);
                invertedListBuffer.write(compressed_docId);
                invertedListBuffer.write(compressed_tfList);
                length += (compressed_docId.length+compressed_tfList.length);

                if(hasPosIndex) {
                    byte[] compressed_offsetList = this.compressor.encode(offsetList);
                    invertedListBuffer.write(compressed_offsetList);
                    length += compressed_offsetList.length;
                }


                dict_part.putInt(key.getBytes(StandardCharsets.UTF_8).length);
                dict_part.put(key.getBytes(StandardCharsets.UTF_8));
                dict_part.putInt(dic_size); //offset
                dict_part.putInt(compressed_docId.length);//inverted list length
                dict_part.putInt(compressed_tfList.length); //term frequency list length
                dict_part.putInt(this.DocumentFrequency.get(key));//document frequency
                dic_size+=length;

                //System.out.println(key + " : "+ offsetList.toString());
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
        dict_part.putInt(dic_size);//add the endListPos
        dict_part.rewind();
        //System.out.println("Size of list end is : " + dic_size);

        //Create the segement file
        Path indexFilePath = Paths.get(this.indexFolder+"segment"+segmentCounter+".seg");
        PageFileChannel segment = PageFileChannel.createOrOpen(indexFilePath);
        //write the dictionary part + <InvertedList + PositionOffsetList> into segement file
        segment.appendAllBytes(dict_part);
        segment.appendAllBytes(ByteBuffer.wrap(invertedListBuffer.toByteArray()));


        if(hasPosIndex) {
            //Create the positional index file
            Path posIndexFilePath = Paths.get(this.indexFolder + "posIndex" + segmentCounter + ".pos");
            PageFileChannel posIndexSeg = PageFileChannel.createOrOpen(posIndexFilePath);
            //write the positional index in to positional Index file
            posIndexSeg.appendAllBytes(ByteBuffer.wrap(posListBuffer.toByteArray()));
            posIndexSeg.close();
        }

        //write the document store file
        DocumentStore ds = MapdbDocStore.createWithBulkLoad(this.indexFolder+"doc"+this.segmentCounter+".db",this.DOCSTORE_BUFFER.entrySet().iterator());

        //Ready for next segment
        segment.close();
        ds.close();
        this.segmentCounter++;
        this.DOCSTORE_BUFFER.clear();
        this.SEGMENT_BUFFER.clear();
        this.POS_BUFFER.clear();
        this.DocumentFrequency.clear();
        this.docCounter = 0;

        try{ invertedListBuffer.close();}
        catch (Exception e){e.printStackTrace();}

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
     *              2. Combine two dictionaries together.
     *              3. If the keywords not equal:
     *                  Fetch the larger keywords lists to memory, and insert it to the new merged file
     *              4. If the keywords are equal:
     *                    Use iterator to get the data chunk:
     *                      Based on the offset, length, Fetch both inverted list and merge them to one,
     *                      Based on the offset+length, next_offset,Fetch both offset list, convert the offsetnumber, and merge them to one
     *                      Use iterator to ge the data chunk of each positional index:
     *                          keep merge them into one segment
     *
     *                      then insert it to the new merged file
     *              5. Decrease the segment number when finish one pair
     */

    public void mergeAndFlush(int segNum1,int segNum2,int mergedSegId){
        //System.out.println("Start merge with POS------------------------------------------");
        //Open two local Doc file to merge
        DocumentStore ds1 = MapdbDocStore.createOrOpen(this.indexFolder+"doc"+segNum1+".db");
        DocumentStore ds2 = MapdbDocStore.createOrOpen(this.indexFolder+"doc"+segNum2+".db");
        DocumentStore merged_ds = MapdbDocStore.createOrOpen(this.indexFolder+"doc"+segNum1+"_tmp"+".db");
        int doc_counter = 0;

        Iterator<Map.Entry<Integer, Document>> it = ds1.iterator();
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

        int offsetForPos2 = 0;

        if(hasPosIndex) {
            //Open two positional file to merge
            Path posIndexFilePath1 = Paths.get(this.indexFolder + "posIndex" + segNum1 + ".pos");
            Path posIndexFilePath2 = Paths.get(this.indexFolder + "posIndex" + segNum2 + ".pos");

            PageFileChannel posSeg1 = PageFileChannel.createOrOpen(posIndexFilePath1);
            PageFileChannel posSeg2 = PageFileChannel.createOrOpen(posIndexFilePath2);

            offsetForPos2 = posSeg1.getNumPages() * PageFileChannel.PAGE_SIZE;

            for (int i = 0; i < posSeg2.getNumPages(); i++) {
                ByteBuffer bf = posSeg2.readPage(i);
                posSeg1.appendPage(bf);
            }

            posSeg1.close();
            posSeg2.close();

            File f1 = new File(this.indexFolder+"posIndex"+segNum1+".pos");
            File f2 = new File(this.indexFolder+"posIndex"+segNum2+".pos");

            File newfile = new File(this.indexFolder+"posIndex"+mergedSegId+".pos");

            if(!f1.renameTo(newfile) || !f2.delete()){
                throw new UnsupportedOperationException();
            }
        }



        //merge inverted list
        ByteArrayOutputStream dictionaryBuffer = new ByteArrayOutputStream();
        ByteArrayOutputStream invertedListBuffer = new ByteArrayOutputStream();
        ByteArrayOutputStream positionalListBuffer = new ByteArrayOutputStream();


        //Open two segement file to merge
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
        //numberOfKey + SizeOfDictPart + numberOfDocument
        ByteBuffer segInfo = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        segInfo.put(segment1.readPage(0));
        segInfo.rewind();
        int seg1_keyNum = segInfo.getInt();
        int seg1_dicSize = segInfo.getInt();
        int seg1_docNum = segInfo.getInt();
        segInfo = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        segInfo.put(segment1.readPage(0));
        segInfo.rewind();
        int seg2_keyNum = segInfo.getInt();
        int seg2_dicSize = segInfo.getInt();
        int seg2_docNum = segInfo.getInt();

        //Build the dictionary part
        int sizeOfDictionary = 0;

        List<String> s1 =  new ArrayList<>(new TreeSet<>(dict1.keySet()));
        //HashMap<String,Integer> listsSize= new HashMap<>();
        for(int i = 0; i < s1.size()-1;i++){
            //listsSize.put(s1.get(i)+"1", dict1.get(s1.get(i+1))[0] - dict1.get(s1.get(i))[0]);
            if(dict1.get(s1.get(i)).length==1){continue;}
            dict1.get(s1.get(i))[4] = 0;

            if(merged_dict.containsKey(s1.get(i))){
                merged_dict.get(s1.get(i)).add(dict1.get(s1.get(i)));
            }

            else{
                sizeOfDictionary++;
                List<int[]>tmp = new ArrayList<>();
                tmp.add(dict1.get(s1.get(i)));
                merged_dict.put(s1.get(i),tmp);
            }
        }

        List<String> s2 =  new ArrayList<>(new TreeSet<>(dict2.keySet()));
        for(int i = 0; i < s2.size()-1;i++){
            //listsSize.put(s2.get(i)+"2", dict2.get(s2.get(i+1))[0] - dict2.get(s2.get(i))[0]);
            if(dict2.get(s2.get(i)).length==1){continue;}

            dict2.get(s2.get(i))[4] = 1;

            if(merged_dict.containsKey(s2.get(i))){
                merged_dict.get(s2.get(i)).add(dict2.get(s2.get(i)));
            }

            else{
                sizeOfDictionary++;
                List<int[]>tmp = new ArrayList<>();
                tmp.add(dict2.get(s2.get(i)));
                merged_dict.put(s2.get(i),tmp);
            }

        }

        int offset = 12;

        for(String key:merged_dict.keySet()){
            offset += (20 + key.getBytes(StandardCharsets.UTF_8).length);
        }

        offset += 4;

        //count how many page it used for offset
        int head_page_size = 1+ offset/PageFileChannel.PAGE_SIZE;
        //System.out.println("Offset is :" + offset);
        ByteBuffer dict_part = ByteBuffer.allocate((head_page_size+1)*PageFileChannel.PAGE_SIZE);

        dict_part.putInt(sizeOfDictionary);
        dict_part.putInt(offset);
        dict_part.putInt(seg1_docNum + seg2_docNum);

        int tmp_offset = offset;

        offset += (PageFileChannel.PAGE_SIZE - offset%PageFileChannel.PAGE_SIZE);

        //insert empty dictionary part
        merged_segement.appendAllBytes(ByteBuffer.allocate(offset));


        HashMap<String,List<Integer>> listSizeIndicator = new HashMap<>();

        ByteBuffer page_tmp = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);

        Iterator<List<byte[]>> chunk_it1 = this.SegmentChunkIterator(segment1);
        Iterator<List<byte[]>> chunk_it2 = this.SegmentChunkIterator(segment2);

        //offsetForPos2
        for(Map.Entry<String,List<int[]>> entry:merged_dict.entrySet()){
            //System.out.println("Start merge for " + entry.getKey());

            //Get list bytebuffer
            ByteBuffer lists = null;
            int lists_size = -1;
            int inverted_len = -1;
            int tf_len = -1;
            int documentFrequency = -1;

            //if there is only one list
            if(entry.getValue().size()==1){
                if(entry.getValue().get(0)[4] == 0){
                    //Insert k1
                    List<byte[]> k1_list = chunk_it1.next();
                    //k1_list = invertedList + offsetList + TermFrequencyList
                    inverted_len = k1_list.get(0).length;
                    tf_len = k1_list.get(2).length;
                    documentFrequency = entry.getValue().get(0)[3];
                    lists_size = inverted_len + k1_list.get(1).length + tf_len;


                    lists = ByteBuffer.allocate(lists_size);

                    lists.put(k1_list.get(0));
                    lists.put(k1_list.get(2));
                    lists.put(k1_list.get(1));

                }
                else{
                    //Insert k2
                    List<byte[]> k2_list = chunk_it2.next();

                    List<Integer> k2_invertList = compressor.decode(k2_list.get(0));
                    //update the doc id for the second segment
                    for(int i =0; i< k2_invertList.size();i++){
                        k2_invertList.set(i,k2_invertList.get(i)+doc_counter);
                    }
                    k2_list.set(0,compressor.encode(k2_invertList));

                    List<Integer> k2_offsets = compressor.decode(k2_list.get(1));
                    //update the offset value for the second segment val
                    for(int i =0; i< k2_offsets.size();i++){
                        k2_offsets.set(i,k2_offsets.get(i)+offsetForPos2);
                    }
                    k2_list.set(1,compressor.encode(k2_offsets));

                    inverted_len = k2_list.get(0).length;
                    tf_len = k2_list.get(2).length;
                    documentFrequency = entry.getValue().get(0)[3];

                    lists_size = inverted_len + k2_list.get(1).length + tf_len;

                    lists = ByteBuffer.allocate(lists_size);

                    lists.put(k2_list.get(0));
                    lists.put(k2_list.get(2));
                    lists.put(k2_list.get(1));
                }
            }
            //if there are two lists, then we need to merge them
            if(entry.getValue().size()==2){ // merged
                if(entry.getValue().get(0)[4] != 0 || entry.getValue().get(1)[4] != 1){
                    //System.out.println("wtf merged list error!!!");
                    throw new UnsupportedOperationException();
                }
                List<byte[]> k1_list = chunk_it1.next();
                List<byte[]> k2_list = chunk_it2.next();

                //1. Merge inverted List
                List<Integer> k1_invertList = compressor.decode(k1_list.get(0));
                List<Integer> k2_invertList = compressor.decode(k2_list.get(0));

                //update the doc id for the second segment
                for(int i =0; i< k2_invertList.size();i++){
                    k2_invertList.set(i,k2_invertList.get(i)+doc_counter);
                }
                k1_invertList.addAll(k2_invertList);

                //2. Merge offset List
                List<Integer> k1_offsets = compressor.decode(k1_list.get(1));
                List<Integer> k2_offsets = compressor.decode(k2_list.get(1));

                //update the offset value for the second segment val
                for(int i =0; i< k2_offsets.size();i++){
                    k2_offsets.set(i,k2_offsets.get(i)+offsetForPos2);
                }
                k1_offsets.addAll(k2_offsets);

                //3. Merge term frequency List
                List<Integer> k1_tf= compressor.decode(k1_list.get(2));
                List<Integer> k2_tf = compressor.decode(k2_list.get(2));
                k1_tf.addAll(k2_tf);

                documentFrequency = entry.getValue().get(0)[3] + entry.getValue().get(1)[3];

                /*
                System.out.println("merge k1 k2");
                System.out.println("offset "+offsetForPos2);
                System.out.println(k1_invertList.toString());
                System.out.println(k2_offsets);
                System.out.println(k1_offsets.toString());
                */

                k1_list.set(0,compressor.encode(k1_invertList));
                k1_list.set(1,compressor.encode(k1_offsets));
                k1_list.set(2,compressor.encode(k1_tf));

                //k1_list = invertedList + offsetList + TermFrequencyList
                inverted_len = k1_list.get(0).length;
                tf_len = k1_list.get(2).length;
                lists_size = inverted_len + k1_list.get(1).length + tf_len;

                lists = ByteBuffer.allocate(lists_size);

                lists.put(k1_list.get(0));
                lists.put(k1_list.get(2));
                lists.put(k1_list.get(1));

            }

            List<Integer> ls = Arrays.asList(inverted_len,lists_size, tf_len,documentFrequency);
            listSizeIndicator.put(entry.getKey(),ls);

            lists.rewind();

            // Write the lists to disk

            if(page_tmp.position()+lists_size < page_tmp.capacity()){
                page_tmp.put(lists);
            }
            else {
                int sizeForCurPage = (PageFileChannel.PAGE_SIZE - page_tmp.position());
                lists.position(0);
                lists.limit(sizeForCurPage);
                page_tmp.put(lists);

                //System.out.println("Special case: sizeForCur->" + sizeForCurPage);

                merged_segement.appendPage(page_tmp);
                page_tmp = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);


                //re-adjust the posting list
                lists.rewind();
                lists.position(sizeForCurPage);

                int sizeForNextPage = lists_size - sizeForCurPage;
                //System.out.println("Special case: sizeForNext->" + sizeForNextPage);

                while (sizeForNextPage > 0) {
                    int write_size = PageFileChannel.PAGE_SIZE;
                    if (sizeForNextPage < write_size) {
                        write_size = sizeForNextPage;
                    }
                    if (lists.position() + write_size < lists.capacity()) {
                        lists.limit(lists.position() + write_size);
                    } else {
                        lists.limit(lists.capacity());
                    }
                    //System.out.println("PostingList: position " + postingList.position() + " Limit " + postingList.limit());
                    page_tmp.put(lists);
                    if (page_tmp.position() == page_tmp.capacity()) {
                        merged_segement.appendPage(page_tmp);
                        page_tmp = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
                    }
                    lists.position(lists.limit());
                    sizeForNextPage -= write_size;
                }
            }
        }

        //If there is any remaining bytes not add into disk
        if(page_tmp.position()>0){
            merged_segement.appendPage(page_tmp);
        }

        //System.out.println("wtf");
        //System.out.println(dict_part.toString());

        for(Map.Entry<String,List<int[]>> entry:merged_dict.entrySet()){
            //Dict format: KeyLength + Key + offset+ InvertedListLength + TermFrequencyListLength + DocumentFrequency
            //listSizeIndicator: inverted_len,lists_size, tf_len, documentFrequency


            dict_part.putInt(entry.getKey().getBytes(StandardCharsets.UTF_8).length);
            dict_part.put(entry.getKey().getBytes(StandardCharsets.UTF_8));
            dict_part.putInt(offset);
            dict_part.putInt(listSizeIndicator.get(entry.getKey()).get(0));
            dict_part.putInt(listSizeIndicator.get(entry.getKey()).get(2));
            dict_part.putInt(listSizeIndicator.get(entry.getKey()).get(3));

            offset += listSizeIndicator.get(entry.getKey()).get(1);

        }
        //System.out.println(dict_part.toString());
        dict_part.putInt(offset);


        /*
        System.out.println("Merging " + segNum1 + " and " + segNum2 + " to " + mergedSegId);
        System.out.println("Dict_part info : " + dict_part.limit());
        System.out.println("Head page num : " + head_page_size);
        System.out.println("offset " + tmp_offset);
        System.out.println("number of docs " + (seg1_docNum + seg2_docNum));
        System.out.println("Size of key " + sizeOfDictionary + " : " + (seg1_keyNum + seg2_keyNum));
        */


        for(int i =0; i< head_page_size; i++){
            dict_part.rewind();
            dict_part.position(i*PageFileChannel.PAGE_SIZE);
            dict_part.limit((i+1)*PageFileChannel.PAGE_SIZE);

            //System.out.println(dict_part.capacity());
            //System.out.println(dict_part.position());
            //System.out.println(dict_part.limit());
            byte[] tmp = new byte[PageFileChannel.PAGE_SIZE];
            dict_part.get(tmp);
            merged_segement.writePage(i,ByteBuffer.wrap(tmp));
        }

        updateSegementDocFile(segNum1,mergedSegId);
        this.segmentCounter--;

    }

    public void mergeAllSegments() {
        // merge only happens at even number of segments
        Preconditions.checkArgument(getNumSegments() % 2 == 0);
        int mergedSegId = 0;
        //System.out.println("!!!!!!!Total segemetns :" + this.segmentCounter);
        int totalSeg = this.segmentCounter;
        for(int i = 1; i<totalSeg; i+=2){
            mergeAndFlush(i-1,i,mergedSegId);
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
        int numFiles = hasPosIndex? 3:2;

        if(this.segmentCounter != (filelist.length/numFiles)){
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
            if(entry.getValue().length==1){continue;}
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
     * @param keyInfo -> [offset, invertedLength, termFrequencyListLength, DocumentFrequency]
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
        List<Integer>res = new ArrayList<>();

        for(int i = startPageNum; i<=finishPageNum;i++){
            list_buffer.put(segment.readPage(i));
        }
        list_buffer.position(pageOffset);

        if(hasPosIndex) {
            byte[] bytes = new byte[keyInfo[1]];
            list_buffer.get(bytes);
            return compressor.decode(bytes,0,keyInfo[1]);
        }

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
     * @return in-memory data structure of dictionary <Key, [offset, InvertedListLength, TermFrequencyList, DocumentFrequency, which segment]>
     */

    public TreeMap<String, int[]> indexDicDecoder(PageFileChannel segment){

        TreeMap<String, int[]> dict = new TreeMap<>();
        ByteBuffer segInfo = segment.readPage(0);
        segInfo.rewind();

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
        dic_content.getInt();//get the number of doc

        //loop through the dic_content to extract key
        //Format -> <key_length, key, offset, InvertedLength, TF_List_length, DocumentFrequency>
        while(key_num > 0){
            int key_length =dic_content.getInt();
            byte[] str = new byte[key_length];
            dic_content.get(str);
            String tmp_key = new String(str,StandardCharsets.UTF_8);
            int[] key_info = new int[5];
            key_info[0] = dic_content.getInt();
            key_info[1] = dic_content.getInt();
            key_info[2] = dic_content.getInt();//TF_List_length
            key_info[3] = dic_content.getInt();//Document Frequency

            dict.put(tmp_key,key_info);
            key_num--;

            /*
            System.out.println("Read: keyLength: "+ key_length);
            System.out.println("Read: Key: "+ tmp_key);
            System.out.println("Read: Offset: "+ key_info[0]);
            System.out.println("Read: Length: "+ key_info[1]);
            */

        }


        TreeSet<String> ts = new TreeSet<>(dict.keySet());

        if(ts.size()==0){
            return dict;
        }

        int[] keyinfo = new int[1];
        keyinfo[0] = dic_content.getInt();
        //System.out.println("The last offset is " + keyinfo[0]);
        dict.put(ts.last()+"z", keyinfo);

        return dict;
    }


    /**
     * Performs a phrase search on a positional index.
     * Phrase search means the document must contain the consecutive sequence of keywords in exact order.
     *
     * You could assume the analyzer won't convert each keyword into multiple tokens.
     * Throws UnsupportedOperationException if the inverted index is not a positional index.
     *
     * 1. Find all the common Doc IDs for all the keywords
     *
     * 2. For each common Doc ID,
     *      extract its positional index, and save it into an list
     *      {k1,k2,k3,k4}
     *      For all the positional index x in k1
     *          Loop the rest of postional index of key words, and try to find the increased number x+1 +1 +1
     *      If all keywords qualified,
     *          return this Doc ID
     *
     * @param phrase, a consecutive sequence of keywords
     * @return a iterator of documents matching the query
     */

    public Iterator<Document> searchPhraseQuery(List<String> phrase) {
        Preconditions.checkNotNull(phrase);
        if(!hasPosIndex){throw new UnsupportedOperationException();}

        List<String> real_phrase = new ArrayList<>();
        for(int i = 0; i<phrase.size();i++){
            //System.out.println(phrase.toString() + " : " + analyzer.analyze(phrase.get(i)));
            if(analyzer.analyze(phrase.get(i)).size()>0) {
                real_phrase.add(analyzer.analyze(phrase.get(i)).get(0));
            }
        }

        Iterator<Document> it = new Iterator<Document>() {
            private int cur_seg_num = 0;
            Iterator<Integer> it =null;
            DocumentStore ds = null;

            private boolean openDoc(){
                while(cur_seg_num < getNumSegments()){
                    if(this.ds != null){
                        this.ds.close();
                    }

                    it = getCommonDocId(real_phrase,cur_seg_num);

                    if(it == null){
                        cur_seg_num++;
                        continue;
                    }

                    this.ds = MapdbDocStore.createOrOpenReadOnly(indexFolder+"doc"+cur_seg_num+".db");
                    cur_seg_num++;
                    return true;
                }
                return false;
            }

            @Override
            public boolean hasNext() {
                if(it==null||!it.hasNext()){
                    if(!openDoc()){return false;}
                }
                return it.hasNext();
            }

            @Override
            public Document next() {
                if(hasNext()){
                    return ds.getDocument(it.next());
                }
                throw  new NoSuchElementException();
            }
        };

        it.hasNext();
        return it;
    }


    /**
     * Get the common doc ids for the given phrase in this current segNum
     */

    public Iterator<Integer> getCommonDocId(List<String> phrase, int cur_seg_num){

        Path indexFilePath = Paths.get(indexFolder+"segment"+cur_seg_num+".seg");
        PageFileChannel segment = PageFileChannel.createOrOpen(indexFilePath);

        //Create the positional index file
        Path posIndexFilePath = Paths.get(this.indexFolder+"posIndex"+cur_seg_num+".pos");
        PageFileChannel posIndexSeg = PageFileChannel.createOrOpen(posIndexFilePath);

        TreeMap<String,int[]> dict = indexDicDecoder(segment);
        List<Set<Integer>> searchID = new ArrayList<>();

        /*
        System.out.println("For segment : " + cur_seg_num);
        System.out.println("Dict part contains "+ dict.toString());
        System.out.println("Search for "+ phrase.toString());
        */

        boolean find = true;

        for(String key:phrase){
            if(key.length()== 0){continue;}
            if(!dict.containsKey(key)){
                find = false;
                break;
            }
        }
        if(!find) {
            segment.close();
            posIndexSeg.close();
            //System.out.println(cur_seg_num + " seg : does not contain all the keys" );
            return null;
        }

        List<String> keys = new ArrayList<>( new TreeSet(dict.keySet()));

        TreeMap<String,List<List<Integer>>> keyPostingList = new TreeMap<>();
        // keyPostingList: <Key, <InvertedIndex, offsetList>>

        for(String key:phrase){
            if(key.length()== 0){continue;}
            String next_key = keys.get(keys.indexOf(key)+1);
            keyPostingList.put(key, getOneDataChunk(dict.get(key),dict.get(next_key)[0],segment));
        }

        TreeSet<Integer> remove = new TreeSet<>();
        //System.out.println("Key positing list is : " + keyPostingList.toString());
        //System.out.println("search phase is " + phrase.toString());
        TreeSet<Integer> commonDocId = new TreeSet<>(keyPostingList.get(phrase.get(0)).get(0));


        for(Integer docId: commonDocId){
            for (int i = 1; i < phrase.size(); i++) {
                if(!keyPostingList.get(phrase.get(i)).get(0).contains(docId)){
                    remove.add(docId);
                }
            }
        }

        commonDocId.removeAll(remove);

        //System.out.println("The common doc id for the seg "+cur_seg_num + " is " + commonDocId.toString());

        TreeSet<Integer> res = new TreeSet<>();
        for(Integer docId: commonDocId){
            // Get the real positional index for this document
            // keyPostingList: <Key, <InvertedIndex, offsetList>>
            List<Set<Integer>> posIndexs = new ArrayList<>();
            for(String key:phrase){
                int pos = keyPostingList.get(key).get(0).indexOf(docId);
                int start = keyPostingList.get(key).get(1).get(pos*2);
                int end = keyPostingList.get(key).get(1).get(pos*2+1);
                posIndexs.add(new HashSet<>(decodePositionalIndex(start,end,posIndexSeg)));
            }

            //System.out.println("For docID : "+ docId);
            //System.out.println(posIndexs);

            boolean rightPos = true;

            for(Integer pos: posIndexs.get(0)){
                rightPos = true;
                for(int i = 1; i<posIndexs.size();i++){
                    if(!posIndexs.get(i).contains(pos+i)){
                        rightPos = false;
                        break;
                    }
                }
                if(rightPos){break;}
            }

            if(rightPos){
                res.add(docId);
            }
        }

        segment.close();
        posIndexSeg.close();

        if(res.size() == 0){
            //System.out.println("Have common id but no right position");
            return null;
        }

        return res.iterator();
    }


    public List<List<Integer>> getOneDataChunk(int[] keyInfo, int end, PageFileChannel segment) {
        int startPageNum = keyInfo[0]/PageFileChannel.PAGE_SIZE;
        int pageOffset = keyInfo[0]%PageFileChannel.PAGE_SIZE;

        int finishPageNum = end/PageFileChannel.PAGE_SIZE;

        ByteBuffer dataChunk = ByteBuffer.allocate((finishPageNum-startPageNum+1)*PageFileChannel.PAGE_SIZE);

        for(int i = startPageNum; i<=finishPageNum;i++) {
            dataChunk.put(segment.readPage(i));
        }

        dataChunk.position(pageOffset);
        dataChunk.limit(pageOffset+(end-keyInfo[0]));

        byte[] invertedList = new byte[keyInfo[1]]; // get the inverted list
        byte[] tfList = new byte[keyInfo[2]];
        byte[] offsetList = new byte[end-keyInfo[0] - keyInfo[1] - keyInfo[2]]; //get the offset list
        dataChunk.get(invertedList);
        dataChunk.get(tfList);
        dataChunk.get(offsetList);
        return Arrays.asList(compressor.decode(invertedList),compressor.decode(offsetList), compressor.decode(tfList));
    }


    /**
     * Reads a disk segment of a positional index into memory based on segmentNum.
     * This function is mainly used for checking correctness in test cases.
     *
     * Throws UnsupportedOperationException if the inverted index is not a positional index.
     *
     * @param segmentNum n-th segment in the inverted index (start from 0).
     * @return in-memory data structure with all contents in the index segment, null if segmentNum don't exist.
     */

    public PositionalIndexSegmentForTest getIndexSegmentPositional(int segmentNum) {
        File seg = new File(indexFolder+"segment"+segmentNum+".seg");
        File doc = new File(indexFolder+"doc"+segmentNum+".db");
        File pos = new File(this.indexFolder+"posIndex"+segmentNum+".pos");

        if (!doc.exists()||!seg.exists()||!pos.exists()) {
            System.out.println("No file?");
            return null;}

        Path indexFilePath = Paths.get(this.indexFolder+"segment"+segmentNum+".seg");
        PageFileChannel segment = PageFileChannel.createOrOpen(indexFilePath);
        Path posFilePath = Paths.get(this.indexFolder+"posIndex"+segmentNum+".pos");
        PageFileChannel posSeg = PageFileChannel.createOrOpen(posFilePath);


        Iterator<List<byte[]>> segmentIterator = SegmentChunkIterator(segment);


        TreeMap<String, List<Integer>> invertedLists = new TreeMap<>();
        TreeMap<Integer, Document> documents = new TreeMap<>();
        TreeBasedTable<String, Integer, List<Integer>> positions = TreeBasedTable.create();

        TreeMap<String,int[]> dict = indexDicDecoder(segment);
        TreeSet<String> dict_set = new TreeSet<>(dict.keySet());

        //System.out.println("Start decoding the whole shit !");
        for(String key:dict_set){
            if(dict.get(key).length == 1){continue;}
            List<byte[]> chunk = segmentIterator.next();
            List<Integer> inverList = compressor.decode(chunk.get(0));
            invertedLists.put(key,inverList);
            List<Integer> offsetList = compressor.decode(chunk.get(1));

            /*
            System.out.println("-------------------" );
            System.out.println("For key : " + key );
            System.out.println("Inverted index :" + inverList.toString());
            System.out.println("Offset Index : is " + offsetList.toString());
            System.out.println("OFFset size is : " + offsetList.size());
            */

            Iterator<List<Integer>> posIndexIter = posIndexIteratorForOneKey(posSeg,offsetList);

            for(int i =0; i< inverList.size(); i++){
                //List<Integer> posIndex = decodePositionalIndex(offsetList.get(i*2),offsetList.get(i*2+1),posSeg);
                //System.out.println("Doc: " + inverList.get(i)+" Positional Index : is " + posIndex.toString());
                positions.put(key,inverList.get(i),posIndexIter.next());
                //positions.put(key,inverList.get(i),posIndex);

            }
        }
        segment.close();
        posSeg.close();

        DocumentStore ds = MapdbDocStore.createOrOpen(indexFolder+"doc"+segmentNum+".db");
        Iterator<Map.Entry<Integer,Document>> it = ds.iterator();

        while(it.hasNext()){
            Map.Entry<Integer,Document> entry =it.next();
            documents.put(entry.getKey(),entry.getValue());
        }
        ds.close();

        return new PositionalIndexSegmentForTest(invertedLists,documents,positions);
    }

    public List<Integer> decodePositionalIndex(int start, int end, PageFileChannel segment){
        //System.out.println("Decoding "+start + " "+ end);
        List<Integer> res = new ArrayList<>();

        int startPageNum = start/PageFileChannel.PAGE_SIZE;
        int pageOffset = start%PageFileChannel.PAGE_SIZE;
        int finishPageNum = end/PageFileChannel.PAGE_SIZE;


        ByteBuffer list_buffer = ByteBuffer.allocate((finishPageNum-startPageNum+1)*PageFileChannel.PAGE_SIZE);

        for(int i = startPageNum; i<=finishPageNum;i++){
            list_buffer.put(segment.readPage(i));
        }

        list_buffer.position(pageOffset);

        byte[] bytes = new byte[end-start];
        list_buffer.get(bytes);
        return compressor.decode(bytes,0,end-start);
    }

    /**
     * Keep return the posIndex for one docID
     */
    public Iterator<List<Integer>> posIndexIteratorForOneKey(PageFileChannel segment, List<Integer> offsetList){
        Iterator<List<Integer>> it = new Iterator<List<Integer>>() {
            int prePageNum = -1;
            ByteBuffer prePage = null;

            Iterator<Integer> it = null;

            private boolean init(){
                it = offsetList.iterator();
                if(!it.hasNext()){return false;}

                prePage = segment.readPage(0);
                prePageNum = 0;

                return true;
            }

            @Override
            public boolean hasNext() {
                if(it == null){
                    if(!init()){
                        return false;
                    }
                }
                return it.hasNext();

            }

            @Override
            public List<Integer> next() {
                if(!hasNext()){return null;}

                int start = it.next();
                int end = it.next();

                int startPageNum = start/PageFileChannel.PAGE_SIZE;
                int pageOffset = start%PageFileChannel.PAGE_SIZE;

                int finishPageNum = end/PageFileChannel.PAGE_SIZE;

                ByteBuffer dataChunk = ByteBuffer.allocate((finishPageNum-startPageNum+1)*PageFileChannel.PAGE_SIZE);

                if(startPageNum == prePageNum){
                    dataChunk.put(prePage);
                    startPageNum++;
                }

                for(int i = startPageNum; i<=finishPageNum;i++){
                    prePage = segment.readPage(i);
                    prePage.rewind();
                    dataChunk.put(prePage);
                }

                dataChunk.position(pageOffset);
                dataChunk.limit(pageOffset+(end-start));

                byte[] positionalIndex = new byte[end-start];

                dataChunk.get(positionalIndex);

                prePageNum = finishPageNum;
                prePage.rewind();

                return compressor.decode(positionalIndex);
            }
        };

        return it;
    }


    /**
     * Keep return the data chunk of one key in one segment
     * Get the dictionary of one segement first
     *
     * @param segment n-th segment in the inverted index to be loop
     * @return byte[], the data chunk that contains <InvertedList, offsetList>
     */

    public Iterator<List<byte[]>> SegmentChunkIterator(PageFileChannel segment) {
        Iterator<List<byte[]>> it = new Iterator<List<byte[]>>() {
            int prePageNum=-1;
            ByteBuffer prePage = null;
            //TreeMap<String,int[]> dict = null;
            Iterator<Map.Entry<String,int[]>> it = null;
            Map.Entry<String,int[]> pre = null;
            Map.Entry<String,int[]> cur = null;

            public void init(){
                it = indexDicDecoder(segment).entrySet().iterator();
                prePage = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
                if(it.hasNext()) {
                    pre = it.next();
                }
                prePageNum = pre.getValue()[0]/PageFileChannel.PAGE_SIZE;
                prePage = segment.readPage(prePageNum);
                prePage.rewind();
            }

            @Override
            public boolean hasNext() {
                if(it ==null){
                    init();
                }
                if(it.hasNext()){
                    return true;
                }
                return false;
            }

            @Override
            public List<byte[]> next() {
                if(!hasNext()){return null;}

                /**
                 * Pre: offset_pre + length
                 * Cur: offset_cur + length
                 * Extract the datachunk between offset_pre to offset_cur
                 */
                cur = it.next();
                //System.out.println("Cur "+cur.toString()+" "+ cur.getValue()[0]);
                //System.out.println("Pre "+pre.toString()+ " " + pre.getValue()[0]);
                int startPageNum = pre.getValue()[0]/PageFileChannel.PAGE_SIZE;
                int pageOffset = pre.getValue()[0]%PageFileChannel.PAGE_SIZE;

                int finishPageNum = cur.getValue()[0]/PageFileChannel.PAGE_SIZE;

                ByteBuffer dataChunk = null;
                try {
                     dataChunk = ByteBuffer.allocate((finishPageNum - startPageNum + 1) * PageFileChannel.PAGE_SIZE).put(prePage);
                }
                catch (Exception e){
                    e.printStackTrace();
                    System.out.println("Cur "+cur.toString()+" "+ cur.getValue()[0]);
                    System.out.println("Pre "+pre.toString()+ " " + pre.getValue()[0]);
                    System.out.println("FinishPageNum "+finishPageNum);
                    System.out.println("StartPage " + startPageNum);
                    throw new UnsupportedOperationException();
                }

                for(int i = startPageNum+1; i<=finishPageNum;i++){
                    prePage = segment.readPage(i);
                    prePage.rewind();
                    dataChunk.put(prePage);
                }
                //the target data are in the range[page offset, page offset + length]
                //length = offset_cur - offset_pre
                dataChunk.position(pageOffset);
                dataChunk.limit(pageOffset+(cur.getValue()[0]-pre.getValue()[0]));

                byte[] invertedList = new byte[pre.getValue()[1]]; // get the inverted list
                byte[] tfList = new byte[pre.getValue()[2]];
                byte[] offsetList = new byte[cur.getValue()[0] - (pre.getValue()[0] + pre.getValue()[1]+pre.getValue()[2])]; //get the offset list
                dataChunk.get(invertedList);
                dataChunk.get(tfList);
                dataChunk.get(offsetList);

                pre = cur;
                prePage.rewind();

                return Arrays.asList(invertedList,offsetList,tfList);

            }

        };

        return it;

    }



    public int getNumDocuments(int segmentNum){
        Path indexFilePath = Paths.get(this.indexFolder+"segment"+segmentNum+".seg");
        PageFileChannel segment = PageFileChannel.createOrOpen(indexFilePath);
        ByteBuffer segInfo = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);
        segInfo.put(segment.readPage(0));
        segment.close();

        segInfo.rewind();
        segInfo.getInt();
        segInfo.getInt();
        return segInfo.getInt();
    }

    public int getDocumentFrequency(int segmentNum, String token){
        Path indexFilePath = Paths.get(this.indexFolder+"segment"+segmentNum+".seg");
        PageFileChannel segment = PageFileChannel.createOrOpen(indexFilePath);
        TreeMap<String, int[]> dict = indexDicDecoder(segment);
        segment.close();

        if(dict.containsKey(token)){
            return dict.get(token)[3];
        }
        return 0;
    }


    /**
     * Performs top-K ranked search using TF-IDF.
     * Returns an iterator that returns the top K documents with highest TF-IDF scores.
     *
     * Each element is a pair of <Document, Double (TF-IDF Score)>.
     *
     * If parameter `topK` is null, then returns all the matching documents.
     *
     * Unlike Boolean Query and Phrase Query where order of the documents doesn't matter,
     * for ranked search, order of the document returned by the iterator matters.
     *
     * @param keywords, a list of keywords in the query
     * @param topK, number of top documents weighted by TF-IDF, all documents if topK is null
     * @return a iterator of top-k ordered documents matching the query
     */
    public Iterator<Pair<Document, Double>> searchTfIdf(List<String> keywords, Integer topK) {
        Preconditions.checkNotNull(keywords);

        List<String> real_phrase = new ArrayList<>();
        for (int i = 0; i < keywords.size(); i++) {
            //System.out.println(phrase.toString() + " : " + analyzer.analyze(phrase.get(i)));
            if (analyzer.analyze(keywords.get(i)).size() > 0) {
                real_phrase.add(analyzer.analyze(keywords.get(i)).get(0));
            }
        }

        //System.out.println("Searching for the " + real_phrase.toString());

        Iterator<Pair<Document,Double>> it = new Iterator<Pair<Document, Double>>() {
            boolean first = true;
            Iterator<Pair<Pair<Integer,Integer>, Double>> score_it = null;

            public void start(){
                score_it = firstSecondPass(real_phrase,topK).iterator();
            }

            @Override
            public boolean hasNext() {
                if(first){
                    start();
                    first = false;
                }
                return score_it.hasNext();
            }

            @Override
            public Pair<Document, Double> next() {
                if(hasNext()){
                    Pair<Pair<Integer,Integer>, Double> cur = score_it.next();
                    Pair<Document, Double> res = new Pair(getDocument(cur.getLeft()),cur.getRight());
                    return res;
                }
                return null;
            }
        };


        return it;
    }

    public Document getDocument(Pair<Integer,Integer> docId){
        DocumentStore ds = MapdbDocStore.createOrOpen(this.indexFolder+"doc"+docId.getLeft()+".db");
        Document res = ds.getDocument((int)docId.getRight());
        ds.close();
        return res;
    }


    public void printDict(TreeMap<String, int[]> dict){
        Set<String> keys = dict.keySet();

        for(String key:keys){
            System.out.println("Key : " + key);
            System.out.println("Offset " + dict.get(key)[0]);
            System.out.println("Inverted length " + dict.get(key)[1]);
            System.out.println("tf list length " + dict.get(key)[2]);
            System.out.println("Document Frequency " + dict.get(key)[3]);
            System.out.println("------------------------------");
        }
    }


    public List<Pair<Pair<Integer,Integer>, Double>>firstSecondPass(List<String> keywords, Integer topK){
        int segNum = getNumSegments();

        //System.out.println("Total Seg number: "+segNum);
        //System.out.println("Search keywords are "+ keywords.toString());

        //First pass, compute the global idf score for each keyword
        int totalDocNum = 0;
        HashMap<String, Double> idfs = new HashMap<>();

        HashSet<String> no_duplicate_keys = new HashSet<>(keywords);

        List<Pair<Pair<Integer,Integer>, Double>> score = new ArrayList<>(); // <DocID, tfidf_score>


        for(int i = 0; i < segNum; i++){
            totalDocNum += getNumDocuments(i);

            //System.out.println("----------------------");
            //System.out.println("For seg-"+i);

            Path indexFilePath = Paths.get(this.indexFolder+"segment"+i+".seg");
            PageFileChannel segment = PageFileChannel.createOrOpen(indexFilePath);

            TreeMap<String, int[]> dict = indexDicDecoder(segment);
            //printDict(dict);
            segment.close();

            for(String key:no_duplicate_keys){
                if(dict.containsKey(key)){
                    if(!idfs.containsKey(key)){
                        idfs.put(key,0.0);
                    }
                    idfs.put(key,idfs.get(key)+dict.get(key)[3]);
                }
            }
        }

        //System.out.println("Total Num of docs " + totalDocNum);
        //System.out.println("IDFS are " + idfs.toString());


        Iterator<Map.Entry<String,Double>> it = idfs.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry<String,Double> tmp = it.next();
            idfs.put(tmp.getKey(),Math.log10(totalDocNum/idfs.get(tmp.getKey())));
        }

        //System.out.println("IDFS are " + idfs.toString());

        if(idfs.size()==0){
            return score;
        }

        //Calculate query tf-idf
        HashMap<String,Double> queryTfIdf = new HashMap<>();
        for(String key:keywords){
            if(!queryTfIdf.containsKey(key)){
                queryTfIdf.put(key,0.0);
            }
            queryTfIdf.put(key,queryTfIdf.get(key)+1);
        }

        Iterator<Map.Entry<String,Double>> it_query = queryTfIdf.entrySet().iterator();
        while(it_query.hasNext()){
            Map.Entry<String,Double> tmp = it_query.next();
            double x = queryTfIdf.get(tmp.getKey())*idfs.get(tmp.getKey());
            queryTfIdf.put(tmp.getKey(),queryTfIdf.get(tmp.getKey())*idfs.get(tmp.getKey()));
        }

        //System.out.println("Query idf is "+ queryTfIdf.toString());

        //Second Pass
        Map<Pair<Integer, Integer>, Double> dotProductAccumulator = new HashMap<>(); //  DocID is <SegmentID, LocalDocID> Double is TF-IDF Score
        Map<Pair<Integer, Integer>, Double> vectorLengthAccumulator = new HashMap<>();

        Pair pair = new Pair(-1,-1);

        //System.out.println("Second pass start");

        for(int i = 0; i < segNum;i++){
            Path indexFilePath = Paths.get(this.indexFolder+"segment"+i+".seg");
            PageFileChannel segment = PageFileChannel.createOrOpen(indexFilePath);
            TreeMap<String, int[]> dict = indexDicDecoder(segment);

            TreeSet<Integer> docIds = new TreeSet<>();

            for(String key:no_duplicate_keys){
                if(dict.containsKey(key)){
                    List<List<Integer>> localIdfInfo = getLocalTfInfo(dict.get(key),segment);
                    if(localIdfInfo.get(0).size() != localIdfInfo.get(1).size()){
                        System.out.println("Local idf information is worrrong!!!!");
                    }

                    //loop all the docID on the posting list of key
                    for(int w = 0; w< localIdfInfo.get(0).size();w++){
                        pair = pair.of(i,localIdfInfo.get(0).get(w));//SegID, DocId
                        docIds.add(localIdfInfo.get(0).get(w));

                        if(!dotProductAccumulator.containsKey(pair)){
                            dotProductAccumulator.put(pair,0.0);
                        }
                        if(!vectorLengthAccumulator.containsKey(pair)){
                            vectorLengthAccumulator.put(pair,0.0);
                        }

                        double tfidf = localIdfInfo.get(1).get(w) * idfs.get(key);
                        dotProductAccumulator.put(pair, dotProductAccumulator.get(pair)+(tfidf*queryTfIdf.get(key)));
                        vectorLengthAccumulator.put(pair,vectorLengthAccumulator.get(pair)+(tfidf*tfidf));
                    }
                }
            }

            /*
            System.out.println("Collected docID "+ docIds.toString() );
            System.out.println(dotProductAccumulator.toString());
            System.out.println(vectorLengthAccumulator.toString());
             */

            for(Integer docId:docIds){
                Pair id = pair.of(i,docId);
                if(vectorLengthAccumulator.get(id) == 0){
                    pair = pair.of(id,0.0);
                }
                else {
                    pair = pair.of(id, dotProductAccumulator.get(id) / Math.sqrt(vectorLengthAccumulator.get(id)));
                }
                score.add(pair);
            }
        }

        //sort the result
        Collections.sort(score, new Comparator< Pair<Pair<Integer,Integer>, Double>>() {
            @Override
            public int compare( Pair<Pair<Integer,Integer>, Double> p1,  Pair<Pair<Integer,Integer>, Double> p2) {
                return Double.compare(p2.getRight(),p1.getRight());
            }
        });

        //System.out.println("Sorted scores are " + score.toString());


        int size = score.size();
        if(topK==null || size<topK) {
            return score;
        }
        return score.subList(0, topK);
    }


    public List<List<Integer>> getLocalTfInfo(int[] keyInfo,PageFileChannel segment) {
        //KeyInfo: offset, InvertedLength, TF_List_length, DocumentFrequency
        int startPageNum = keyInfo[0]/PageFileChannel.PAGE_SIZE;
        int pageOffset = keyInfo[0]%PageFileChannel.PAGE_SIZE;

        int end = keyInfo[0] + keyInfo[1] + keyInfo[2];

        int finishPageNum = end/PageFileChannel.PAGE_SIZE;

        ByteBuffer dataChunk = ByteBuffer.allocate((finishPageNum-startPageNum+1)*PageFileChannel.PAGE_SIZE);

        for(int i = startPageNum; i<=finishPageNum;i++) {
            dataChunk.put(segment.readPage(i));
        }

        dataChunk.position(pageOffset);
        dataChunk.limit(pageOffset+(end-keyInfo[0]));

        byte[] invertedList = new byte[keyInfo[1]]; // get the inverted list
        byte[] tfList = new byte[keyInfo[2]];

        dataChunk.get(invertedList);
        dataChunk.get(tfList);

        return Arrays.asList(compressor.decode(invertedList),compressor.decode(tfList));
    }



    public static void main(String[] args) throws Exception {
        String x = "zübeck";

        String y = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";

        TreeSet<String> ts = new TreeSet<>();

        ts.add(x);
        ts.add(y);

        System.out.println(ts.last());
    }

}
