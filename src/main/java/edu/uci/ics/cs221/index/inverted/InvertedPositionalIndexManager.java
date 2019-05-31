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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class InvertedPositionalIndexManager extends InvertedIndexManager {

    protected InvertedPositionalIndexManager(String indexFolder, Analyzer analyzer, Compressor compressor) {
        docCounter = 0;
        segmentCounter =0;
        if(indexFolder.charAt(indexFolder.length()-1) != '/'){
            indexFolder += '/';
        }
        this.indexFolder = indexFolder;
        this.DOCSTORE_BUFFER = new TreeMap<>();
        this.SEGMENT_BUFFER = new TreeMap<>();
        this.analyzer = analyzer;
        this.compressor = compressor;
        this.POS_BUFFER = TreeBasedTable.create();
        this.hasPosIndex = true;
    }

    @Override
    public void flush(){

        if(this.docCounter == 0){
            return;
        }

        //System.out.println("Current segment buffer is " + this.SEGMENT_BUFFER.toString());
        //System.out.println("Current position buffer is " + this.POS_BUFFER.toString());


        ByteArrayOutputStream invertedListBuffer = new ByteArrayOutputStream();
        ByteArrayOutputStream posListBuffer = new ByteArrayOutputStream();

        //Create the segement file
        Path indexFilePath = Paths.get(this.indexFolder+"segment"+segmentCounter+".seg");
        PageFileChannel segment = PageFileChannel.createOrOpen(indexFilePath);

        //Create the positional index file
        Path posIndexFilePath = Paths.get(this.indexFolder+"posIndex"+segmentCounter+".pos");
        PageFileChannel posIndexSeg = PageFileChannel.createOrOpen(posIndexFilePath);

        //get sorted key from the segment buffer
        Set<String> keys = this.SEGMENT_BUFFER.keySet();

        //calculate the estimated size of the dictionary part
        int dic_size = 8;// numberOfKey + SizeOfDictPart

        for(String key:keys){
            dic_size += (12+key.getBytes(StandardCharsets.UTF_8).length);//offset,listLength,keyLength+real key;
        }

        dic_size += 4; // the end of all the file

        ByteBuffer dict_part = ByteBuffer.allocate(dic_size+PageFileChannel.PAGE_SIZE - dic_size%PageFileChannel.PAGE_SIZE);
        dict_part.putInt(keys.size());
        dict_part.putInt(dic_size);

        dic_size += (PageFileChannel.PAGE_SIZE - dic_size%PageFileChannel.PAGE_SIZE);
        //System.out.println("Size of dict is : " + dic_size);
        //System.out.println("Size of list start is : " + dic_size);

        int posIndexOffset = 0;

        //build the dictionary part
        //For each key, add its docID to inverted list
        //For each posID of one key and docID, add it to the positional list, and record the offset
        for(String key:keys){
            try {

                List<Integer> offsetList = new ArrayList<>();
                List<Integer> docIds = this.SEGMENT_BUFFER.get(key);


                for(Integer docId:docIds){
                    offsetList.add(posIndexOffset);
                    byte[] compressed_posId = this.compressor.encode(this.POS_BUFFER.get(key,docId));
                    posListBuffer.write(compressed_posId);
                    posIndexOffset += compressed_posId.length;
                    offsetList.add(posIndexOffset);

                }

                //offsetList.add(posIndexOffset); //add the listEndOffset

                byte[] compressed_docId = this.compressor.encode(docIds);
                byte[] compressed_offsetList = this.compressor.encode(offsetList);
                invertedListBuffer.write(compressed_docId);
                invertedListBuffer.write(compressed_offsetList);


                dict_part.putInt(key.getBytes(StandardCharsets.UTF_8).length);
                dict_part.put(key.getBytes(StandardCharsets.UTF_8));
                dict_part.putInt(dic_size);
                dict_part.putInt(compressed_docId.length);//only save the length of docID list
                dic_size+=(compressed_docId.length+compressed_offsetList.length);

                //System.out.println(key + " : "+ offsetList.toString());
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
        dict_part.putInt(dic_size);//add the endListPos
        dict_part.rewind();
        //System.out.println("Size of list end is : " + dic_size);

        //write the dictionary part + <InvertedList + PositionOffsetList> into segement file
        segment.appendAllBytes(dict_part);
        segment.appendAllBytes(ByteBuffer.wrap(invertedListBuffer.toByteArray()));


        //write the positional index in to positional Index file
        posIndexSeg.appendAllBytes(ByteBuffer.wrap(posListBuffer.toByteArray()));


        //write the document store file
        DocumentStore ds = MapdbDocStore.createWithBulkLoad(this.indexFolder+"doc"+this.segmentCounter+".db",this.DOCSTORE_BUFFER.entrySet().iterator());

        //Ready for next segment
        segment.close();
        ds.close();
        posIndexSeg.close();
        this.segmentCounter++;
        this.DOCSTORE_BUFFER.clear();
        this.SEGMENT_BUFFER.clear();
        this.POS_BUFFER.clear();
        this.docCounter = 0;
        try{ invertedListBuffer.close();}
        catch (Exception e){e.printStackTrace();}

        if(this.segmentCounter == this.DEFAULT_MERGE_THRESHOLD){
            mergeAllSegments();
        }

    }


    @Override
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

        //Open two positional file to merge
        Path posIndexFilePath1 = Paths.get(this.indexFolder+"posIndex"+segNum1+".pos");
        Path posIndexFilePath2 = Paths.get(this.indexFolder+"posIndex"+segNum2+".pos");

        PageFileChannel posSeg1 = PageFileChannel.createOrOpen(posIndexFilePath1);
        PageFileChannel posSeg2 = PageFileChannel.createOrOpen(posIndexFilePath2);

        int offsetForPos2 = posSeg1.getNumPages()*PageFileChannel.PAGE_SIZE;

        for(int i = 0; i<posSeg2.getNumPages();i++){
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
        int offset = seg1_offset+seg2_offset-8-4;//get rid of the duplicated metainfo


        List<String> s1 =  new ArrayList<>(new TreeSet<>(dict1.keySet()));
        //HashMap<String,Integer> listsSize= new HashMap<>();
        for(int i = 0; i < s1.size()-1;i++){
            //listsSize.put(s1.get(i)+"1", dict1.get(s1.get(i+1))[0] - dict1.get(s1.get(i))[0]);

            dict1.get(s1.get(i))[2] = 0;

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
            dict2.get(s2.get(i))[2] = 1;

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

        //count how many page it used for offset
        int head_page_size = 1+ offset/PageFileChannel.PAGE_SIZE;
        //System.out.println("Offset is :" + offset);
        ByteBuffer dict_part = ByteBuffer.allocate((head_page_size+1)*PageFileChannel.PAGE_SIZE);

        dict_part.putInt(sizeOfDictionary);
        dict_part.putInt(offset);

        offset += (PageFileChannel.PAGE_SIZE - offset%PageFileChannel.PAGE_SIZE);

        //insert empty dictionary part
        merged_segement.appendAllBytes(ByteBuffer.allocate(offset));


        HashMap<String,List<Integer>> listSize = new HashMap<>();

        ByteBuffer page_tmp = ByteBuffer.allocate(PageFileChannel.PAGE_SIZE);

        Iterator<List<byte[]>> chunk_it1 = this.SegmentChunkIterator(segment1);
        Iterator<List<byte[]>> chunk_it2 = this.SegmentChunkIterator(segment2);

        //offsetForPos2

        for(Map.Entry<String,List<int[]>> entry:merged_dict.entrySet()){
            //System.out.println("Start merge for " + entry.getKey());

            ByteBuffer lists = null;
            int lists_size = -1;
            int inverted_len = -1;

            if(entry.getValue().size()==1){
                if(entry.getValue().get(0)[2] == 0){
                    //Insert k1
                    List<byte[]> k1_list = chunk_it1.next();
                    lists_size = k1_list.get(0).length + k1_list.get(1).length;
                    inverted_len = k1_list.get(0).length;
                    lists = ByteBuffer.allocate(lists_size);

                    lists.put(k1_list.get(0));
                    lists.put(k1_list.get(1));

                }
                else{
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

                    lists_size = k2_list.get(0).length + k2_list.get(1).length;
                    inverted_len = k2_list.get(0).length;
                    lists = ByteBuffer.allocate(lists_size);

                    lists.put(k2_list.get(0));
                    lists.put(k2_list.get(1));
                }
            }
            if(entry.getValue().size()==2){ // merged
                if(entry.getValue().get(0)[2] != 0 || entry.getValue().get(1)[2] != 1){
                    //System.out.println("wtf merged list error!!!");
                    throw new UnsupportedOperationException();
                }
                List<byte[]> k1_list = chunk_it1.next();
                List<byte[]> k2_list = chunk_it2.next();

                List<Integer> k1_invertList = compressor.decode(k1_list.get(0));
                List<Integer> k2_invertList = compressor.decode(k2_list.get(0));
                //update the doc id for the second segment
                for(int i =0; i< k2_invertList.size();i++){
                    k2_invertList.set(i,k2_invertList.get(i)+doc_counter);
                }
                k1_invertList.addAll(k2_invertList);

                List<Integer> k1_offsets = compressor.decode(k1_list.get(1));
                List<Integer> k2_offsets = compressor.decode(k2_list.get(1));
                //update the offset value for the second segment val
                for(int i =0; i< k2_offsets.size();i++){
                    k2_offsets.set(i,k2_offsets.get(i)+offsetForPos2);
                }
                k1_offsets.addAll(k2_offsets);

                /*
                System.out.println("merge k1 k2");
                System.out.println("offset "+offsetForPos2);
                System.out.println(k1_invertList.toString());
                System.out.println(k2_offsets);
                System.out.println(k1_offsets.toString());
                */


                k1_list.set(0,compressor.encode(k1_invertList));
                k1_list.set(1,compressor.encode(k1_offsets));

                lists_size = k1_list.get(0).length + k1_list.get(1).length;
                inverted_len = k1_list.get(0).length;

                lists = ByteBuffer.allocate(lists_size);

                lists.put(k1_list.get(0));
                lists.put(k1_list.get(1));

            }

            List<Integer> ls = Arrays.asList(inverted_len,lists_size);
            listSize.put(entry.getKey(),ls);

            lists.rewind();

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
            //try {
            //System.out.println(entry.getKey()+ " : " + dict_part.toString());

            dict_part.putInt(entry.getKey().getBytes(StandardCharsets.UTF_8).length);
            dict_part.put(entry.getKey().getBytes(StandardCharsets.UTF_8));
            dict_part.putInt(offset);

            //compute the length of new list
            dict_part.putInt(listSize.get(entry.getKey()).get(0));

            offset += listSize.get(entry.getKey()).get(1);

            //catch (Exception e){
            //  e.printStackTrace();
            //}
        }
        //System.out.println(dict_part.toString());
        dict_part.putInt(offset);


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
    @Override
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
        byte[] offsetList = new byte[end-keyInfo[0] - keyInfo[1]]; //get the offset list
        dataChunk.get(invertedList);
        dataChunk.get(offsetList);
        return Arrays.asList(compressor.decode(invertedList),compressor.decode(offsetList));
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

                ByteBuffer dataChunk = ByteBuffer.allocate((finishPageNum-startPageNum+1)*PageFileChannel.PAGE_SIZE).put(prePage);

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
                byte[] offsetList = new byte[cur.getValue()[0] - pre.getValue()[0] - pre.getValue()[1]]; //get the offset list
                dataChunk.get(invertedList);
                dataChunk.get(offsetList);

                pre = cur;
                prePage.rewind();

                return Arrays.asList(invertedList,offsetList);

            }

        };

        return it;

    }

}
