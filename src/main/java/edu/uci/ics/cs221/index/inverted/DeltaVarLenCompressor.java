package edu.uci.ics.cs221.index.inverted;

import org.apache.commons.lang.ArrayUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Implement this compressor with Delta Encoding and Variable-Length Encoding.
 * See Project 3 description for details.
 */
public class DeltaVarLenCompressor {

    //@Override
    public static byte[] encode(List<Integer> integers) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        int pre = integers.get(0);

        for(int i = 0; i<integers.size();i++){
            if(i > 0) {
                //calculate the difference
                integers.set(i, integers.get(i) - pre);
                //save pre-value
                pre += integers.get(i);
            }

            try {
                //encode the delta value and write it in the output stream
                outputStream.write(encode_one(integers.get(i)));
            }
            catch (Exception e){
                e.printStackTrace();
            }

        }

        return outputStream.toByteArray();
    }

    //@Override
    public static List<Integer> decode(byte[] bytes, int start, int length) {
        if(start + length > bytes.length){
            throw new UnsupportedOperationException();
        }
        List<Integer> res = new ArrayList<>();
        boolean first = true;
        int pre=0;

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        //loop the byte array
        for(int i = start; i < start+length; i++){
            outputStream.write(bytes[i]);
            //if this byte starts with 0, then it is the last bytes
            if((bytes[i] & (1<<7)) == 0){
                //decode the bytes array to int
                int val = decode_one(outputStream.toByteArray());
                if(first) {
                    res.add(val);
                    pre = val;
                    first = false;
                }
                else{
                    res.add(pre+val);
                    pre = pre+val;
                }
                //clean the outputstream for the next element
                outputStream.reset();
            }
        }
        try {
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return res;
    }


    public static byte[] encode_one(int num){
        //Calculate how many bytes needed after compression
        int totalBytes = ((32 - Integer.numberOfLeadingZeros(num)) + 6) / 7;
        //Need at least one byte
        if(totalBytes==0){totalBytes=1;}
        byte[] res = new byte[totalBytes];
        //Append bytes
        for(int i = totalBytes-1; i >=0; i--) {
            //Always read the last 7 bytes into the res
            //Change the first bit to 1
            res[i] = (byte) ((num & 0b1111111) | 0b10000000);
            num >>= 7;
        }
        //change the last bit back to indicate the end
        res[totalBytes-1] &= 0b01111111;
        return res;
    }

    private static int decode_one(byte[]bytes){
        int res = 0;
        for(int i = 0; i < bytes.length;i++){
            //shift result 7 position, and put 7 bits of the current byte into res.
            res = (res << 7 | (bytes[i] & 0b1111111));
        }
        //System.out.println("Byte : "+ bytes.length + " Output: " + Integer.toBinaryString(res));
        return res;
    }


    public static void main(String[] args) {
        //List<Integer> nums = Arrays.asList(200,400);

        List<Integer> nums = Arrays.asList(1,2,3,4,5,6,7,8,9,10,90,100,111,999,10000,123455);
        System.out.println(nums.toString());
        byte[] encoded = encode(nums);
        List<Integer> returns = decode(encoded,0,encoded.length);
        System.out.println(returns.toString());

    }

}
