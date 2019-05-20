package edu.uci.ics.cs221.index.inverted;

import org.apache.commons.lang.ArrayUtils;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
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
                integers.set(i, integers.get(i) - pre);
                pre += integers.get(i);
            }

            try {
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
        for(int i = start; i < start+length; i++){
            outputStream.write(bytes[i]);
            if((bytes[i] & (1<<7)) == 0){
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


    public static byte[] encode_one(int input){
        int totalBytes = ((32 - Integer.numberOfLeadingZeros(input)) + 6) / 7;
        //System.out.println("Byte : "+ totalBytes + " input: " + Integer.toBinaryString(input));
        totalBytes = totalBytes > 0 ? totalBytes : 1;
        byte[] output = new byte[totalBytes];
        for(int i = 0; i < totalBytes; i++) {
            output[i] = (byte) ((input & 0b1111111) | 0b10000000);
            input >>= 7;
        }
        output[0] &= 0b01111111;
        ArrayUtils.reverse(output);
        return output;
    }

    private static int decode_one(byte[]bytes){
        int res = 0;
        for(int i = 0; i < bytes.length;i++){
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
