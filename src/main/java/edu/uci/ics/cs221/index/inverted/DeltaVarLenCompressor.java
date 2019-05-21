package edu.uci.ics.cs221.index.inverted;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


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
            int diff = pre;
            if(i > 0) {
                diff = integers.get(i) - pre;
                //save pre-value
                pre += diff;
            }

            try {
                //encode the delta value and write it in the output stream
                outputStream.write(encode_one(diff));
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
        int totalBytes = (6 + 32 - Integer.numberOfLeadingZeros(num)) / 7;
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

        test();

    }

    public static void test(){
        List<Integer> nums = new ArrayList<>();

        int x =0;
        int pre = 0;
        while(x<10000){
            Random rand = new Random();
            int n = rand.nextInt(50);
            nums.add(pre+n);
            pre = pre+n;
            x++;
        }

        System.out.println(nums.toString());
        byte[] encoded = encode(nums);
        List<Integer> returns = new ArrayList<>();

        returns= decode(encoded,0,encoded.length);
        System.out.println(returns.toString());

        if(nums.equals(returns)){
            System.out.println("Good!!!");
        }
        else{
            System.out.println(">>>>????");
        }


    }

}
