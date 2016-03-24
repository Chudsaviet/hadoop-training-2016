package com.hortonworks.simpleyarnapp;

import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

public class UrlContainer {

    public void run(String[] args) throws Exception {
        final String url_list = args[0];
        final String output_path = args[1];

        System.out.println("Container code launched.");
        System.out.println("url_list=<"+url_list+">");
        System.out.println("output_path=<"+output_path+">");

        BufferedReader br=openUrlList(url_list);
        String line=br.readLine(); //skip first line because it contains column names
        while (line != null){
                line=br.readLine();
                processLine(line,output_path);
        }

        br.close();
    }

    private void processLine(String line, String output_path) throws Exception {
        final String[] parts = line.split("\t");
        final String id = parts[0];
        final String url = parts[5];

        System.out.println("Processing id=<"+id+">, url=<"+url+">.");
        Map<String,Integer> words_count = calcWords(url);

        String out_file_path = output_path + "/" + id + ".txt";
        System.out.println("Printing output to <" + out_file_path + ">");
        PrintWriter output_writer = openOutput(out_file_path);
        output_writer.println(words_count.toString());
        output_writer.close();
    }

    private Map<String,Integer> calcWords(String text) throws Exception {
        Map<String,Integer> words_count = new HashMap<String,Integer>(); 
        String[] words = text.split("-|\\.|:|/|_|\\s");
        for (String word : words) {
            if(!word.equals("")) {
                word=word.toLowerCase();
                if(words_count.containsKey(word)) {
                    words_count.put(word, words_count.get(word) + 1);
                }
                else {
                    words_count.put(word, 1);
                };
            };
            
        }
        return words_count;
    }

    private PrintWriter openOutput(String output_path) throws Exception {
        Path pt=new Path(output_path);
        FileSystem fs = FileSystem.get(new Configuration());
        return new PrintWriter(fs.create(pt));
    }

    private BufferedReader openUrlList(String url_list) throws Exception {
        Path pt=new Path(url_list);
        FileSystem fs = FileSystem.get(new Configuration());
        return new BufferedReader(new InputStreamReader(fs.open(pt)));
    }

    public static void main(String[] args) throws Exception {
        UrlContainer uc = new UrlContainer();
        uc.run(args);
    }
}