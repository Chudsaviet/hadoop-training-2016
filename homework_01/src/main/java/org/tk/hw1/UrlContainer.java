package org.tk.hw1;

import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.jsoup.Jsoup;
import org.jsoup.safety.Cleaner;
import org.jsoup.safety.Whitelist;

public class UrlContainer {

    private FileSystem fs;

    public UrlContainer() throws Exception {
      System.out.println("Creating FileSystem object");
      fs = FileSystem.get(new Configuration());
    }

    public void run(String[] args) throws Exception {
        final String url_list = args[0];
        final String output_path = args[1];

        System.out.println("Container code launched.");
        System.out.println("url_list=<"+url_list+">");
        System.out.println("output_path=<"+output_path+">");

        BufferedReader br=openUrlList(url_list);
        String line="";
        while (line != null){
                line=br.readLine();
                int sleep_time = (int)(Math.random()*100000) + 5000;
                System.out.println("Sleeping "+Integer.toString(sleep_time)+" milliseconds.");
                Thread.sleep(sleep_time);
                System.out.println("Processing line <"+line+">");
                processLine(line,output_path);
        }

        br.close();
        fs.close();
    }

    private void processLine(String line, String output_path) throws Exception {
        final String[] parts = line.split("\t");
        final String id = parts[0];
        final String url = parts[5];

        String out_file_path = output_path + "/" + id + ".txt";
        System.out.println("Processing id=<"+id+">, url=<"+url+">.");

        PrintWriter output_writer = openOutput(out_file_path);
        try {
          Map<String,Integer> words_count = calcWords(getPageText(url));
          
          System.out.println("Printing output to <" + out_file_path + ">");
          
          output_writer.println(getBiggestValues((HashMap)words_count,10).toString());
        }
        catch (Exception e) {
          System.out.println(e.toString());
        }
        finally {
          output_writer.close();
        }
    }

    private String getPageText(String url) throws Exception {
        System.out.println("URL: <"+url+">");
        Cleaner cleaner = new Cleaner(Whitelist.simpleText());
        return cleaner.clean(Jsoup.parse(new URL(url),10000)).text();
    }

    private Map<String,Integer> calcWords(String text) throws Exception {
        Map<String,Integer> words_count = new HashMap<String,Integer>(); 
        String[] words = text.split("-|\\.|:|/|_|\\s|<|>|=|\"|\'");
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
        return new PrintWriter(fs.create(pt));
    }

    private BufferedReader openUrlList(String url_list) throws Exception {
        Path pt=new Path(url_list);
        return new BufferedReader(new InputStreamReader(fs.open(pt)));
    }

    public LinkedHashMap getBiggestValues(HashMap passedMap, int n) {
       List mapKeys = new ArrayList(passedMap.keySet());
       List mapValues = new ArrayList(passedMap.values());
       Collections.sort(mapKeys,Collections.reverseOrder());
       Collections.sort(mapValues,Collections.reverseOrder());

       LinkedHashMap sortedMap = new LinkedHashMap();

       Iterator valueIt = mapValues.iterator();
       int i=0;
       while (valueIt.hasNext()) {
           Object val = valueIt.next();
           Iterator keyIt = mapKeys.iterator();

           while (keyIt.hasNext()) {
               Object key = keyIt.next();
               String comp1 = passedMap.get(key).toString();
               String comp2 = val.toString();

               if (comp1.equals(comp2)){
                   passedMap.remove(key);
                   mapKeys.remove(key);
                   sortedMap.put((String)key, (Integer)val);
                   i++;
                   break;
               }

           }
          if(i==n) break;
       }
       return sortedMap;
    }

    public static void main(String[] args) throws Exception {
        UrlContainer uc = new UrlContainer();
        System.out.println("Starting container");
        uc.run(args);
    }
}