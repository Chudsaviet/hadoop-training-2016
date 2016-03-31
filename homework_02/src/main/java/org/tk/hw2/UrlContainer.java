package org.tk.hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jsoup.Jsoup;
import org.jsoup.safety.Cleaner;
import org.jsoup.safety.Whitelist;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.util.*;

public class UrlContainer {

    private FileSystem fs;

    public UrlContainer() throws Exception {
        System.out.println("Creating FileSystem object");
        fs = FileSystem.get(new Configuration());
    }

    public static void main(String[] args) throws Exception {
        UrlContainer uc = new UrlContainer();
        System.out.println("Starting container");
        uc.run(args);
    }

    public void run(String[] args) throws Exception {
        final String url_list = args[0];
        final String output_path = args[1];

        System.out.println("Container code launched.");
        System.out.println("url_list=<" + url_list + ">");
        System.out.println("output_path=<" + output_path + ">");

        //String out_file_path = output_path + "/" + id + ".txt";
        String out_file_path = output_path + "/out.txt";
        System.out.println("Printing output to <" + out_file_path + ">");

        PrintWriter output_writer = openOutput(out_file_path);

        BufferedReader br = openUrlList(url_list);
        String line = br.readLine();
        while (line != null) {
            int sleep_time = (int) (Math.random() * 500) + 500;
            System.out.println("Sleeping " + Integer.toString(sleep_time) + " milliseconds.");
            Thread.sleep(sleep_time);
            System.out.println("Processing line <" + line + ">");
            processLine(line, output_path, output_writer);
            line = br.readLine();
        }

        output_writer.close();
        br.close();
        fs.close();
    }

    private void processLine(String line, String output_path, PrintWriter output_writer) throws Exception {
        final String[] parts = line.split("\t");
        final String id = parts[0];
        final String url = parts[5];

        System.out.println("Processing id=<" + id + ">, url=<" + url + ">.");


        HashMap<String, Integer> words_count = calcWords(getPageText(url));
        LinkedHashMap<String, Integer> biggest_values = getBiggestValues(words_count, 10);


        for (Map.Entry<String, Integer> entry : biggest_values.entrySet()) {
            output_writer.println(id + "\t" + entry.getKey() + "\t" + entry.getValue());
        }

    }

    private String getPageText(String url) throws Exception {
        System.out.println("URL: <" + url + ">");
        Cleaner cleaner = new Cleaner(Whitelist.simpleText());
        return cleaner.clean(Jsoup.parse(new URL(url), 10000)).text();
    }

    private HashMap<String, Integer> calcWords(String text) throws Exception {
        HashMap<String, Integer> words_count = new HashMap<String, Integer>();
        String[] words = text.split("-|\\.|:|/|_|\\s|<|>|=|\"|\'");
        for (String word : words) {
            if (!word.equals("")) {
                word = word.toLowerCase();
                if (words_count.containsKey(word)) {
                    words_count.put(word, words_count.get(word) + 1);
                } else {
                    words_count.put(word, 1);
                }
                ;
            }
            ;

        }
        return words_count;
    }

    private PrintWriter openOutput(String output_path) throws Exception {
        Path pt = new Path(output_path);
        return new PrintWriter(fs.create(pt));
    }

    private BufferedReader openUrlList(String url_list) throws Exception {
        Path pt = new Path(url_list);
        return new BufferedReader(new InputStreamReader(fs.open(pt)));
    }

    public LinkedHashMap getBiggestValues(HashMap passedMap, int n) {
        List mapKeys = new ArrayList(passedMap.keySet());
        List mapValues = new ArrayList(passedMap.values());
        Collections.sort(mapKeys, Collections.reverseOrder());
        Collections.sort(mapValues, Collections.reverseOrder());

        LinkedHashMap sortedMap = new LinkedHashMap();

        Iterator valueIt = mapValues.iterator();
        int i = 0;
        while (valueIt.hasNext()) {
            Object val = valueIt.next();
            Iterator keyIt = mapKeys.iterator();

            while (keyIt.hasNext()) {
                Object key = keyIt.next();
                String comp1 = passedMap.get(key).toString();
                String comp2 = val.toString();

                if (comp1.equals(comp2)) {
                    passedMap.remove(key);
                    mapKeys.remove(key);
                    sortedMap.put((String) key, (Integer) val);
                    i++;
                    break;
                }

            }
            if (i == n) break;
        }
        return sortedMap;
    }
}
