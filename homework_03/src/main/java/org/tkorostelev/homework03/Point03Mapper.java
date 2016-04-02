package org.tkorostelev.homework03;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

public class Point03Mapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Map<String, Map<String, Integer>> tagsMap;
    private Logger logger = Logger.getLogger(Point03Mapper.class);

    @Override
    public void setup(Context context)
            throws IOException {
        URI[] cachedFiles = context.getCacheFiles();

        String tagsFileName = (new Path(cachedFiles[0].getPath())).getName();

        logger.info("Loading tags list from next archive: " + tagsFileName);
        tagsMap = loadTagsMap(tagsFileName);
        logger.info("Tags list loaded");
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        DatasetRow inputRow = DatasetRow.parseInputLine(value.toString());
        if (inputRow == null) {
            context.getCounter(Point03Counter.MALFORMED_ROW).increment(1);
            return;
        }

        Map<String, Integer> tagMap = tagsMap.get(inputRow.getID());

        if (tagMap == null) {
            context.getCounter(Point03Counter.NO_SUCH_ID).increment(1);
            return;
        }

        for (Map.Entry<String, Integer> entry : tagMap.entrySet()) {
            context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
        }

    }

    private Map<String, Map<String, Integer>> loadTagsMap(String tagsFileName)
            throws IOException {
        Map<String, Map<String, Integer>> result = new HashMap<String, Map<String, Integer>>();

        InputStream fileStream = new FileInputStream("./" + tagsFileName);
        InputStream gzipStream = new GZIPInputStream(fileStream);
        InputStreamReader decoder = new InputStreamReader(gzipStream);
        BufferedReader reader = new BufferedReader(decoder);

        String line = reader.readLine();
        while (line != null) {
            if (line.length() == 0) break;

            StringTokenizer tokenizer = new StringTokenizer(line, "\t");
            if (tokenizer.countTokens() != 3)
                throw new IOException("Bad tags file: line <" + line + "> has not enough columns");
            String id = tokenizer.nextToken();
            String tag = tokenizer.nextToken();
            int tagOccurences = Integer.parseInt(tokenizer.nextToken());

            Map<String, Integer> tagMap = result.get(id);
            if (tagMap == null) {
                tagMap = new HashMap<String, Integer>();
                result.put(id, tagMap);
            }

            if (tagMap.containsKey(tag))
                throw new IOException("Bad tags file: tag <" + tag + "> for id <" + id + "> repeated");

            tagMap.put(tag, tagOccurences);

            line = reader.readLine();
        }

        reader.close();
        decoder.close();
        gzipStream.close();
        fileStream.close();

        return result;
    }

}
