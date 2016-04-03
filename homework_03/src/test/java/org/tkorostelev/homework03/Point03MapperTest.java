package org.tkorostelev.homework03;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class Point03MapperTest {
    //Sorry, hardcoded path because parametrized tests need more work.
    private final String tagsMapPath = "./tags.txt.gz";
    private Point03Mapper mapper;

    @Before
    public void createMapper() throws IOException{
        mapper = new Point03Mapper();
        mapper.loadTagsMap(tagsMapPath);
    }

    @Test
    public void testValidLine() throws Exception {
        Text value = new Text("dd3e16c3bca86788eacd1efb523d871\t20130609205907916\tnull\tMozilla/4.0 (Windows; U; Windows NT 5.1; zh-TW; rv:1.9.0.11)\t119.179.249.*\t146\t150\t3\tnull\tnull\tnull\tLV_1001_LDVi_LD_ADX_5\t300\t250\t0\t0\t100\ta499988a822facd86dd0e8e4ffef8532\t300\t1458\t282825712806\t0");

        MapDriver<LongWritable, Text, Text, IntWritable> testDriver =
                new MapDriver<LongWritable, Text, Text, IntWritable>()
                        .withMapper(mapper)
                        .withCounter(Point03ErrorCounter.NO_SUCH_ID,0)
                        .withCounter(Point03ErrorCounter.MALFORMED_ROW,0);

        testDriver.addInput(new LongWritable(0), value);

        testDriver.addOutput(new Text("the"),new IntWritable(5));
        testDriver.addOutput(new Text("please"),new IntWritable(2));
        testDriver.addOutput(new Text("on"),new IntWritable(2));
        testDriver.addOutput(new Text("in"),new IntWritable(2));
        testDriver.addOutput(new Text("box"),new IntWritable(2));
        testDriver.addOutput(new Text("a"),new IntWritable(2));
        testDriver.addOutput(new Text("©"),new IntWritable(1));
        testDriver.addOutput(new Text("you’ve"),new IntWritable(1));
        testDriver.addOutput(new Text("you"),new IntWritable(1));
        testDriver.addOutput(new Text("we"),new IntWritable(1));

        testDriver.runTest(false);
    }

    @Test
    public void testMalformedLine() throws Exception {
        Text value = new Text("dd3e16c3bca86788eacd1efb523d871\tMozilla/4.0 (Windows; U; Windows NT 5.1; zh-TW; rv:1.9.0.11)\t119.179.249.*\t146\t150\t3\tnull\tnull\tnull\tLV_1001_LDVi_LD_ADX_5\t300\t250\t0\t0\t100\ta499988a822facd86dd0e8e4ffef8532\t300\t1458\t282825712806\t0");

        MapDriver<LongWritable, Text, Text, IntWritable> testDriver =
                new MapDriver<LongWritable, Text, Text, IntWritable>()
                        .withMapper(mapper)
                        .withCounter(Point03ErrorCounter.NO_SUCH_ID,0)
                        .withCounter(Point03ErrorCounter.MALFORMED_ROW,1);

        testDriver.addInput(new LongWritable(0), value);

        testDriver.runTest(false);
    }

    @Test
    public void testInvalidID() throws Exception {
        Text value = new Text("dd3e16c3bca86788eacd1efb523d871\t20130609205907916\tnull\tMozilla/4.0 (Windows; U; Windows NT 5.1; zh-TW; rv:1.9.0.11)\t119.179.249.*\t146\t150\t3\tnull\tnull\tnull\tLV_1001_LDVi_LD_ADX_5\t300\t250\t0\t0\t100\ta499988a822facd86dd0e8e4ffef8532\t300\t1458\t9999999\t0");

        MapDriver<LongWritable, Text, Text, IntWritable> testDriver =
                new MapDriver<LongWritable, Text, Text, IntWritable>()
                        .withMapper(mapper)
                        .withCounter(Point03ErrorCounter.NO_SUCH_ID,1)
                        .withCounter(Point03ErrorCounter.MALFORMED_ROW,0);

        testDriver.addInput(new LongWritable(0), value);

        testDriver.runTest(false);
    }

}