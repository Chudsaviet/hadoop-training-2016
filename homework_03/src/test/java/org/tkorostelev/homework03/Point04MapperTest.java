package org.tkorostelev.homework03;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

public class Point04MapperTest {

    @Test
    public void testValidRecord()
            throws IOException, InterruptedException {
        Text value = new Text("dd3e16c3bca86788eacd1efb523d871\t20130609205907916\tnull\tMozilla/4.0 (Windows; U; Windows NT 5.1; zh-TW; rv:1.9.0.11)\t119.179.249.*\t146\t150\t3\tnull\tnull\tnull\tLV_1001_LDVi_LD_ADX_5\t300\t250\t0\t0\t100\ta499988a822facd86dd0e8e4ffef8532\t300\t1458\t282825712806\t0");

        Point04Writable assumedOutput = new Point04Writable(new IntWritable(1), new DoubleWritable(300.0));

        new MapDriver<LongWritable, Text, Text, Point04Writable>()
                .withMapper(new Point04Mapper())
                .withInput(new LongWritable(0), value)
                .withOutput(new Text("119.179.249.*"), assumedOutput)
                .withCounter(Point04ErrorCounter.MALFORMED_ROW,0)
                .withCounter(Point04BrowserType.IE,0)
                .withCounter(Point04BrowserType.CHROME,0)
                .withCounter(Point04BrowserType.MOZILLA,1)
                .withCounter(Point04BrowserType.OPERA,0)
                .withCounter(Point04BrowserType.OTHER,0)
                .runTest();
    }

    @Test
    public void testMalformedRecord()
            throws IOException, InterruptedException {
        Text value = new Text("6a33dcae5dcce5fcda37a4f1ddaa45\t20130609205907800\tnull\tMozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17 SE 2.X MetaSr 1.0\t222.185.170.*\t80\t84\t3\t31xSTvprdN1RFt\tc355da082cb288d4556f42ecf68a89d6\tnull\tEnt_F_Width1\t1000\t90\t0\t070\t832b91d59d0cb5731431653204a76c0e\t300\t1458\t282825712806\t0\n");

        new MapDriver<LongWritable, Text, Text, Point04Writable>()
                .withMapper(new Point04Mapper())
                .withInput(new LongWritable(0), value)
                .withCounter(Point04ErrorCounter.MALFORMED_ROW, 1)
                .withCounter(Point04BrowserType.IE,0)
                .withCounter(Point04BrowserType.CHROME,0)
                .withCounter(Point04BrowserType.MOZILLA,0)
                .withCounter(Point04BrowserType.OPERA,0)
                .withCounter(Point04BrowserType.OTHER,0)
                .runTest();
    }
}