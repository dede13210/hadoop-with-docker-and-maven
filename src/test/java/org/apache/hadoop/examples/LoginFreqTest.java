package org.apache.hadoop.examples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class LoginFreqencieTest {
    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        LoginFreqencie.TokenizerMapper mapper = new LoginFreqencie.TokenizerMapper();
        LoginFreqencie.IntSumReducer reducer = new LoginFreqencie.IntSumReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testMapper() throws Exception {
        mapDriver.withInput(new LongWritable(), new Text("154.131.45.155 - - [27/Dec/2037:12:00:00 +0530] \"GET /usr/login HTTP/1.0\" 200 5059 \"-\" \"Mozilla/5.0 (Android 10; Mobile; rv:84.0) Gecko/84.0 Firefox/84.0\" 607\n207.194.20.187 - - [27/Dec/2037:12:00:00 +0530] \"PUT /usr HTTP/1.0\" 303 4989 \"-\" \"Mozilla/5.0 (Linux; Android 10; ONEPLUS A6000) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.116 Mobile Safari/537.36 EdgA/45.12.4.5121\" 4241"))
                .withOutput(new Text("[27/Dec/2037:"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testReducer() throws Exception {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("date"), values)
                .withOutput(new Text("date"), new IntWritable(2))
                .runTest();
    }
}