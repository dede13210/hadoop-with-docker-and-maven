import org.apache.hadoop.examples.Count;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class WordCountTest {
    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        Count.TokenizerMapper mapper = new Count.TokenizerMapper();
        Count.IntSumReducer reducer = new Count.IntSumReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testMapper() throws Exception {
        mapDriver.withInput(new LongWritable(), new Text("test, test"));
        mapDriver.withOutput(new Text("test"), new IntWritable(1));
        mapDriver.withOutput(new Text(","), new IntWritable(1));
        mapDriver.withOutput(new Text("test"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws Exception {
        List<IntWritable> values = Arrays.asList(new IntWritable(1), new IntWritable(1));
        reduceDriver.withInput(new Text("test"), values);
        reduceDriver.withOutput(new Text("test"), new IntWritable(2));
        reduceDriver.runTest();
    }
}