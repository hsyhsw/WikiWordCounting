package kaist.adward.wikimr;

import kaist.adward.wikimr.mappers.InvertedIndexMapper;
import kaist.adward.wikimr.reducers.InvertedIndexReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Ignore
public class TestInvertedIndex {

    MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, Text> mapReduceDriver;
    MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
    ReduceDriver<Text, LongWritable, Text, Text> reduceDriver;

    @Before
    public void setup() {
        InvertedIndexMapper mapper = new InvertedIndexMapper();
        InvertedIndexReducer reducer = new InvertedIndexReducer();
        mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
        mapDriver.setMapper(mapper);
        reduceDriver = new ReduceDriver<Text, LongWritable, Text, Text>();
        reduceDriver.setReducer(reducer);
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, Text>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);

    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text("<title>AccessibleComputing</title> <ns>0</ns> <id>10</id> <redirect title=\"Computer accessibility\" /> <revision> <id>381202555</id> <parentid>381200179</parentid> <timestamp>2010-08-26T22:38:36Z</timestamp> <contributor> <username>OlEnglish</username> <id>7181920</id> </contributor> <minor /> <comment>[[Help:Reverting|Reverted]] edits by [[Special:Contributions/76.28.186.133|76.28.186.133]] ([[User talk:76.28.186.133|talk]]) to last version by Gurch</comment> <text xml:space=\"preserve\">#REDIRECT [[Computer accessibility]] {{R from CamelCase}}</text> <sha1>lo15ponaybcg2sf49sstw9gdjmdetnk</sha1> <models>wikitext</models> <format>text/x-wiki</format> </revision>"));
        mapDriver.withOutput(new Text("computer"), new LongWritable(10));
        mapDriver.withOutput(new Text("accessibility"), new LongWritable(10));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {

        List<LongWritable> values = new ArrayList();
        values.add(new LongWritable(10));
        reduceDriver.withInput(new Text("accessibility"), values);
        reduceDriver.withInput(new Text("computer"), values);
        reduceDriver.withOutput(new Text("accessibility"), new Text("10"));
        reduceDriver.withOutput(new Text("computer"), new Text("10"));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(1), new Text("<title>AccessibleComputing</title> <ns>0</ns> <id>10</id> <redirect title=\"Computer accessibility\" /> <revision> <id>381202555</id> <parentid>381200179</parentid> <timestamp>2010-08-26T22:38:36Z</timestamp> <contributor> <username>OlEnglish</username> <id>7181920</id> </contributor> <minor /> <comment>[[Help:Reverting|Reverted]] edits by [[Special:Contributions/76.28.186.133|76.28.186.133]] ([[User talk:76.28.186.133|talk]]) to last version by Gurch</comment> <text xml:space=\"preserve\">#REDIRECT [[Computer accessibility]] {{R from CamelCase}}</text> <sha1>lo15ponaybcg2sf49sstw9gdjmdetnk</sha1> <models>wikitext</models> <format>text/x-wiki</format> </revision>"));
        mapReduceDriver.addOutput(new Text("accessibility"), new Text("10"));
        mapReduceDriver.addOutput(new Text("computer"), new Text("10"));
        mapReduceDriver.runTest();
    }
}
