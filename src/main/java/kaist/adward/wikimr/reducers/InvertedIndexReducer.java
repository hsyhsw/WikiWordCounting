package kaist.adward.wikimr.reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class InvertedIndexReducer extends Reducer<Text, LongWritable, Text, Text> {

    private IntWritable result = new IntWritable();

    public InvertedIndexReducer() {
        System.out.println("Init InvertedIndexReducer");
    }

    @Override
    protected void reduce(Text word, Iterable<LongWritable> docIdList, Context context) throws IOException, InterruptedException {
        //int sum = 0;
        boolean firstDocId = true;
        StringBuilder docIdCsv = new StringBuilder();

        Iterator<LongWritable> itr = docIdList.iterator();

        while (itr.hasNext()) {
            if (!firstDocId) {
                docIdCsv.append(",");
            }
            firstDocId = false;
            docIdCsv.append(itr.next().toString());
		}

        context.write(word, new Text(docIdCsv.toString()));
    }
}