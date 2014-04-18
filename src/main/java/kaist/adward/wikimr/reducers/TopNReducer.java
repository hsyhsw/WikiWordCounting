package kaist.adward.wikimr.reducers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    private int N;
    private TreeMap<Integer, Text> topN = new TreeMap<Integer, Text>();

    public TopNReducer() {
        System.out.println("Init TopNReducer");
    }

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        N = Integer.parseInt(conf.get("N"));
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] contents = value.toString().split(" ");
            Integer count = Integer.parseInt(contents[1]);

            // add this record to local top N map
            topN.put(count, new Text(value));
            // if we have more than N records in top N now, remove the lowest
            if (topN.size() > N) {
                topN.remove(topN.firstKey());
            }
        }

        // we are guaranteed to have at most N records in top N map now
        // emit those records in descending order here
        for (Text record : topN.descendingMap().values()) {
            context.write(NullWritable.get(), record);
        }
    }
}