package triangle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;

/**
 * Created by huqiu on 16-11-15.
 */
public class FeedAndCount {

    private static int RESULT = 0;
    public static class feedMapper extends Mapper<Object, Text, Text, Text> {
        private Text a = new Text();
        private Text b = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /**
             * key: line_id
             * value: "102030#239023    #"   have
             *        "102320#239234    @"   need
             */
            String []line = value.toString().split("\t");
            a.set(line[0]);
            b.set(line[1]);
            context.write(a, b);
        }
    }

    public static class feedReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int jin = 0, at = 0;
            for (Text val : values) {
                if (val.toString().equals("#")) jin++;
                else if (val.toString().equals("@")) at++;
            }
            if (jin > 0) RESULT += at;
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Total Triangle Count:"), new Text(String.valueOf(RESULT)));
            System.out.println("Total Triangle Count:\t" + RESULT);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: JustCount <in> <out>");
            System.exit(2);
        }
        @SuppressWarnings("deprecation")
        Job job3 = new Job(conf, "job3");
        job3.setJarByClass(FeedAndCount.class);
        job3.setMapperClass(feedMapper.class);
        job3.setReducerClass(feedReducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setNumReduceTasks(1);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1]));

        job3.waitForCompletion(true);
    }
}
