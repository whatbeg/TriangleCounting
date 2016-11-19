package combine;

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
import java.util.ArrayList;
import java.util.TreeSet;

/**
 * Created by huqiu on 16-11-15.
 */
public class JustCount {

    private static int RESULT = 0;
    public static class feedMapper extends Mapper<Object, Text, Text, Text> {
        private Text pair = new Text();
        private Text exist = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /**
             * key: line_id
             * value: "A    B,C,D...,S"
             */
            String []line = value.toString().split("\t");
            String A = line[0];
            String []adjs = line[1].split(",");
            exist.set("#");   //supply
            for (String adj : adjs) {
                pair.set(A + "#" + adj);
                context.write(pair, exist);
            }
            exist.set("@");   // need
            for (int i = 0; i < adjs.length ; i++) {
                for (int j = i+1; j < adjs.length; j++) {
                    if (adjs[i].compareTo(adjs[j]) < 0)  {        //字符串的比较
                        pair.set(adjs[i] + "#" + adjs[j]);
                        context.write(pair, exist);
                    }
                    else if (adjs[i].compareTo(adjs[j]) > 0){
                        pair.set(adjs[j] + "#" + adjs[i]);
                        context.write(pair, exist);
                    }
                }
            }
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
        String[] myArgs = {"", "", ""};
        if (otherArgs.length != 2) {
            System.err.println("Usage: JustCount <in> <out>");
            //System.exit(2);
            myArgs[0] = "tm";
            myArgs[1] = "tout";
        }
        else {
            System.arraycopy(otherArgs, 0, myArgs, 0, 2);
        }
        @SuppressWarnings("deprecation")
        Job job3 = new Job(conf, "CB_JOB2");
        job3.setJarByClass(JustCount.class);
        job3.setMapperClass(feedMapper.class);
        job3.setReducerClass(feedReducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setNumReduceTasks(1);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(myArgs[0]));
        FileOutputFormat.setOutputPath(job3, new Path(myArgs[1]));

        job3.waitForCompletion(true);
    }
}
