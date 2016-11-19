package triangle;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.util.ArrayList;
import java.math.BigInteger;
/**
 * Created by huqiu on 16-11-14.
 */
public class TilingGraph {

    public static class tileMapper extends Mapper<Object, Text, Text, Text> {
        private Text a = new Text();
        private Text b = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /**
             * key: line_id
             * value: "102030#239023    #"
             */
            String []line = value.toString().split("\t");
            String []ab = line[0].split("#");   // ab = [a, b]
            a.set(ab[0]);
            b.set(ab[1]);
            context.write(a, b);
        }
    }

    public static class tileReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> to = new ArrayList<String>();
            Text pair = new Text();
            Text exist = new Text("#");
            for (Text val : values) {
                to.add(val.toString());
                pair.set(key.toString() + "#" + val);
                context.write(pair, exist);
            }
            exist.set("@");   // need
            for (int i = 0; i < to.size() ; i++) {
                for (int j = i+1; j < to.size(); j++) {
                    if (to.get(i).compareTo(to.get(j)) <= 0)  {//字符串的比较
                        pair.set(to.get(i) + "#" + to.get(j));
                        context.write(pair, exist);
                    }
                    else {
                        pair.set(to.get(j) + "#" + to.get(i));
                        context.write(pair, exist);
                    }
                }
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: TilingGraph <in> <out>");
            System.exit(2);
        }
        @SuppressWarnings("deprecation")
        Job job2 = new Job(conf, "job2");
        job2.setJarByClass(TilingGraph.class);
        job2.setMapperClass(tileMapper.class);
        job2.setReducerClass(tileReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setNumReduceTasks(Integer.parseInt(otherArgs[2]));
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));

        job2.waitForCompletion(true);
    }
}











