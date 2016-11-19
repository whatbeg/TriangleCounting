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
import java.math.BigInteger;
import java.util.*;

/**
 * Created by huqiu on 16-11-14.
 */
public class GraphUndirecterAndTiling {

    public static class graphMapper extends Mapper<Object, Text, Text, Text> {
        private Text a = new Text();     // "1230030"
        private Text b = new Text();     // "1201032"
        // HashSet<String> emited = new HashSet<String>();
        int c = 0;
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /**
             * key:   line_id
             * value: "0230030   0201032"
             */
            StringTokenizer st = new StringTokenizer(value.toString());
            if (st.countTokens() == 2) {    // 空行不算
                String A = st.nextToken();
                String B = st.nextToken();
                if (!A.equals(B)) {      // not A->A
                    if (A.compareTo(B) < 0) {
                        a.set(A);
                        b.set(B);
                    }
                    else {
                        a.set(B);
                        b.set(A);
                    }
                    context.write(a, b);
                }
            }
        }
    }

    public static class graphReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * output key:   "A"
         * output value: "B,C,D...,S"
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            TreeSet<String> emited = new TreeSet<String>();
            Text from = new Text(key);
            Text to = new Text();
            for (Text val : values)
                emited.add(val.toString());
            Iterator<String> it = emited.iterator();
            String adj = "";
            while (it.hasNext()) {
                String nxt = it.next();
                adj += "," + nxt;
            }
            if (adj.length() > 0)
                adj = adj.substring(1);
            to.set(adj);
            context.write(from, to);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String[] myArgs = {"", "", ""};
        if (otherArgs.length != 3) {
            System.err.println("Usage: GraphUndirecterAndTiling <in> <out>");
            //System.exit(2);
            myArgs[0] = "twitter";
            myArgs[1] = "tm";
            myArgs[2] = "6";
        }
        else {
            System.arraycopy(otherArgs, 0, myArgs, 0, 3);
        }
        @SuppressWarnings("deprecation")
        Job job1 = new Job(conf, "CB_JOB1");
        job1.setJarByClass(GraphUndirecterAndTiling.class);
        job1.setMapperClass(graphMapper.class);
        job1.setReducerClass(graphReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setNumReduceTasks(Integer.parseInt(myArgs[2]));
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(myArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(myArgs[1]));

        job1.waitForCompletion(true);
    }
}

