package triangle;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.StringTokenizer;

/**
 * Created by huqiu on 16-11-14.
 */
public class GraphUndirecter {

    public static class graphMapper extends Mapper<Object, Text, Text, Text> {
        private Text a = new Text();     // "0230030^0201032"
        private Text b = new Text("#");  // have
        HashSet<String> emited = new HashSet<String>();
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
                String emit;
                if (A.compareTo(B) != 0) {   // not A->A
                    if (A.compareTo(B) < 0) emit = A + "#" + B;
                    else emit = B + "#" + A;
                    if (!emited.contains(emit)) {
                        emited.add(emit);
                        a.set(emit);
                        context.write(a, b);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: GraphUndirecterAndTiling <in> <out>");
            System.exit(2);
        }
        @SuppressWarnings("deprecation")
        Job job1 = new Job(conf, "job1");
        job1.setJarByClass(GraphUndirecter.class);
        job1.setMapperClass(graphMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setNumReduceTasks(0);         //设置个数为0
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

        job1.waitForCompletion(true);
    }
}

