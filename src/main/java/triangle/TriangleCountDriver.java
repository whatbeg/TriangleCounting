package triangle;

import org.apache.hadoop.util.GenericOptionsParser;
/**
 * Created by huqiu on 16-11-15.
 */
public class TriangleCountDriver {
    public static void main(String args[]) throws Exception {

        String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
        String[] myArgs = {"", "", "", "", ""};
        if (otherArgs.length != 5) {
            System.err.println("Your Arguments are not enough~ We prepared arguments for you :)");
            myArgs[0] = "twitter";
            myArgs[1] = "M1";
            myArgs[2] = "M2";
            myArgs[3] = "triangle_Out";
            myArgs[4] = "6";
        }
        else {
            System.arraycopy(otherArgs, 0, myArgs, 0, 5);
        }
        GraphUndirecter.main(new String[]{myArgs[0], myArgs[1]});  // input -> middle1
        System.out.println("Undirected Graph Construction Completed!");
        TilingGraph.main(new String[]{myArgs[1], myArgs[2], myArgs[4]});      // middle1 -> middle2
        System.out.println("Graph Tiling Completed");
        FeedAndCount.main(new String[]{myArgs[2], myArgs[3]});     // middle2 -> output
        System.out.println("Triangle Counting All Completed, Congratulations");
    }
}
