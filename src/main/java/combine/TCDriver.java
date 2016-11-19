package combine;

import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by huqiu on 16-11-15.
 */
public class TCDriver {
    public static void main(String args[]) throws Exception {

        String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
        String[] myArgs = {"", "", "", ""};
        if (otherArgs.length != 4) {
            System.err.println("Your Arguments are not enough~ We prepared arguments for you :)");
            myArgs[0] = "twitter";
            myArgs[1] = "combine_middle";
            myArgs[2] = "combine_out";
            myArgs[3] = "6";   // numReduceTasks
        }
        else {
            System.arraycopy(otherArgs, 0, myArgs, 0, 4);
        }
        GraphUndirecterAndTiling.main(new String[]{myArgs[0], myArgs[1], myArgs[3]});  // input -> middle
        System.out.println("Undirected Graph Construction and Tiling Completed!");
        JustCount.main(new String[]{myArgs[1], myArgs[2]});     // middle -> output
        System.out.println("Triangle Counting All Completed, Congratulations");
    }
}
