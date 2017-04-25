package JoiningPattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by ameyutturkar on 4/24/17.
 */
public class Joining_Reducer extends Reducer<CompsiteKeyWritable, Text, Text, Text>
{
    private static final Text EMPTY_TEXT = new Text("");
    private Text tmp = new Text();
    private ArrayList<Text> listA = new ArrayList<Text>();
    private ArrayList<Text> listB = new ArrayList<Text>();
    private String joinType = null;

    public void setup(Context context)
    {
        // Get the type of join from our configuration
        joinType = context.getConfiguration().get("join.type");
    }

    public void reduce(CompsiteKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Clear our lists
        listA.clear();
        listB.clear();
        // iterate through all our values, binning each record based on what
        // it was tagged with. Make sure to remove the tag!
        while (values.iterator().hasNext())
        {
            tmp = values.iterator().next();
            System.out.println(Character.toString((char) tmp.charAt(0)));
            if (Character.toString((char) tmp.charAt(0)).equals("A"))
            {
                System.out.println("here4");
                listA.add(new Text(tmp.toString().substring(1)));
            }
            if (Character.toString((char) tmp.charAt(0)).equals("B"))
            {
                System.out.println("here5");
                listB.add(new Text(tmp.toString().substring(1)));
            }
            System.out.println(tmp);
        }
        // Execute our join logic now that the lists are filled

        System.out.println(listB.size());
        executeJoinLogic(context);
    }

    private void executeJoinLogic(Context context) throws IOException, InterruptedException {

        if (joinType.equalsIgnoreCase("innerJoin")) {
            // If both lists are not empty, join A with B
            //System.out.println("here3");
            if (!listA.isEmpty() && !listB.isEmpty()) {
                System.out.println("here");
                for (Text A : listA) {
                    //System.out.println("here1");
                    for (Text B : listB) {
                        //System.out.println("here2");
                        context.write(A, B);
                    }
                }
            }
        } else if (joinType.equalsIgnoreCase("leftouter")) {
            // For each entry in A,
            for (Text A : listA) {
                // If list B is not empty, join A and B
                if (!listB.isEmpty()) {
                    for (Text B : listB) {
                        context.write(A, B);
                    }
                } else {
                    // Else, output A by itself
                    context.write(A, EMPTY_TEXT);
                }
            }
        } else if (joinType.equalsIgnoreCase("rightouter")) {
            // For each entry in B,
            for (Text B : listB) {
                // If list A is not empty, join A and B
                if (!listA.isEmpty()) {
                    for (Text A : listA) {
                        context.write(A, B);
                    }
                } else {
                    // Else, output B by itself
                    context.write(EMPTY_TEXT, B);
                }
            }
        } else if (joinType.equalsIgnoreCase("fullouter")) {
            // If list A is not empty
            if (!listA.isEmpty()) {
                // For each entry in A
                for (Text A : listA) {
                    // If list B is not empty, join A with B
                    if (!listB.isEmpty()) {
                        for (Text B : listB) {
                            context.write(A, B);
                        }
                    } else {
                        // Else, output A by itself
                        context.write(A, EMPTY_TEXT);
                    }
                }
            } else {
                // If list A is empty, just output B
                for (Text B : listB) {
                    context.write(EMPTY_TEXT, B);
                }
            }
        } else if (joinType.equalsIgnoreCase("anti")) {
            // If list A is empty and B is empty or vice versa
            if (listA.isEmpty() ^ listB.isEmpty()) {
                // Iterate both A and B with null values
                // The previous XOR check will make sure exactly one of
                // these lists is empty and therefore the list will be
                // skipped
                for (Text A : listA) {
                    context.write(A, EMPTY_TEXT);
                }
                for (Text B : listB) {
                    context.write(EMPTY_TEXT, B);
                }
            }
        }
    }
}
