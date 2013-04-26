import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * This is the skeleton for CS61c project 1, Fall 2012.
 *
 * Contact Alan Christopher or Ravi Punj with questions and comments.
 *
 * Reminder:  DO NOT SHARE CODE OR ALLOW ANOTHER STUDENT TO READ YOURS.
 * EVEN FOR DEBUGGING. THIS MEANS YOU.
 *
 */
public class Proj1 {

    /** An Example Writable which contains two String Objects. */
    public static class StringPair implements Writable {
        /** The String objects I wrap. */
	private String a, b;

	/** Initializes me to contain empty strings. */
	public StringPair() {
	    a = b = "";
	}
	
	/** Initializes me to contain A, B. */
        public StringPair(String a, String b) {
            this.a = a;
	    this.b = b;
        }

        /** Serializes object - needed for Writable. */
        public void write(DataOutput out) throws IOException {
            new Text(a).write(out);
	    new Text(b).write(out);
        }

        /** Deserializes object - needed for Writable. */
        public void readFields(DataInput in) throws IOException {
	    Text tmp = new Text();
	    tmp.readFields(in);
	    a = tmp.toString();
	    
	    tmp.readFields(in);
	    b = tmp.toString();
        }

	/** Returns A. */
	public String getA() {
	    return a;
	}
	/** Returns B. */
	public String getB() {
	    return b;
	}
    }
/** An Example Writable which contains two String Objects. */
    public static class NumPair implements Writable {
        /** The String objects I wrap. */
        DoubleWritable a = new DoubleWritable();
        DoubleWritable b = new DoubleWritable();

	/** Initializes me to contain empty strings. */
	public NumPair() {
	    a.set(0);
            b.set(0);
	}

	/** Initializes me to contain A, B. */
        public NumPair(DoubleWritable a, DoubleWritable b) {
            a.set(a);
	    b.set(b);
        }

        /** Serializes object - needed for Writable. */
        public void write(DataOutput out) throws IOException {
            new DoubleWritable(a).write(out);
	    new DoubleWritable(b).write(out);
        }

        /** Deserializes object - needed for Writable. */
        public void readFields(DataInput in) throws IOException {
	    DoubleWritable tmp = new DoubleWritable();
	    tmp.readFields(in);
	    a = tmp.get();

	    tmp.readFields(in);
	    b = tmp.get();
        }

	/** Returns A. */
	public DoubleWritable getA() {
	    return a;
	}
	/** Returns B. */
	public DoubleWritable getB() {
	    return b;
	}
    }

  /**
   * Inputs a set of (docID, document contents) pairs.
   * Outputs a set of (Text, Text) pairs.
   */
    public static class Map1 extends Mapper<WritableComparable, Text, Text, DoubleWritable> {
        /** Regex pattern to find words (alphanumeric + _). */
        final static Pattern WORD_PATTERN = Pattern.compile("\\w+");
        private DoubleWritable distance = new DoubleWritable();

        private String targetGram = null;
	private int funcNum = 0;

        /*
         * Setup gets called exactly once for each mapper, before map() gets called the first time.
         * It's a good place to do configuration or setup that can be shared across many calls to map
         */
        @Override
        public void setup(Context context) {
            targetGram = context.getConfiguration().get("targetGram").toLowerCase();
	    try {
		funcNum = Integer.parseInt(context.getConfiguration().get("funcNum"));
	    } catch (NumberFormatException e) {
		/* Do nothing. */
	    }
        }

        @Override
        public void map(WritableComparable docID, Text docContents, Context context)
                throws IOException, InterruptedException {
            Matcher matcher = WORD_PATTERN.matcher(docContents.toString());
	    Func func = funcFromNum(funcNum);
            
            Text word = new Text();
            ArrayList<String> myArray_allwords = new ArrayList<String>();           //all contents in an arraylist
            ArrayList<String> targetGramArrayForm = new ArrayList<String>();        //targetGram in arraylist form
            ArrayList<String> chunking = new ArrayList<String>();                   //how we will chunk through the text
            ArrayList<Integer> targetGramPositions = new ArrayList<Integer>();      //positions of targetGram in contents
            ArrayList<Integer> halfwaypoints = new ArrayList<Integer>();        //this is how we'll iterate through this fella
            
            String[] targetGramArray = targetGram.split(" ");       //split up targetGram

            for(String each : targetGramArray)
            {
                targetGramArrayForm.add(each);                      //create arraylist form of targetGram
            }


            Integer targetGramSize = targetGramArray.length;        //calculate # of words in target gram

	    //Maybe read in the input?
	    parse:
            while (matcher.find()) {                                //
                String thisword = matcher.group();                  // Put all docContents into an ArrayList
                thisword = thisword.toLowerCase();                  // in lower case form
                myArray_allwords.add(thisword);                     //
            }
	    //Maybe do something with the input?
            
            for(int i = 0; i<myArray_allwords.size()-targetGramSize; i++)          //record all positions of targetGram
            {
               
               if(myArray_allwords.subList(i, i+targetGramSize).equals(targetGramArrayForm))       //record all positions of targetGram
               {
                   targetGramPositions.add(i);                      //record all positions of targetGram
               }
            }

            int halfwaypoint;

            for(int j = 0; j<targetGramPositions.size()-1; j++)
            {
                halfwaypoint = (targetGramPositions.get(j) + targetGramPositions.get(j+1))/2;       //get halfway point, round down if decimal (1 2 3 4 5 6)
                halfwaypoints.add(halfwaypoint);                                //soon we will chunk it
            }                                                                                       //6-1=5 5/2=2      (1through3) (4through6)
            halfwaypoints.add(myArray_allwords.size());                         //add in the last length

            int chunkindex=0;                                                                       //index for leftmost positions of closest target gram
            for(int a = 0; a<targetGramSize-1; a++)
            {
                chunking.add(myArray_allwords.get(a));                                              //initialize # of words in gram minus 1
            }

            int distance_to_nearest_targetGram;
            String indexing_n_gram = "";
            for(int k = targetGramSize; k < myArray_allwords.size()-targetGramSize-1; k++)
            {
                chunking.add(myArray_allwords.get(k));                                              //initialize last word to make targetGramSize
                if(k == halfwaypoints.get(chunkindex))                          //when we hit the next halfway point, move the next one
                {
                    chunkindex++;
                }
                else{
                    distance_to_nearest_targetGram = Math.abs(k - targetGramPositions.get(chunkindex));     //position of word - position of leftmost position of closest target gram
                }
                for(String each : chunking){
                    indexing_n_gram = indexing_n_gram + each;       //create string version of current n-gram
                }
                distance_to_nearest_targetGram = func.f(distance_to_nearest_targetGram);
                distance.set(distance_to_nearest_targetGram);
                distance_num_pair = new NumPair(distance, new DoubleWritable(1));

                word.set("" + targetGram + ":" + indexing_n_gram);
                context.write(word, distance_num_pair);  //emit string pair of target n-gram: current n-gram, longwritable distance

                chunking.remove(0);         //take off first word to add on next on next loop
                indexing_n_gram = "";       //reset string
                }

            }
	    //Maybe generate output?
	}

	/** Returns the Func corresponding to FUNCNUM*/
	private Func funcFromNum(int funcNum) {
	    Func func = null;
	    switch (funcNum) {
	    case 0:	
		func = new Func() {
			public double f(double d) {
			    return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0;
			}			
		    };	
		break;
	    case 1:
		func = new Func() {
			public double f(double d) {
			    return d == Double.POSITIVE_INFINITY ? 0.0 : 1.0 + 1.0 / d;
			}			
		    };
		break;
	    case 2:
		func = new Func() {
			public double f(double d) {
			    return d == Double.POSITIVE_INFINITY ? 0.0 : Math.sqrt(d);
			}			
		    };
		break;
	    }
	    return func;
	}
    

    /** Here's where you'll be implementing your combiner. It must be non-trivial for you to receive credit. */
    public static class Combine1 extends Reducer<Text, NumPair, Text, NumPair> {

      @Override
      public void reduce(Text key, Iterable<NumPair> values,
              Context context) throws IOException, InterruptedException {

          double output = 0;
          double counter = 0;
          for(NumPair distance : values) //should be an iterator with all the distances of a n-gram to a target gram over several documents
          {
              output += distance.getA().get();      //add up the distances
              counter += distance.getB().get();     //add up the counts
          }

          context.write(key, new NumPair(new DoubleWritable(output), new DoubleWritable(counter)));  //emit a (target n-gram, current n-gram), (summed distnace, # of appearances)
      }
    }


    public static class Reduce1 extends Reducer<Text, NumPair, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<NumPair> values,
			   Context context) throws IOException, InterruptedException {

          double output = 0;
          double counter = 0;
          for(NumPair distance : values) //should be an iterator with all the distances of a n-gram to a target gram over several documents
          {
              output += distance.getA().get();      //add up the distances
              counter += distance.getB().get();     //add up the counts
          }

          DoubleWritable co_occurance = new DoubleWritable(0);
          if(output*Math.pow((Math.log(output)),3)/counter > 0)
          {
              co_occurance.set(output*Math.pow((Math.log(output)),3)/counter);
          }
          context.write(key, co_occurance);

           
        }
    }

    public static class Map2 extends Mapper<Text, DoubleWritable, DoubleWritable, Text> {
	//maybe do something, maybe don't
        //
        @Override
        public void map(Text keyvalue, DoubleWritable co_occurance, Context context)
                throws IOException, InterruptedException {
            DoubleWritable negative_of_co_occurance = new DoubleWritable(-co_occurance.get());
            context.write(negative_of_co_occurance, keyvalue);
        }
    }

    public static class Reduce2 extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

      int n = 0;
      static int N_TO_OUTPUT = 100;

      /*
       * Setup gets called exactly once for each reducer, before reduce() gets called the first time.
       * It's a good place to do configuration or setup that can be shared across many calls to reduce
       */
      @Override
      protected void setup(Context c) {
        n = 0;
      }

        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
	    //you should be outputting the final values here.
            Text word = new Text();

            for(Text each : values)
            {
                String keyvalue = each.toString;
                String[] splitted = each.split(":");
                String output = "Target n-gram: " + splitted[0] + "Other n-gram: " + splitted[1] + "Cooccurance is: ";
                word.set(output);
                context.write(word, Math.abs(key.get()));
            }
        }
    }
}

    /*
     *  You shouldn't need to modify this function much. If you think you have a good reason to,
     *  you might want to discuss with staff.
     *
     *  The skeleton supports several options.
     *  if you set runJob2 to false, only the first job will run and output will be
     *  in TextFile format, instead of SequenceFile. This is intended as a debugging aid.
     *
     *  If you set combiner to false, neither combiner will run. This is also
     *  intended as a debugging aid. Turning on and off the combiner shouldn't alter
     *  your results. Since the framework doesn't make promises about when it'll
     *  invoke combiners, it's an error to assume anything about how many times
     *  values will be combined.
     */
    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        boolean runJob2 = conf.getBoolean("runJob2", true);
        boolean combiner = conf.getBoolean("combiner", false);

        if(runJob2)
          System.out.println("running both jobs");
        else
          System.out.println("for debugging, only running job 1");

        if(combiner)
          System.out.println("using combiner");
        else
          System.out.println("NOT using combiner");

        Path inputPath = new Path(args[0]);
        Path middleOut = new Path(args[1]);
        Path finalOut = new Path(args[2]);
        FileSystem hdfs = middleOut.getFileSystem(conf);
        int reduceCount = conf.getInt("reduces", 32);

        if(hdfs.exists(middleOut)) {
          System.err.println("can't run: " + middleOut.toUri().toString() + " already exists");
          System.exit(1);
        }
        if(finalOut.getFileSystem(conf).exists(finalOut) ) {
          System.err.println("can't run: " + finalOut.toUri().toString() + " already exists");
          System.exit(1);
        }

        {
            Job firstJob = new Job(conf, "wordcount+co-occur");

            firstJob.setJarByClass(Map1.class);

	    /* You may need to change things here */
            firstJob.setMapOutputKeyClass(WritableComparable.class);
            firstJob.setMapOutputValueClass(Text.class);
            firstJob.setOutputKeyClass(Text.class);
            firstJob.setOutputValueClass(DoubleWritable.class);
	    /* End region where we expect you to perhaps need to change things. */

            firstJob.setMapperClass(Map1.class);
            firstJob.setReducerClass(Reduce1.class);
            firstJob.setNumReduceTasks(reduceCount);


            if(combiner)
              firstJob.setCombinerClass(Combine1.class);

            firstJob.setInputFormatClass(SequenceFileInputFormat.class);
            if(runJob2)
              firstJob.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.addInputPath(firstJob, inputPath);
            FileOutputFormat.setOutputPath(firstJob, middleOut);

            firstJob.waitForCompletion(true);
        }

        if(runJob2) {
            Job secondJob = new Job(conf, "sort");

            secondJob.setJarByClass(Map1.class);
	    /* You may need to change things here */
            secondJob.setMapOutputKeyClass(Text.class);
            secondJob.setMapOutputValueClass(DoubleWritable.class);
            secondJob.setOutputKeyClass(DoubleWritable.class);
            secondJob.setOutputValueClass(Text.class);
	    /* End region where we expect you to perhaps need to change things. */

            secondJob.setMapperClass(Map2.class);
            if(combiner)
              secondJob.setCombinerClass(Reduce2.class);
            secondJob.setReducerClass(Reduce2.class);

            secondJob.setInputFormatClass(SequenceFileInputFormat.class);
            secondJob.setOutputFormatClass(TextOutputFormat.class);
            secondJob.setNumReduceTasks(1);


            FileInputFormat.addInputPath(secondJob, middleOut);
            FileOutputFormat.setOutputPath(secondJob, finalOut);

            secondJob.waitForCompletion(true);
        }
    }

}