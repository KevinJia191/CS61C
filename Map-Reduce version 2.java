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

	/** A Number pair class */
	public static class NumPair implements Writable {
		public double dist, count;

		public NumPair() {
			this.dist = dist;
			this.count = count;
		}

		public NumPair(double dist, double count) {
			this.dist = dist;
			this.count = count;
		}

		public void write(DataOutput out) throws IOException {
			new DoubleWritable(dist).write(out);
			new DoubleWritable(count).write(out);
		}

		public void readFields(DataInput in) throws IOException {
			DoubleWritable tmp = new DoubleWritable();
			tmp.readFields(in);
			dist = tmp.get();

			tmp.readFields(in);
			count = tmp.get();
		}

		/** Returns the function of the distance. */
		public double getA() {
			return dist;
		}
		/** Returns the count. */
		public double getB() {
			return count;
		}
	}

	/**
	 * Inputs a set of (docID, document contents) pairs.
	 * Outputs a set of (Text, Text) pairs.
	 */
	public static class Map1 extends Mapper<WritableComparable, Text, Text, NumPair> {
		/** Regex pattern to find words (alphanumeric + _). */
		final static Pattern WORD_PATTERN = Pattern.compile("\\w+");

		private String targetGram = null;
		private ArrayList<String> targetList = new ArrayList<String>();
		private int nGram = -1; //the size of the targetGram phrase (a single word is nGram of 1)
		private int funcNum = 0;

		/*
		 * Setup gets called exactly once for each mapper, before map() gets called the first time.
		 * It's a good place to do configuration or setup that can be shared across many calls to map
		 */
		@Override
		public void setup(Context context) {
			targetGram = context.getConfiguration().get("targetGram").toLowerCase();
			String[] targetListTemp = targetGram.split(" ");
			for(String t : targetListTemp) {
				targetList.add(t);
			}
			nGram = targetListTemp.length;
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

			Text output = new Text();
			NumPair distance = new NumPair();
			distance.count = 1.0;
			ArrayList<String> docContent = new ArrayList<String>();
			ArrayList<Integer> targetPos = new ArrayList<Integer>();

			while (matcher.find()) {
				String w = matcher.group().toLowerCase();
				docContent.add(w);
			}

			for(int i = 0; i <= docContent.size() - nGram; i++) {
				if(docContent.subList(i, i+nGram).equals(targetList)){
					targetPos.add(new Integer(i));
				}
			}

			int docPos = 0;
			if(targetPos.size() <= 1) {
				while(docPos + nGram <= docContent.size()){
					String str = makeNString(docContent.subList(docPos, docPos+nGram));
					output.set(str);
					if(targetPos.size() == 0) {
						distance.dist = 0;
					} else{
						distance.dist = func.f(Math.abs(docPos - targetPos.get(0)));
					}
					if(!str.equals(targetGram)) {
						context.write(output, distance);
					}
					docPos++;
				}
			}
			else if (targetPos.size() > 1) {
				int midPos = -1;

				for(int a = 0; a < targetPos.size(); a++){
					if(a == tawrgetPos.size() - 1) {
						midPos = docContent.size();
					}
					else {
						midPos = (targetPos.get(a) + targetPos.get(a+1)) / 2;
					}

					while(docPos + nGram <= docContent.size() && docPos <= midPos){
						String str = makeNString(docContent.subList(docPos, docPos+nGram));
						output.set(str);
						if(!str.equals(targetGram)) {
							distance.dist = func.f(Math.abs(docPos - targetPos.get(a)));
							context.write(output, distance);
						}
						docPos++;
					}
				}

			}
		}
		private String makeNString(List<String> wordlist) {
			String finalWord = "";
			for(String w : wordlist) {
				finalWord += w + " ";
			}
			return finalWord.substring(0,finalWord.length() - 1);
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
	}

	/** Here's where you'll be implementing your combiner. It must be non-trivial for you to receive credit. */
	public static class Combine1 extends Reducer<Text, NumPair, Text, NumPair> {

		@Override
		public void reduce(Text key, Iterable<NumPair> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			double count = 0.0;
			for (NumPair value : values) {
				sum += value.dist;
				count += value.count;
			}
			context.write(key, new NumPair(sum, count));
		}
	}


	public static class Reduce1 extends Reducer<Text, NumPair, DoubleWritable, Text> {
		@Override
		public void reduce(Text key, Iterable<NumPair> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			double count = 0.0;
			for (NumPair value : values) {
				sum += value.dist;
				count += value.count;
			}
			double coRate = 0;
			if (sum != 0) {
				coRate = ((sum * Math.pow(Math.log(sum), 3))/count) * -1;
			}
			context.write(new DoubleWritable(coRate), key);
		}
	}

	public static class Map2 extends Mapper<DoubleWritable, Text, DoubleWritable, Text> {
		//do nothing
	}

	public static class Reduce2 extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

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
			int count = 0;
			for (Text t : values) {
				key.set(Math.abs(key.get()));
				context.write(key, t);
				count++;
				if (count >= 100) {
				    break;
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
			firstJob.setMapOutputKeyClass(Text.class);
			firstJob.setMapOutputValueClass(NumPair.class);
			firstJob.setOutputKeyClass(DoubleWritable.class);
			firstJob.setOutputValueClass(Text.class);
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
			secondJob.setMapOutputKeyClass(DoubleWritable.class);
			secondJob.setMapOutputValueClass(Text.class);
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