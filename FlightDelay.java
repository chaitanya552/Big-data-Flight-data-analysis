import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.TreeSet;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class FlightDelay {
	public static TreeSet<OutputPair> sortedOutput = new TreeSet<>();
	public static TreeSet<OutputPair> sortedOutput2 = new TreeSet<>();
	// public static TreeSet<OutputPair> HighestProbablitytop3 = new
	// TreeSet<>();
	// public static TreeSet<OutputPair> LowestProbablitytop3 = new TreeSet<>();

	public static class Map extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] elements = value.toString().split(",");
			String airlines = elements[8].trim();
			String arrivalDelay = elements[14].trim();
			String departureDelay = elements[15].trim();
			if (!arrivalDelay.equalsIgnoreCase("ArrDelay")
					&& !arrivalDelay.equalsIgnoreCase("NA")
					&& !departureDelay.equalsIgnoreCase("DepDelay")
					&& !departureDelay.equalsIgnoreCase("NA")) {
				if (Integer.parseInt(arrivalDelay) <= 10
						&& Integer.parseInt(departureDelay) <= 10) {
					// System.out.println(Integer.parseInt(ad));
					context.write(new Text(airlines + " " + "ontime"),
							new LongWritable(1));
				}
				context.write(new Text(airlines + " " + "all"),
						new LongWritable(1));
			}

		}
	}

	public static class Reduce extends Reducer<Text, LongWritable, Text, Text> {

		private DoubleWritable sumCount = new DoubleWritable();
		private DoubleWritable relativeCount = new DoubleWritable();
		private Text currentElement = new Text("B-L-A-N-K / E-M-P-T-Y");

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			String[] keyComp = key.toString().split(" ");
			if (keyComp[1].equals("all")) {
				if (keyComp[0].equals(currentElement.toString())) {
					sumCount.set(sumCount.get() + fetchSumCount(values));
				} else {
					currentElement.set(keyComp[0]);
					sumCount.set(0);
					sumCount.set(fetchSumCount(values));
				}
			} else {
				// on schedule count is count
				// total airlines count is sumCount
				double count = fetchSumCount(values);

				relativeCount.set((double) count / sumCount.get());
				Double relativeCountD = relativeCount.get();
				sortedOutput.add(new OutputPair(relativeCountD,count, key.toString(), currentElement.toString()));
				sortedOutput2.add(new OutputPair(relativeCountD,count, key.toString(), currentElement.toString()));
 //System.out.println(sortedOutput.size()+ " soterd output size check ");
				if (sortedOutput.size() > 3) {
					sortedOutput.pollLast();
					
				}
				if (sortedOutput2.size() > 3) {
					sortedOutput2.pollFirst();
					
				}
				/*
				 * HighestProbablitytop3.add(new
				 * OutputPair(relativeCountD,currentElement.toString()));
				 * LowestProbablitytop3.add(new
				 * OutputPair(relativeCountD,currentElement.toString()));
				 * 
				 * if(HighestProbablitytop3.size()>3){
				 * HighestProbablitytop3.pollLast(); }
				 * if(LowestProbablitytop3.size()>3){
				 * LowestProbablitytop3.pollFirst(); }
				 */

				context.write(key,
						new Text(Double.toString(relativeCountD)));
				//System.out.println("printing in reducer");
				//System.out.println(keyComp[0] + " space"
				//		+ relativeCountD);
			}
		}

		private double fetchSumCount(Iterable<LongWritable> values) {
			double count1 = 0;
			for (LongWritable value : values) {
				count1 += value.get();
			}
			return count1;
		}

	}

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(FlightDelay.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.waitForCompletion(true);
		
		File file1 = new File(args[1] + "/top3.txt");
		file1.createNewFile();
		FileWriter fw1 = new FileWriter(file1);
		for (OutputPair now : sortedOutput) {
			fw1.write( "Airlines name  ->  " + now.value + "    Highest Probability for being on schedule-> " + now.relativeFrequency + "\n");
		}
		fw1.close();
		File file2 = new File(args[1] + "/bottom3.txt");
		file2.createNewFile();
		FileWriter fw2 = new FileWriter(file2);
		for (OutputPair now : sortedOutput2) {
			fw2.write( "Airlines name ->  " + now.value + "    Lowest Probability for being on schedule-> " + now.relativeFrequency + "\n");
		}
		fw2.close();
		
		  
		 
	}

	public static class OutputPair implements Comparable<OutputPair> {
		double relativeFrequency;
		double count;
		String key;
		String value;

		OutputPair(double relativeFrequency, double count, String key, String value) {
			this.relativeFrequency = relativeFrequency;
			this.count = count;
			this.key = key;
			this.value = value;
		}

		@Override
		public int compareTo(OutputPair outputPair) {
			
				if(this.relativeFrequency<=outputPair.relativeFrequency){
				
				return 1;
			} else {
				return -1;
			}
			
		}
	}
}
