import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class TaxiAverage {
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
			String airportorigin = elements[16].trim();
			String airportdep = elements[17].trim();
			String  In = elements[19].trim();
			String  Out = elements[20].trim();

			if(isInteger(In)){
				int taxiIn = Integer.parseInt(In);
				context.write(new Text(airportorigin), new LongWritable(taxiIn));
			}
			if(isInteger(Out)){
				int taxiOut = Integer.parseInt(Out);
				context.write(new Text(airportdep), new LongWritable(taxiOut));
			}   

		}
		public static boolean isInteger(String s) {
			boolean isValidInteger = false;
			try
			{
				Integer.parseInt(s);

				// s is a valid integer

				isValidInteger = true;
			}
			catch (NumberFormatException ex)
			{
				// s is not an integer
			}

			return isValidInteger;
		}
	}

	public static class Reduce extends Reducer<Text, LongWritable, Text, Text> {

		private DoubleWritable sumCount = new DoubleWritable();
		private DoubleWritable relativeCount = new DoubleWritable();

		public void reduce(Text key, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			int count =0;
			long TotalValue = 0;
			double average = 0.0;
			for (LongWritable value : values) {
				TotalValue = TotalValue+value.get();
				count++;
			}
			average = TotalValue/count;
			sortedOutput.add(new OutputPair(average, key.toString()));
			sortedOutput2.add(new OutputPair(average, key.toString()));
//System.out.println(sortedOutput.size()+ " soterd output size check ");
			if (sortedOutput.size() > 3) {
				sortedOutput.pollLast();
				
			}
			if (sortedOutput2.size() > 3) {
				sortedOutput2.pollFirst();
				
			}
			context.write(key,new Text(Double.toString(average)));
		}
	}


	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(TaxiAverage.class);
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
			fw1.write( "Airport name  ->  " + now.key + "    Highest average time-> " + now.average + "\n");
		}
		fw1.close();
		File file2 = new File(args[1] + "/bottom3.txt");
		file2.createNewFile();
		FileWriter fw2 = new FileWriter(file2);
		for (OutputPair now : sortedOutput2) {
			fw2.write( "Airport name ->  " + now.key + "    Lowest average time-> " + now.average + "\n");
		}
		fw2.close();



	}

	public static class OutputPair implements Comparable<OutputPair> {
		double average;
		String key;

		OutputPair(double average, String key) {
			this.average= average;
			this.key = key;
		}

		@Override
		public int compareTo(OutputPair outputPair) {

			if(this.average<=outputPair.average){

				return 1;
			} else {
				return -1;
			}

		}
	}
}
