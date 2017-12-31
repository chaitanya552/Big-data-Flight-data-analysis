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

public class Cancellation {
	public static TreeSet<OutputPair> sortedOutput = new TreeSet<>();

	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] elements = value.toString().split(",");

			String canc = elements[22].trim();
			if ((!canc.equalsIgnoreCase("")) && !canc.equalsIgnoreCase("CancellationCode") && !canc.equalsIgnoreCase("NA")) {

				// int taxiIn = Integer.parseInt(In);
				context.write(new Text(canc), new LongWritable(1));
			}

		}

	}

	public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

		private DoubleWritable sumCount = new DoubleWritable();
		private DoubleWritable relativeCount = new DoubleWritable();
		private Text currentElement = new Text("B-L-A-N-K / E-M-P-T-Y");
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			long result1;
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result1 = sum;
			result.set(sum);
			sortedOutput.add(new OutputPair(result1, key.toString()));
			context.write(key, result);
			if (sortedOutput.size() > 1) {
				sortedOutput.pollLast();
			}

		}

	}

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(Cancellation.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.waitForCompletion(true);

		File file1 = new File(args[1] + "/MostcommonReason.txt");
		file1.createNewFile();
		FileWriter fw1 = new FileWriter(file1);
		for (OutputPair now : sortedOutput) {
			String code = now.key ;
			System.out.println(code);
			if(code.equals("A")){
				fw1.write("Cancellation Reason  -> Carrier    No of happenings -> " + now.result2 + "\n");
			}
			else if(code.equals("B")){
				fw1.write("Cancellation Reason  -> weather    No of happenings -> " + now.result2 + "\n");
			}
			else if(code.equals("C")){
				fw1.write("Cancellation Reason  -> NAS    No of happenings -> " + now.result2 + "\n");
			}
			else if(code.equals("D")){
				fw1.write("Cancellation Reason  -> security    No of happenings -> " + now.result2 + "\n");
			}
			else if(code.equals(null)){
				fw1.write("Cancellation Reason -> NA");
			}
		}
		fw1.close();
		/*
		 * fw1.close(); File file2 = new File(args[1] + "/bottom3.txt");
		 * file2.createNewFile(); FileWriter fw2 = new FileWriter(file2); for
		 * (OutputPair now : sortedOutput2) { fw2.write("Airport name ->  " +
		 * now.key + "    Lowest average time-> " + now.average + "\n"); }
		 * fw2.close();
		 */

	}

	public static class OutputPair implements Comparable<OutputPair> {
		long result2;
		String key;

		OutputPair(long result2, String key) {
			this.result2 = result2;
			this.key = key;
		}

		@Override
		public int compareTo(OutputPair outputPair) {

			if (this.result2 <= outputPair.result2) {

				return 1;
			} else {
				return -1;
			}

		}
	}
}
