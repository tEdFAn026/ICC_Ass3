/*

  Example solution to CO7219 Assignment 3, Task 1

  The mapper extracts the relevant fields from the input line and emits a key-value
  pair with the airport as key and a String containing four numbers separated by
  commas as value: number of matched flights for arrivals, total delay of arrivals,
  number of matched flights for departures, total delay of departures. Two of the
  four numbers are always 0 because each call of the map method processes either
  arrivals or departures. The total delay is calculated by multiplying the number
  of matched flights with the average delay.

  The combiner aggregates the values for each airport by calculating the sum
  of each of the four components.

  The reducer does the same aggregation as the combiner and then calculates
  the average delay of arrivals by dividing the total delay of arrivals by
  the number of arrivals, and similarly for departures.

*/

package org.leicester;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Delay {

	public static class DelayMapper
		extends Mapper<Object, Text, Text, Text>{

			private Text airport = new Text(); // for mapper output key
			private Text value = new Text(); // for mapper output value

			// split a string into fields separated by commas, ignoring commas inside quoted strings
			public static String[] splitbycomma(String S) {
				ArrayList<String> L = new ArrayList<String>();
				String[] a = new String[0];
				int i = 0;
				while (i<S.length()) {
					int start = i;
					int end=-1;
					if (S.charAt(i)=='"') {
						end = S.indexOf('"',i+1);
					} 
					else {
						end = S.indexOf(',',i)-1;
						if (end<0) end = S.length()-1;
					}
					L.add(S.substring(start,end+1));
					i = end+2;
				}
				return L.toArray(a);
			}

			public void map(Object key, Text value, Context context
				       ) throws IOException, InterruptedException {
				String S = value.toString();
				String[] A = splitbycomma(value.toString());

				// skip empty lines and header lines
				if (A.length==0 || A[0].equals("run_date")) return;

				if (A[7].trim().equals("S")) { // process only scheduled flights

					int numFlights = Integer.parseInt(A[8].trim()); // number of matched flights

					if (numFlights==0) return; // skip input lines with 0 matched flights

					double delay = Double.parseDouble(A[16].trim()) * numFlights; // total delay of matched flights

					// create value v to be output in the format (num_arrivals,total_arrival_delay,num_departures,total_departure_delay)
					String v;
					if (A[6].trim().equals("D")) v = "0,0,"+numFlights+","+delay;
					else if (A[6].trim().equals("A")) v = numFlights+","+delay+",0,0";
					else return;

					airport.set(A[2]);
					value.set(v);
					context.write(airport, value);
				}
			}
		}

	public static class DelayCombiner
		extends Reducer<Text,Text,Text,Text> {
			static Text result = new Text();

			public void reduce(Text key, Iterable<Text> values,
					Context context
					) throws IOException, InterruptedException {
				double sumA = 0; // arrival delay
				double sumD = 0; // departure delay
				double countA = 0; // number of arrivals
				double countD = 0; // number of departures

				// loop through all values and aggregate them
				for (Text val : values) {
					String v = val.toString();
					String[] L = v.split(",");
					countA += Double.parseDouble(L[0]);
					sumA += Double.parseDouble(L[1]);
					countD += Double.parseDouble(L[2]);
					sumD += Double.parseDouble(L[3]);
				}

				// emit one key-value pair with the aggregated values
				result.set(countA+","+sumA+","+countD+","+sumD);
				context.write(key, result);
			}
		}

	public static class DelayReducer
		extends Reducer<Text,Text,Text,Text> {
			static Text result = new Text();

			public void reduce(Text key, Iterable<Text> values,
					Context context
					) throws IOException, InterruptedException {

				// do the same aggregation as in the combiner
				double sumA = 0;
				double sumD = 0;
				double countA = 0;
				double countD = 0;
				for (Text val : values) {
					String v = val.toString();
					String[] L = v.split(",");
					countA += Double.parseDouble(L[0]);
					sumA += Double.parseDouble(L[1]);
					countD += Double.parseDouble(L[2]);
					sumD += Double.parseDouble(L[3]);
				}

				// compute average delays and output them
				double resA = sumA/countA;
				double resD = sumD/countD;
				result.set(resA+","+resD);
				context.write(key, result);
			}
		}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Delay");
		job.setJarByClass(Delay.class);
		job.setMapperClass(DelayMapper.class);
		job.setCombinerClass(DelayCombiner.class);
		job.setReducerClass(DelayReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/delay"));
		

		// 判断output文件夹是否存在，如果存在则删除
		Path path = new Path(args[1]+"/delay");// 取第1个表示输出目录参数（第0个参数是输入目录）
		FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
		}

		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
