/*

  Example solution to CO7219 Assignment 3, Task 2

  The mapper extracts the relevant fields from the input line (taking the year from
  the first four characters of the reporting_period field) and emits a key-value
  pair with airport,year as key and a String containing the number of matched flights
  and the number of matched flights that are at least 31 minutes late.
  The latter is calculated by adding up the four percentages for late flights
  and multiplying the sum with the number of matched flights.

  The combiner aggregates the values for each airport,year key by calculating
  the sum of each of the two components.

  The reducer does the same aggregation as the combiner and then calculates
  the percentage of late departures by dividing the number of late flights
  by the total number of flights, and outputs it if it is at least 50%.

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

public class Late {

	public static class AirlineMapper
		extends Mapper<Object, Text, Text, Text>{

			private Text airport_year = new Text(); // mapper output key
			private Text value = new Text(); // mapper output value

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

				if (A[6].equals("D") && A[7].equals("S")) { // process only scheduled departures
					int numFlights = Integer.parseInt(A[8].trim());
					if (numFlights==0) return; // skip records with 0 matched flights

					// calculate number of late flights
					double percentLate = Double.parseDouble(A[12].trim())+Double.parseDouble(A[13].trim())+Double.parseDouble(A[14].trim())+Double.parseDouble(A[15].trim());
					int numLate = (int)Math.round(numFlights*percentLate/100.0);

					// extract year from reporting_period
					String year=A[1].substring(0,4);

					// emit key-value pair
					airport_year.set(A[5]+","+year);
					value.set(numFlights+","+numLate);
					context.write(airport_year, value);
				}
			}
		}

	public static class LateCombiner
		extends Reducer<Text,Text,Text,Text> {
			Text result = new Text();

			public void reduce(Text key, Iterable<Text> values,
					Context context
					) throws IOException, InterruptedException {

				// aggregate values
				int numFlights = 0;
				int numLate = 0;
				for (Text val : values) {
					String v = val.toString();
					String[] L = v.split(",");
					numFlights += Integer.parseInt(L[0]);
					numLate += Integer.parseInt(L[1]);
				}
				// output aggregated key-value pair
				result.set(numFlights+","+numLate);
				context.write(key, result);
			}
		}

	public static class LateReducer
		extends Reducer<Text,Text,Text,DoubleWritable> {

			public void reduce(Text key, Iterable<Text> values,
					Context context
					) throws IOException, InterruptedException {
				int numFlights = 0;
				int numLate = 0;
				for (Text val : values) {
					String v = val.toString();
					String[] L = v.split(",");
					numFlights += Integer.parseInt(L[0]);
					numLate += Integer.parseInt(L[1]);
				}
				double res = 100.0*numLate/(double)numFlights;
				if (res>=50) {
					DoubleWritable result = new DoubleWritable(res);
					context.write(key, result);
				}
			}
		}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Late.class);
		job.setMapperClass(AirlineMapper.class);
		job.setCombinerClass(LateCombiner.class);
		job.setReducerClass(LateReducer.class);

		// set final output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// set Mapper output key and value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/Late"));
		
		// 判断output文件夹是否存在，如果存在则删除
		Path path = new Path(args[1]+"/Late");// 取第1个表示输出目录参数（第0个参数是输入目录）
		FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
		}
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
