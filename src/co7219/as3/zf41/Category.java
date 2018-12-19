/*
 * zf41 solution to CO7219 Assignment 3, Task 1 
 * 
 * The mapper extracts the relevant fields from the input line and emits a 
 * key-value pair with the category as key and a String containing four 
 * numbers separated by commas as value: number of free apps, 0, number of 
 * paid apps, total paid apps price. Two of the four numbers are always 0 
 * because each call of the map method processes either free or paid apps.
 * 
 * The combiner aggregates the values for each category by calculating the
 * sum of each of the four components.
 * 
 * The reducer does the same aggregation as the combiner and then calculates
 * the average paid apps of arrivals by dividing the number of paid apps,
 * and similarly for departures.
 * 
 * please input arguments when you run in command line: 
 * -output "output path" 	set output path
 * -data "data source path"	set data source path
 */

package co7219.as3.zf41;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Category {

	public static class CategoryMapper extends Mapper<Object, Text, Text, Text> {
		private Text category = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// String[] lineElements = splitbycomma(value.toString());

			// regex for split string from csv
			// https://blog.csdn.net/wwd0501/article/details/53333384
			String csvSplitBy = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

			String[] lineElements = value.toString().split(csvSplitBy);

			// skip empty lines and header lines
			if (lineElements.length == 0 || lineElements[0].equals("App"))
				return;

			// file A(googleplaystore.csv) or file format like that have 13
			// columns
			if (lineElements.length != 13)
				return;

			// create value emitValue to be output in the format
			// (num_Free,0,num_Paid,Paid_price)
			String emitValue;
			if (lineElements[6].trim().equals("Free"))
				emitValue = "1,0,0,0";
			else if (lineElements[6].trim().equals("Paid"))
				emitValue = "0,0,1," + lineElements[7];
			else
				return;

			// emit
			category.set(lineElements[1].trim());
			value.set(emitValue);
			context.write(category, value);
		}
	}

	public static class CategoryCombiner extends Reducer<Text, Text, Text, Text> {
		Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sumfree = 0; // number of free apps
			int sumPaid = 0; // number of paid apps
			// I use BigDecimal instead of double or float because for avoiding
			// loss of precision
			BigDecimal sumPrice = new BigDecimal("0"); // total price

			for (Text val : values) {
				String[] v = val.toString().split(",");
				sumfree += Integer.parseInt(v[0]);
				sumPaid += Integer.parseInt(v[2]);
				sumPrice = sumPrice.add(new BigDecimal((v[3].trim().replace("$", ""))));
			}

			// emit one key-value pair with the aggregated values
			result.set(sumfree + ",0," + sumPaid + "," + sumPrice.toString());
			context.write(key, result);

		}
	}

	public static class CategoryReducer extends Reducer<Text, Text, Text, Text> {
		static Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// do the same aggregation as in the combiner
			int sumfree = 0; // number of free apps
			int sumPaid = 0; // number of paid apps
			BigDecimal sumPrice = new BigDecimal("0"); // total price
			for (Text val : values) {
				String[] v = val.toString().split(",");
				sumfree += Integer.parseInt(v[0]);
				sumPaid += Integer.parseInt(v[2]);
				sumPrice = sumPrice.add(new BigDecimal((v[3].trim().replace("$", ""))));
			}

			// compute average delays and output them
			result.set(sumfree + ", " + sumPaid + ", "
					+ div(sumPrice, new BigDecimal(String.valueOf(sumPaid)), 2).toString());
			context.write(key, result);
		}

		/**
		 * Provides (relatively) precise division. When an inexhaustible
		 * situation occurs, the precision is specified by the scale parameter,
		 * and the subsequent numbers are rounded off.
		 * 
		 * @param v1
		 *            is divisor
		 * @param v2
		 *            divisor
		 * @param scale
		 *            indicates that it needs to be accurate to a few decimal
		 *            places.
		 * @return quotient of two parameters
		 */
		public static BigDecimal div(BigDecimal b1, BigDecimal b2, int scale) {

			// Returns 0 if the denominator is 0
			if (b2.compareTo(BigDecimal.ZERO) == 0)
				return b2.setScale(scale, BigDecimal.ROUND_HALF_UP);

			return b1.divide(b2, scale, BigDecimal.ROUND_HALF_UP);
		}
	}

	// Settings for now this program only have two option setting, datapath and
	// output path
	public enum SettingType {
		cmd_dataPath, cmd_outputPath
	}

	// store settings
	private static EnumMap<SettingType, String> settings = new EnumMap<SettingType, String>(SettingType.class);
	// {
	// {
	// settings.put(SettingType.cmd_dataPath, "GooglePlayStoreDataTest");
	// settings.put(SettingType.cmd_outputPath, "output/category");
	// }
	// };

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		praseArgs(args);

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Category type and price");
		job.setJarByClass(Category.class);
		job.setMapperClass(CategoryMapper.class);
		job.setCombinerClass(CategoryCombiner.class);
		job.setReducerClass(CategoryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(settings.get(SettingType.cmd_dataPath)));
		FileOutputFormat.setOutputPath(job, new Path(settings.get(SettingType.cmd_outputPath)));

		// Determine if the output folder exists, delete if it exists
		Path path = new Path(settings.get(SettingType.cmd_outputPath));

		// Find this file according to path
		FileSystem fileSystem = path.getFileSystem(conf);
		if (fileSystem.exists(path)) {
			// True means that even if there is something in the output, it is
			// deleted.
			fileSystem.delete(path, true);
		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	// Get arguments and prase to settings.
	public static void praseArgs(String[] args) {
		// add default setting
		settings.put(SettingType.cmd_dataPath, "GooglePlayStoreData");
		settings.put(SettingType.cmd_outputPath, "output/category");

		ArrayList<String> argsList = new ArrayList<String>(Arrays.asList(args));

		for (int i = 0; i < argsList.size(); i++) {

			switch (argsList.get(i)) {
			// argument match, set up output path, if do not
			// follow a cmd, then path not empty
			case "-output":
				if ((i + 1) < argsList.size() && !checkCMD(argsList
						.get(i + 1))/* && checkPath(argsList.get(i + 1)) */)
					settings.put(SettingType.cmd_outputPath, argsList.get(i + 1));
				break;

			// same but for data path
			case "-data":
				if ((i + 1) < argsList.size() && !checkCMD(argsList
						.get(i + 1))/* && checkPath(argsList.get(i + 1)) */)
					settings.put(SettingType.cmd_dataPath, argsList.get(i + 1));
				break;
			}

		}

	}

	// public static boolean checkPath(String path) {
	// String pattern = "[a-zA-Z]:(\\\\([0-9a-zA-Z]+))+|(\\/([0-9a-zA-Z]+))+";
	// return match(pattern, path);
	// }

	// check the string is a cmd or not
	public static boolean checkCMD(String cmd) {
		String pattern = "-.*";
		return match(pattern, cmd);
	}

	private static boolean match(String regex, String str) {
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(str);
		return matcher.matches();
	}
}
