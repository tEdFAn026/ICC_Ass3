/*
 * zf41 solution to CO7219 Assignment 3, Task 2 
 * 
 * The mapper extracts the relevant fields from the input line and emits a 
 * key-value pair with the apps name as key and a String containing 3 
 * elements separated by commas as value: category, number_of_view, sentiment 
 * polarity. And Two of the four numbers will be 0 when get data from file 
 * A because file A do not contain these data and we only use file A to get 
 * apps which content rating is for every one. Also category will be empty
 * when we get data from file B. 
 * 
 * The combiner aggregates the values for each app by calculating the sum of 
 * each of the number_of_view and sentiment polarity and also set the category 
 * if it is not empty.
 * 
 * The reducer does the same aggregation as the combiner and then calculates
 * the average polarity of apps, after that when the app's category is empty 
 * because when category is empty it means we do not find this app in file A,
 * in other words this app's content rating is not "Everyone". Or App has less 
 * than 50 reviews or Sentiment_polarity is lower than 0.3 will not be emited.
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
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sentiment {

//	public static class SentimentAppsMapper extends Mapper<Object, Text, Text, Text> {
//		private Text app = new Text();
//		
//		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//			// regex for split string from csv
//			String csvSplitBy = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
//
//			String[] lineElements = value.toString().split(csvSplitBy);
//
//			// skip empty lines and header lines
//			if (lineElements.length == 0 || lineElements[0].equals("App"))
//				return;
//
//			// create value v to be output in the format
//			// (num_Free,0,num_Paid,Paid_price)
////			for(String s: lineElements)
////				System.out.println(s);
//			
//			String emitValue;
//			if (lineElements[6].trim().equals("Free"))
//				emitValue = "1,0,0,0";
//			else if (lineElements[6].trim().equals("Paid"))
//				emitValue = "0,0,1," + lineElements[7];
//			else
//				return;
//
//			// emit
//			app.set(lineElements[1].trim());
//			value.set(emitValue);
//			context.write(app, value);
//		}
//	}
//
//	public static class SentimentUsersMapper extends Mapper<Object, Text, Text, Text> {
//		private Text app = new Text();
//		
//		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//			// regex for split string from csv
//			String csvSplitBy = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
//
//			String[] lineElements = value.toString().split(csvSplitBy);
//
//			// skip empty lines and header lines
//			if (lineElements.length == 0 || lineElements[0].equals("App"))
//				return;
//
//			// create value v to be output in the format
//			// (num_Free,0,num_Paid,Paid_price)
//			for(String s: lineElements)
//				System.out.println(s);
//
//			// emit
//			app.set(lineElements[0].trim());
//			value.set(lineElements[0].trim());
//			context.write(app, value);
//		}
//	}
	
	public static class SentimentMapper extends Mapper<Object, Text, Text, Text> {
		private Text app = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// regex for split string from csv
			// https://blog.csdn.net/wwd0501/article/details/53333384
			String csvSplitBy = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

			String[] lineElements = value.toString().split(csvSplitBy);

			// skip empty lines and header lines
			if (lineElements.length == 0 || lineElements[0].equals("App"))
				return;

			String category = "", number_of_view = "0", sentiment_polarity = "0";

			//file A(googleplaystore.csv) or file format like that have 13 columns
			if (lineElements.length == 13) {

				if (lineElements[8].equals("Everyone")) {
					category = lineElements[1];
				}
			}

			//file B(googleplaystore_user_reviews.csv) or file format like that have 5 columns
			if (lineElements.length == 5) {
				// Sentiment_polarity is not 'nan'
				if (/* !lineElements[1].equals("nan") && */!lineElements[2].equals("nan")) {
					sentiment_polarity = lineElements[3];
					number_of_view = "1";
				}

			}

			// create value emitValue to be output in the format
			// (category, number_of_view, sentiment_polarity)

			String emitValue = category + "," + number_of_view + "," + sentiment_polarity;

			// emit
			app.set(lineElements[0].trim());
			value.set(emitValue);
			context.write(app, value);
		}
	}

	public static class SentimentCombiner extends Reducer<Text, Text, Text, Text> {
		static Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String category = ""; // category
			int number_of_view = 0; // number_of_view
			// I use BigDecimal instead of double or float because for avoiding
			// loss of precision
			BigDecimal sentiment_polarity = new BigDecimal("0"); // total
																	// polarity

			for (Text val : values) {
				System.out.println(val);
				String[] v = val.toString().split(",");
				if (!v[0].isEmpty())
					category = v[0]; // set category if the v[0] which store the
										// categorys is not empty
				number_of_view += Integer.parseInt(v[1]);
				sentiment_polarity = sentiment_polarity.add(new BigDecimal((v[2].trim())));
			}

			// emit
			result.set(category + "," + number_of_view + "," + sentiment_polarity);
			context.write(key, result);

		}
	}


	public static class SentimentReducer extends Reducer<Text, Text, Text, Text> {
		static Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// do the same aggregation as in the combiner
			String category = ""; // category
			int number_of_view = 0; // number_of_view
			// I use BigDecimal instead of double or float because for avoiding
			// loss of precision
			BigDecimal sentiment_polarity = new BigDecimal("0"); // total
																	// polarity

			for (Text val : values) {
				String[] v = val.toString().split(",");
				if (!v[0].isEmpty())
					category = v[0];
				number_of_view += Integer.parseInt(v[1]);
				sentiment_polarity = sentiment_polarity.add(new BigDecimal((v[2].trim())));
			}

			BigDecimal avgpolarity = div(sentiment_polarity, new BigDecimal(String.valueOf(number_of_view)), 2);

			// not emit Content Rating is not "Everyone" or App has less than 50
			// reviews or Sentiment_polarity is lower than 0.3
			if (category.isEmpty() || number_of_view < 50 || avgpolarity.compareTo(new BigDecimal(0.3)) < 0)
				return;
			// compute average delays and output them
			result.set(category + ", " + number_of_view + ", " + avgpolarity.toString());
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

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		praseArgs(args);

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Reviews  and sentiment polarity");
		job.setJarByClass(Sentiment.class);

		// MultipleInputs.addInputPath(job, new
		// Path(settings.get(SettingType.cmd_dataPath) +
		// "/googleplaystore.csv"),
		// TextInputFormat.class, SentimentAppsMapper.class);
		// MultipleInputs.addInputPath(job,
		// new Path(settings.get(SettingType.cmd_dataPath) +
		// "/googleplaystore_user_reviews.csv"),
		// TextInputFormat.class, SentimentUsersMapper.class);
		job.setMapperClass(SentimentMapper.class);
		job.setCombinerClass(SentimentCombiner.class);
		job.setReducerClass(SentimentReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(settings.get(SettingType.cmd_dataPath)));

		FileOutputFormat.setOutputPath(job, new Path(settings.get(SettingType.cmd_outputPath)));

		// Determine if the output folder exists, delete if it exists
		Path path = new Path(settings.get(SettingType.cmd_outputPath));
		FileSystem fileSystem = path.getFileSystem(conf);// Find this file
															// according to path
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);// True means that even if there is
											// something in the output, it is
											// deleted.
		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	// Get arguments and prase to settings.
	public static void praseArgs(String[] args) {
		// add default setting
		settings.put(SettingType.cmd_dataPath, "GooglePlayStoreData");
		settings.put(SettingType.cmd_outputPath, "output/sentiment");

		ArrayList<String> argsList = new ArrayList<String>(Arrays.asList(args));

		for (int i = 0; i < argsList.size(); i++) {

			switch (argsList.get(i)) {
			case "-output": // argument match, set up output path, if do not
							// follow a cmd, then path not empty
				if ((i + 1) < argsList.size() && !checkCMD(argsList.get(i + 1)))
					settings.put(SettingType.cmd_outputPath, argsList.get(i + 1));
				break;
			case "-data": // same but for data path
				if ((i + 1) < argsList.size() && !checkCMD(argsList.get(i + 1)))
					settings.put(SettingType.cmd_dataPath, argsList.get(i + 1));
				break;
			}

		}

	}

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
