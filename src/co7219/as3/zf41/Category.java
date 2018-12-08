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

		public static String[] splitbycomma(String S) {
			ArrayList<String> L = new ArrayList<String>();
			String[] a = new String[0];
			int i = 0;
			while (i < S.length()) {
				int start = i;
				int end = -1;
				if (S.charAt(i) == '"') {
					end = S.indexOf('"', i + 1);
				} else {
					end = S.indexOf(',', i) - 1;
					if (end < 0)
						end = S.length() - 1;
				}
				L.add(S.substring(start, end + 1));
				i = end + 2;
			}
			return L.toArray(a);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lineElements = splitbycomma(value.toString());

			if (lineElements.length == 0 || lineElements[0].equals("App"))
				return;
			if (lineElements[1].equals("Baby\"\" pistol explained\"")) {
				System.out.println(value.toString());
				System.out.println(lineElements[1]);
			}
			category.set(lineElements[1].trim());
			value.set(lineElements[6] + "," + lineElements[7]);
			context.write(category, value);
		}
	}

	public static class CategoryReducer extends Reducer<Text, Text, Text, Text> {
		static Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int sumfree = 0;
			int sumPaid = 0;
			BigDecimal sumPrice = new BigDecimal("0");
			// ArrayList<Float> price = new ArrayList<Float>();
			for (Text val : values) {
				String[] v = val.toString().split(",");
				if (v[0].trim().equals("Free"))
					sumfree++;
				if (v[0].trim().equals("Paid")) {
					sumPaid++;
					// price.add(Float.parseFloat(v[1]));
//					System.out.print(sumPrice +"+"+""+v[1].trim().replace("$", "")+"=");
					
					sumPrice = sumPrice.add(new BigDecimal((v[1].trim().replace("$", ""))));
//					System.out.println(sumPrice);
				}
			}

			result.set(sumfree + ", " + sumPaid + ", "
					+ div(sumPrice, new BigDecimal(String.valueOf(sumPaid)), 2).toString());
			context.write(key, result);
		}

		public static BigDecimal div(BigDecimal b1, BigDecimal b2, int scale) {

			if (b2.compareTo(BigDecimal.ZERO)==0)
				return b2.setScale(scale, BigDecimal.ROUND_HALF_UP);
			
			return b1.divide(b2, scale, BigDecimal.ROUND_HALF_UP);
		}
	}

	public enum CMD {
		cmd_dataPath, cmd_outputPath
	}

	private static EnumMap<CMD, String> settings = new EnumMap<CMD, String>(CMD.class);{
		{
			settings.put(CMD.cmd_dataPath, "GooglePlayStoreDataTest");
			settings.put(CMD.cmd_outputPath, "output/category");
		}
	};

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		praseArgs(args);

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Category type and price");
		job.setJarByClass(Category.class);
		job.setMapperClass(CategoryMapper.class);
		// job.setCombinerClass(CategoryCombiner.class);
		job.setReducerClass(CategoryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(settings.get(CMD.cmd_dataPath)));
		FileOutputFormat.setOutputPath(job, new Path(settings.get(CMD.cmd_outputPath)));

		// 判断output文件夹是否存在，如果存在则删除
		Path path = new Path(settings.get(CMD.cmd_outputPath));// 取第1个表示输出目录参数（第0个参数是输入目录）
		FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static void praseArgs(String[] args) {
		// add default setting
		settings.put(CMD.cmd_dataPath, "GooglePlayStoreDataTest");
		settings.put(CMD.cmd_outputPath, "output/category");

		ArrayList<String> argsList = new ArrayList<String>(Arrays.asList(args));
//		System.out.println(settings);

		for (int i = 0; i < argsList.size(); i++) {
			// System.out.println( argsList.get(i));
			switch (argsList.get(i)) {
			case "-output":
				if ((i + 1) < argsList.size() && !checkCMD(argsList
						.get(i + 1))/* && checkPath(argsList.get(i + 1)) */)
					settings.put(CMD.cmd_outputPath, argsList.get(i + 1));
				break;
			case "-data":
				if ((i + 1) < argsList.size() && !checkCMD(argsList
						.get(i + 1))/* && checkPath(argsList.get(i + 1)) */)
					settings.put(CMD.cmd_dataPath, argsList.get(i + 1));
				break;
			}

		}

//		System.out.println(settings);
	}

	// public static boolean checkPath(String path) {
	// String pattern = "[a-zA-Z]:(\\\\([0-9a-zA-Z]+))+|(\\/([0-9a-zA-Z]+))+";
	// return match(pattern, path);
	// }

	public static boolean checkCMD(String path) {
		String pattern = "-.*";
		return match(pattern, path);
	}

	private static boolean match(String regex, String str) {
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(str);
		return matcher.matches();
	}
}
