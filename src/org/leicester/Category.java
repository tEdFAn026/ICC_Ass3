/*
  Example solution for CO7219 Assignment 3 Task 1, 2018/19 Semester 1

  The mapper emits the app category as key and a value of the
  form 1,0,0 for free apps and of the form 0,1,price for paid apps.

  The combiner aggregates values by adding up each of the three
  components.

  The reducer does the same aggregation and then computes the
  average price of paid apps and makes the output.

*/
package org.leicester;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Category {

  public static class CSVMapper
       extends Mapper<Object, Text, Text, Text>{

	// split a line from a csv file into fields, returned as String-array
	public static String[] splitbycomma(String S) {
            ArrayList<String> L = new ArrayList<String>();
            String[] a = new String[0];
			StringBuffer B = new StringBuffer();
            int i = 0;
            while (i<S.length()) {
                    int start = i;
                    int end=-1;
                    if (S.charAt(i)=='"') { // parse field enclosed in quotes
		            	B.setLength(0); // clear the StringBuffer
						B.append('"');
						i++;
						while (i<S.length()) {
							if (S.charAt(i)!='"') { // not a quote, just add to B
								B.append(S.charAt(i));
								i++;
							}
							else { // quote, need to check if next character is also quote
								if (i+1 < S.length() && S.charAt(i+1)=='"') {
									B.append('"');
									i+=2;
								}
								else { // no, so the quote marked the end of the String
									B.append('"');
									L.add(B.toString());
									i+=2;
									break;
								}
							}
						}
                    }
                    else { // standard field, extends until next comma
                            end = S.indexOf(',',i)-1;
                            if (end<0) end = S.length()-1;
                            L.add(S.substring(start,end+1));
                    		i = end+2;
                    }
            }
            return L.toArray(a);
    	}

    private Text category = new Text();
    private Text value = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

	String S = value.toString();

	// skip header line
	if (S.equals("App,Category,Rating,Reviews,Size,Installs,Type,Price,Content Rating,Genres,Last Updated,Current Ver,Android Ver"))
		return;

	String[] A = splitbycomma(S);
	category.set(A[1]);

	if (!A[6].equals("Paid")) { // Free app
		value.set("1,0,0");
	}
	else { // Paif app
		value.set("0,1,"+A[7].substring(1));
	}
        context.write(category, value);
    }
  }

  public static class CategoryCombiner
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int free = 0;
      int paid = 0;
      double sumprice = 0;

      // aggregate free, paid and sumprice from all values received
      for (Text val : values) {
        String[] A = val.toString().split(",");
	free += Integer.parseInt(A[0]);
	paid += Integer.parseInt(A[1]);
	sumprice += Double.parseDouble(A[2]);
      }

      result.set(free+","+paid+","+sumprice);
      context.write(key, result);
    }
  }

  public static class CategoryReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int free = 0;
      int paid = 0;
      double sumprice = 0;

      // same aggregation as in combiner
      for (Text val : values) {
        String[] A = val.toString().split(",");
	free += Integer.parseInt(A[0]);
	paid += Integer.parseInt(A[1]);
	sumprice += Double.parseDouble(A[2]);
      }

      // produce final output
      result.set(free+", "+paid+", "+String.format("%.2f",(sumprice/paid)));
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "app analysis");
    job.setJarByClass(Category.class);
    job.setMapperClass(CSVMapper.class);
    job.setCombinerClass(CategoryCombiner.class);
    job.setReducerClass(CategoryReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
