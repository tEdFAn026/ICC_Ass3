/*
  Example solution for CO7219 Assignment 3 Task 1, 2018/19 Semester 1

  The mapper determines by the number of columns in the input line
  whether it is from the app file or from the review file.

  If the line is from the app file and the content rating is "Everyone",
  the mapper emits a key-value pair with the app name as key and
  a value of the form category,0,0.

  If the line is from the review file and the review is valid,
  the mapper emits a key-value pair with the app name as key and
  a value of the form ,1,sentimentpolarity.

  The combiner aggregates values by adding up the second
  and third component, and taking the category if it is
  present in any input values. (As the combiner receives key-value
  pairs from only one mapper, it will either get key-value pairs
  with a category, or key-value pairs with review sentiment
  polarity, but not both.)

  The reducer does the same aggregation as the combiner and then
  checks whether the number of valid reviews is >=50, the average
  sentiment polarity is >=0.3, and a category has been received
  (indicating that the app has content rating "Everyone"). If so,
  the information is output.
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

public class Sentiment {

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

    private Text appName = new Text();
    private Text value = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

	String S=value.toString();

	// skip header line of app file
	if (S.equals("App,Category,Rating,Reviews,Size,Installs,Type,Price,Content Rating,Genres,Last Updated,Current Ver,Android Ver"))
		return;

	// skip header line of review file
	if (S.equals("App,Translated_Review,Sentiment,Sentiment_Polarity,Sentiment_Subjectivity"))
		return;

	String[] A = splitbycomma(S);

	appName.set(A[0]); // app name

	if (A.length==5) { // review file
		if (!A[3].equals("nan")) {
			value.set(",1,"+A[3]); // ,1,polarity
        		context.write(appName, value);
		}
	}
	else { // app file
		if (A[8].equals("Everyone")) {
			value.set(A[1]+",0,0"); // category,0,0
        		context.write(appName, value);
		}
	}
    }
  }

  public static class SentimentCombiner
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
	String category=""; // category
	int num=0; // number of reviews
	double sum=0; // sum of polarities

	// aggregate values
	for (Text val : values) {
       		String T = val.toString();
		String[] A = T.split(",");
		num += Integer.parseInt(A[1]);
		sum += Double.parseDouble(A[2]);
		if (category.isEmpty())
			category = A[0];
      	}

	result.set(category+","+num+","+sum);
	context.write(key, result);
    }
  }

  public static class SentimentReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
	String category=""; // category
	int num=0; // number of reviews
	double sum=0; // sum of polarities

	// aggregate values
	for (Text val : values) {
       		String T = val.toString();
		String[] A = T.split(",");
		num += Integer.parseInt(A[1]);
		sum += Double.parseDouble(A[2]);
		if (category.isEmpty())
			category = A[0];
	}

	// check conditions and, if met, make output
	if (!category.isEmpty() && num >= 50 && sum/num>=0.3) {
		result.set(category+", "+num+", "+String.format("%.2f",sum/num));
		context.write(key, result);
	}
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "app analysis");
    job.setJarByClass(Sentiment.class);
    job.setMapperClass(CSVMapper.class);
    job.setCombinerClass(SentimentCombiner.class);
    job.setReducerClass(SentimentReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
