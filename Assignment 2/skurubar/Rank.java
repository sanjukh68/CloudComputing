/*
 * Name: Sanju Kurubara Budi Hall Hiriyanna Gowda
 * Email ID: skurubar@uncc.edu
 * Uncc Id: 800953525
 * 
 * */

package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.lang.Math;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class Rank extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( Rank.class);

	public static void main( String[] args) throws  Exception {
		int res  = ToolRunner .run( new Rank(), args);
		System .exit(res);
	}

	// Thread to execute Map and Reduce jobs
	public int run( String[] args) throws  Exception {
		Configuration conf = new Configuration();
		
		// Setting up first job to run first Map and Reduce task. It generates 1st intermediate result
		Job job  = Job .getInstance(getConf(), " rank ");
		job.setJarByClass( this .getClass());

		FileInputFormat.addInputPaths(job,  args[0]);
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		job.setMapperClass( Map .class);
		job.setReducerClass( Reduce .class);
		job.setOutputKeyClass( Text .class);
		job.setOutputValueClass( IntWritable .class);

		// getting number of files in the input folder and saving it in configuration
		FileSystem numberOfFile = FileSystem.get(job.getConfiguration());
		FileStatus[] status = numberOfFile.listStatus(new Path(args[0]));
		conf.set("name", Integer.toString(status.length));

		// Setting up second job to run second Map and Reduce task. It generates 2nd intermediate result
		Job job2 = Job.getInstance(conf, " wordcount ");
		job2.setJarByClass( this .getClass());
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPaths(job2, args[1]);
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		conf.set("user_query", args[5]);

		// Setting up third job to run third Map and Reduce task. It generates 3rd intermediate result
		Job job3 = Job.getInstance(conf, " wordcount ");
		job3.setJarByClass( this .getClass());
		job3.setMapperClass(Map3.class);
		job3.setReducerClass(Reduce3.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.addInputPaths(job3, args[2]);
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));

		// Setting up fourth job to run fourth Map and Reduce task. It generates final result
		Job job4 = Job.getInstance(conf, " wordcount ");
		job4.setJarByClass( this .getClass());
		job4.setMapperClass(Map4.class);
		job4.setReducerClass(Reduce4.class);
		job4.setSortComparatorClass(MyComparator.class);
		job4.setOutputKeyClass(DoubleWritable.class);
		job4.setOutputValueClass(Text.class);
		FileInputFormat.addInputPaths(job4, args[3]);
		FileOutputFormat.setOutputPath(job4, new Path(args[4]));

		int result = 0;
		if (job.waitForCompletion(true)) {
			if (job2.waitForCompletion(true))
				if (job3.waitForCompletion(true))
					result = job4.waitForCompletion(true) ? 0 : 1;
		}
		return result;
	}

	//1st Mapper Class
	public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
		private final static IntWritable one  = new IntWritable( 1);

		private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			//getting current input file name 
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

			String line  = lineText.toString();
			Text currentWord  = new Text();

			for ( String word  : WORD_BOUNDARY .split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				//converting uppercase words to lowercase
				word = word.toLowerCase();
				String finalWord = new String(word + "#####" + fileName);
				currentWord  = new Text(finalWord);
				context.write(currentWord,one);
			}
		}
	}

	//1st Reducer class
	public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
				throws IOException,  InterruptedException {
			int sum  = 0;
			for ( IntWritable count  : counts) {
				sum  += count.get();
			}
			//to get Term frequency value
			context.write(word,  new DoubleWritable(1 + Math.log10(sum)));
		}
	}

	//2nd Mapper Class
	public static class Map2 extends Mapper<LongWritable ,  Text ,  Text , Text > {

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			//changing data as per requirement
			String line  = lineText.toString();
			line = line.replace("\t", "=");
			String key = line.substring(0, line.indexOf("#"));
			String value = line.substring(line.lastIndexOf("#")+1);

			context.write(new Text(key),new Text(value));
		}
	}

	//2nd Reducer class
	public static class Reduce2 extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context)
				throws IOException,  InterruptedException {

			//getting number of files
			Configuration conf = context.getConfiguration();
			Double size = Double.parseDouble(conf.get("name"));
			Double count = 0.0;
			ArrayList<String> value = new ArrayList<String>();

			//to find total number of files containing word
			for(Text t : counts){
				String val = t.toString();
				count++;
				value.add(val);
			}

			// to write back into file with TFIDF calculation
			for(int i=0;i<value.size();i++){
				String fn = value.get(i);
				String file = fn.substring(0, fn.lastIndexOf("="));
				Double val = Double.parseDouble(fn.substring(fn.indexOf("=")+1));
				context.write(new Text(word + "#####" + file), new DoubleWritable(Math.log10(1+(size / count))*val));	
			}
		}
	}

	//3rd Mapper Class
	public static class Map3 extends Mapper<LongWritable ,  Text ,  Text , Text > {
		MapWritable mapWritable = new MapWritable();

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {
			String line  = lineText.toString();
			String wordText = line.substring(0, line.indexOf("#"));
			String value = line.substring(line.lastIndexOf("\t")+1);
			String file = line.substring(line.lastIndexOf("#")+1, line.indexOf("\t") );

			Configuration conf = context.getConfiguration();
			String userText = conf.get("user_query");
			
			//converting user entered uppercase value to lower case
			userText = userText.toLowerCase();
			StringTokenizer st = new StringTokenizer(userText, " ");
			while (st.hasMoreTokens())
			{
				if (st.nextToken().compareTo(wordText) == 0)
				{
					context.write(new Text(file),new Text(value));
				}
			}
		}
	}

	//3rd Reducer Class
	public static class Reduce3 extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context)
				throws IOException,  InterruptedException {
			Double sum = 0.0;

			//to calculate total TFIDF value for word in file
			for(Text t : counts){
				Double val = Double.parseDouble(t.toString());
				sum += val;
			}
			context.write(new Text(word), new DoubleWritable(sum));
		}
	}

	//4th Mapper Class
	public static class Map4 extends Mapper<LongWritable ,  Text ,  DoubleWritable , Text > {
		MapWritable mapWritable = new MapWritable();
		private Text word = new Text();
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {
			String line = lineText.toString();
			StringTokenizer st = new StringTokenizer(line);
			while (st.hasMoreTokens())
			{
				word.set(st.nextToken());
				DoubleWritable numberValue = new DoubleWritable(Double.parseDouble(st.nextToken()));
				//storing number as key so that we can sort
				context.write(numberValue,word);
			}

		}
	}

	//Comparator to sort the data
	public static class MyComparator extends WritableComparator {

		protected MyComparator() {
			super(DoubleWritable.class, true);
		}
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleWritable k1 = (DoubleWritable)w1;
			DoubleWritable k2 = (DoubleWritable)w2;

			return -1 * k1.compareTo(k2);
		}
	}
	
	//4th Reducer class
	public static class Reduce4 extends Reducer<DoubleWritable ,  Text ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( DoubleWritable word,  Iterable<Text > counts,  Context context)
				throws IOException,  InterruptedException {
			for (Text file  : counts) {
				context.write(file,  word);
			}
		}
	}
}

