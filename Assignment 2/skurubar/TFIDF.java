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
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
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


public class TFIDF extends Configured implements Tool {

	public static void main( String[] args) throws  Exception {
		int res  = ToolRunner .run( new TFIDF(), args);
		System .exit(res);
	}

	public int run( String[] args) throws  Exception {
		Configuration conf = new Configuration();
		// Thread to execute Map and Reduce jobs
		Job job  = Job .getInstance(getConf(), " TFIDF ");
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

		int result = 0;
		if (job.waitForCompletion(true)) {
			result = job2.waitForCompletion(true) ? 0 : 1;
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
				String finalWord = new String(word.toString() + "#####" + fileName);
				currentWord  = new Text(finalWord);
				context.write(currentWord,one);
			}
		}
	}

	//1st Reducer Class
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
		MapWritable mapWritable = new MapWritable();

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
				System.out.println(val + "////" + size + "////" + count);
				context.write(new Text(word + "#####" + file), new DoubleWritable((Math.log10(1+(size / count)))*val));			
			}
		}
	}
}

