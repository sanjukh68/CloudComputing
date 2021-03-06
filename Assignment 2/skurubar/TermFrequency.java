/*
 * Name: Sanju Kurubara Budi Hall Hiriyanna Gowda
 * Email ID: skurubar@uncc.edu
 * Uncc Id: 800953525
 * 
 * */
package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.lang.Math;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class TermFrequency extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( TermFrequency.class);

	public static void main( String[] args) throws  Exception {
		int res  = ToolRunner .run( new TermFrequency(), args);
		System .exit(res);
	}
	// Thread to execute Map and Reduce jobs
	public int run( String[] args) throws  Exception {

		Job job  = Job .getInstance(getConf(), " termfrequency ");
		job.setJarByClass( this .getClass());

		FileInputFormat.addInputPaths(job,  args[0]);
		FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
		job.setMapperClass( Map .class);
		job.setReducerClass( Reduce .class);
		job.setOutputKeyClass( Text .class);
		job.setOutputValueClass( IntWritable .class);

		return job.waitForCompletion( true)  ? 0 : 1;
	}

	//Mapper Class
	public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
		private final static IntWritable one  = new IntWritable( 1);
		private Text word  = new Text();

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
}
