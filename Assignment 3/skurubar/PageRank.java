package org.myorg;

/*
 * Name: Sanju Kurubara Budi Hall Hiriyanna Gowda
 * Email ID: skurubar@uncc.edu
 * Uncc Id: 800953525
 * 
 * */



import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class PageRank extends Configured implements Tool {

	private static final String DOCSIZE = "docsize";

	public static void main( String[] args) throws  Exception {
		int res  = ToolRunner .run( new PageRank(), args);
		System .exit(res);
	}

	// Thread to execute Map and Reduce jobs
	public int run( String[] args) throws  Exception {

		// Setting up first job to run first Map and Reduce task. It generates count of docs
		Job job1  = Job .getInstance(getConf(), " pagerank ");
		job1.setJarByClass( this .getClass());

		FileInputFormat.addInputPaths(job1,  args[0]);
		FileOutputFormat.setOutputPath(job1,  new Path(args[1]+"count"));
		job1.setMapperClass( MapCount .class);
		job1.setReducerClass( ReduceCount .class);
		job1.setOutputKeyClass( Text .class);
		job1.setOutputValueClass( IntWritable .class);

		if (!job1.waitForCompletion(true))
			return 0;

		FileSystem fs = FileSystem.get(getConf());
		fs.delete(new Path(args[1]+"count"), true);

		//to get document size from first mapreduce job
		long docSize = job1.getCounters().findCounter("Result", "Result").getValue();

		// Setting up second job to run second Map and Reduce task. It generates 1st page rank values
		Job job2 = Job.getInstance(getConf(), " pagerank ");
		job2.setJarByClass( this .getClass());
		job2.setMapperClass(InitialMap.class);
		job2.setReducerClass(InitialReduce.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		//to use this document size for calculating initial pagerank
		job2.getConfiguration().setStrings(DOCSIZE, docSize + "");
		FileInputFormat.addInputPaths(job2,  args[0]);
		FileOutputFormat.setOutputPath(job2,  new Path(args[1]+"0"));
		if (!job2.waitForCompletion(true))
			return 0;

		//infinite iterations to converge the page rank values. When converged, loop will exit.
		int i = 1;
		for  (;;){
			Boolean converged =  true;
			
			Job job3  = Job.getInstance(getConf(), " pagerank ");
			job3.setJarByClass( this.getClass());
			FileInputFormat.addInputPaths(job3,  args[1]+(i-1));
			FileOutputFormat.setOutputPath(job3, new Path(args[1]+i));  
			job3.setMapperClass( MapRankIterate.class);
			job3.setReducerClass( ReduceRankIterate.class);
			job3.setOutputKeyClass( Text.class);
			job3.setOutputValueClass( Text.class);

			if (!job3.waitForCompletion(true))
				return 0;

			fs.delete(new Path(args[1]+(i-1)), true);
			i++;
			
			//getting the convergence value through counter
			if (job3.getCounters().findCounter("Result", "Result").getValue() == 0)
				converged = false;
			else
				converged = true;
			if (converged == true)
			{
				System.out.println("converged at " + i +"th iteration");
				break;
			}

		}

		//job to sort the page rank values
		Job job4 = Job.getInstance(getConf(), " pagerank ");
		job4.setJarByClass(this.getClass());
		job4.setMapperClass(SortMap.class);
		job4.setReducerClass(SortReduce.class);
		job4.setMapOutputKeyClass(DoubleWritable.class);
		job4.setMapOutputValueClass(Text.class);

		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(DoubleWritable.class);

		// sort the output in descending order
		job4.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		// setting 1 reduce task for sorting
		job4.setNumReduceTasks(1);
		FileInputFormat.addInputPaths(job4, args[1] +(i-1) );
		FileOutputFormat.setOutputPath(job4, new Path(args[1]));

		if (!job4.waitForCompletion(true))
			return 0;

		fs.delete(new Path(args[1]+(i-1)), true);
		return 1;
	}

	//map task to get the count of total number of documents
	public static class MapCount extends
	Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String line = lineText.toString();
			try
			{
				if (line != null && !line.trim().isEmpty()) {
					// parse title in document using line text
					String title = StringUtils.substringBetween(line, "<title>", "</title>");
					context.write(new Text(title), new IntWritable(1));
				}
			}
			catch(Exception e)
			{
				return;
			}
		}
	}

	//reduce task to get the count of total number of documents
	public static class ReduceCount extends
	Reducer<Text, IntWritable, Text, IntWritable> {

		int count = 0;
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException,
				InterruptedException {
			count++;
		}

		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			//storing result for next mapreduce
			context.getCounter("Result", "Result").increment(count);
		}
	}

	//	InitialMap takes the document as input
	//	Input: <title>Page 1</title> <text>[[Page 2]][[Page 3]]</text>
	//	Output: Key->Page 1 Value->###(Initial Rank)###Page 2@@@Page 3
	public static class InitialMap extends Mapper<LongWritable, Text, Text, Text> {

		double docsize;

		public void setup(Context context) throws IOException, InterruptedException{
			docsize = context.getConfiguration().getDouble(DOCSIZE, 1);
		}

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString();
			try
			{
				if (line != null && !line.isEmpty()) {
					// parse the title from line
					String title = StringUtils.substringBetween(line, "<title>", "</title>");
					title.replace(" ", "_");
					//parse the linked pages for "title" 
					String pages[] = StringUtils.substringsBetween(line, "[[", "]]");

					//initial default rank for all pages
					double defaultrank = (double)1/(docsize);
					String rank = "###" + defaultrank + "###";
					boolean first = true;

					for (int i = 0; i < pages.length; i++) 
					{
						if (!first) 
						{
							rank = rank + "@@@";
						}
						rank = rank + pages[i].toString().trim();
						first = false;
					}
					context.write(new Text(title), new Text(rank));
				}
			}
			catch(Exception e)
			{
				return;
			}
		}

	}

	//	InitialReduce returns first pagerank values for documents
	//	Input: Key->Page 1 Value->###(Initial Rank)###Page 2@@@Page 3
	//	Output: Prints as it is
	public static class InitialReduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text title, Iterable<Text> link, Context context) throws IOException, InterruptedException {

			for (Text t : link)
			{
				context.write(title, new Text(t.toString()));	
			}
		}
	}

	//	MapRankIterate returns iterative pagerank values for documents
	//	Input: Key->Page 1, Value->###(Page Rank to be updated)###Page 2@@@Page 3
	//	Output: Key->Page 1, Value->###(Page rank to be updated)###Page 2@@@Page 3
	//			Key->Page 2^^$$, Value->###0.10701937
	//			Key->Page 3^^$$, Value->###0.10701937
	public static class MapRankIterate extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {
			String line  = lineText.toString();

			String[] values=line.split("###");		        
			float initialrank =Float.parseFloat(values[1]);

			if(values.length==3)
			{
				//collecting outlinks
				values[0] = values[0].trim();
				String[] outlinks=values[2].split("@@@");
				System.out.println("Mapper"+ values[0].toString() + "###"+values[1].toString()+"###" + values[2].toString() + " " +outlinks.length);
				//to identify between existing titles and sinking links
				context.write(new Text(values[0]), new Text("$$$"));
				for(int j=0;j< outlinks.length;j++)
				{	
					String outlinkPage= outlinks[j].trim();
					String pageRankTotalLinks = values[0] + "###"+initialrank +"###"+ outlinks.length;
					System.out.println("Mapper" + outlinkPage+ "-->"+"Pageranktotals = "+pageRankTotalLinks);
					
					context.write(new Text(outlinkPage), new Text(pageRankTotalLinks));
				}
				System.out.println(values[0]+ "-->"+"values[2] = &&&"+values[2]);
				//sending initial rank to find convergence
				context.write(new Text(values[0]), new Text("&&&" + values[2] + "&&&" + initialrank));
			}

			else if(values.length<3)
			{
				//for no outlinks
				System.out.println(values[0]+ "-->"+"values[2] = ###"+values[2]);
				context.write(new Text(values[0]),new Text("###"+values[1])); 
			}
		}
	}

	//	ReduceRankIterate returns iterative pagerank values for documents
	//	Input: Key->Page 1, Value->###(Page rank to be updated)###Page 2@@@Page 3
	//			Key->Page 2^^$$, Value->###0.10701937
	//			Key->Page 3^^$$, Value->###0.10701937
	//  Output: Key->Page 1, Value->###(Updated page rank)###Page 2@@@Page 3
	public static class ReduceRankIterate extends Reducer<Text ,  Text ,  Text ,  Text > {

		boolean converged = true;
		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context)
				throws IOException,  InterruptedException 
		{
			String outlinks = "";
			String titleRank;
			String[] rankOutlink;
			String[] parentValues = null;
			boolean titleExists = false;
			float totalSharedRank = 0.0f;
			int roundPrecision = 1000;
			
			for (Text value : counts){
				System.out.println("Key = "+word.toString()+"--->value="+value.toString());
				titleRank = value.toString();

				//filtering unwanted sinking links in the output
				if(titleRank.equals("$$$")) {
					titleExists = true;
					continue;
				}

				//getting outlinks and previous pagerank of title to update
				if(titleRank.startsWith("&&&")){
					String temp = titleRank.substring(3);

					parentValues = temp.split("&&&");
					System.out.println("Parent values"+ parentValues[0] +"---" +parentValues[1]);
					outlinks = "###"+parentValues[0];
					continue;
				}

				rankOutlink = titleRank.split("###");

				float parentPageRank = Float.valueOf(rankOutlink[1]);
				int parentOutLinksCount = Integer.valueOf(rankOutlink[2]);

				//calculating total shared rank of all the inlinks
				totalSharedRank += (parentPageRank/parentOutLinksCount);
			}

			if(!titleExists) return;

			//calculating new page rank
			float newRank = 0.85f * totalSharedRank + 0.15f;
			double originalRank = (double)Math.round(Double.parseDouble(parentValues[1])*roundPrecision)/roundPrecision;
			double latestRank = (double)Math.round(newRank*roundPrecision)/roundPrecision;
			//condition for convergence
			if ((originalRank!=latestRank) && (converged == true))
			{
				converged = false;
				System.out.println("convergence false");
			}
			context.write(word, new Text("###"+newRank + outlinks));
		}



		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			//storing result for next mapreduce
			System.out.println("Cleanup converged:"+converged);
			//setting the value to terminate infinite loop in main function after convergence 
			if (converged)
				context.getCounter("Result", "Result").increment(1);
			else
				context.getCounter("Result", "Result").increment(0);
		}
	}

	//Sorting the data in descending order
	//Input: Key->Page 1, Value->###(Final page rank)###Page 2@@@Page 3
	//Output: Page 1	(Final page rank)
	public static class SortMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			try{
				// removing unnecessary things
				String line = lineText.toString();
				String[] pageValues = line.split("###");

				context.write(new DoubleWritable(Double.parseDouble(pageValues[1].trim())), new Text(pageValues[0].trim()));
			}catch(Exception e){
				return;
			}
		}
	}

	//Sorting the data in descending order
	//Input: Key->Page 1, Value->###(Final page rank)###Page 2@@@Page 3
	//Output: Page 1	(Final page rank)
	public static class SortReduce extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

		public void reduce(DoubleWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			double temp = key.get();
			for(Text page : value){

				context.write(page, new DoubleWritable(temp));
			}
		}
	}
}
