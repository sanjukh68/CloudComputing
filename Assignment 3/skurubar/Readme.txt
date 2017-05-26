Name: Sanju Kurubara Budi Hall Hiriyanna Gowda
UNCC Id: 800953525

At any point of time please make sure to delete output folder to execute the code again next time.
Step 1: Initial Setup
	$ sudo su hdfs
	$ hadoop fs -mkdir /user/cloudera
	$ hadoop fs -chown cloudera /user/cloudera
	$ exit
	$ sudo su cloudera
	$ hadoop fs -mkdir /user/cloudera/pagerank /user/cloudera/pagerank/input

Step 2. Copy the input file into the created input directory
	hadoop fs -put <input file to be copied> /user/cloudera/pagerank/input/

Step 3: To execute PageRank.java
	$hadoop fs -rm -r <output path>		(to remove output directory if it exists)
		For eg: $hadoop fs -rm -r /user/cloudera/wordcount/output 
	$javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* PageRank.java -d build -Xlint 
	$jar -cvf pagerank.jar -C build/ . 
	$hadoop jar pagerank.jar org.myorg.PageRank <iput path> <output path>
		For eg: $hadoop jar pagerank.jar org.myorg.PageRank /user/cloudera/wordcount/input /user/cloudera/wordcount/output
	$hadoop fs -cat /user/cloudera/wordcount/output/*	or
	$hadoop fs -get /user/cloudera/wordcount/output/* .


Extra credit ideas
1.	Page rank will converge, with an assumption that page will converge if 3 starting decimal points of all pageranks of <title> are same.
	Tested considering all decimal points
		For 7 nodes -> 400+ iterations (killed after that)
		For 2346 nodes -> 250+ iterations (killed after that)
	Tested considering only starting 3 decimal points of pagerank as same
		For 7 nodes -> 40 iterations
		For 2346 nodes -> 9 iterations



