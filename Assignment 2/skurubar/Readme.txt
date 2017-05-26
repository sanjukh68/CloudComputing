Name: Sanju Kurubara Budi Hall Hiriyanna Gowda
UNCC Id: 800953525

At any point of time please make sure to delete tempOutput and output folders to execute the code again smoothly. Same instructions are posted in below steps also
Step 1: Initial Setup
	$ sudo su hdfs
	$ hadoop fs -mkdir /user/cloudera
	$ hadoop fs -chown cloudera /user/cloudera
	$ exit
	$ sudo su cloudera
	$ hadoop fs -mkdir /user/cloudera/wordcount /user/cloudera/wordcount/input

Step 2. Copy all the input files into the new input directory
	hadoop fs -put canterbury/* /user/cloudera/wordcount/input/

Step 3: To execute DocWordCount.java
	$hadoop fs -rm /user/cloudera/wordcount/output/*
	$javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocWordCount.java -d build -Xlint 
	$jar -cvf docwordcount.jar -C build/ . 
	$hadoop jar docwordcount.jar org.myorg.DocWordCount /user/cloudera/wordcount/input /user/cloudera/wordcount/output
	$hadoop fs -cat /user/cloudera/wordcount/output/*

Step 4: To execute TermFrequency.java
	$hadoop fs -rm /user/cloudera/wordcount/output/*
	$javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TermFrequency.java -d build -Xlint 
	$jar -cvf termfrequency.jar -C build/ . 
	$hadoop jar termfrequency.jar org.myorg.TermFrequency /user/cloudera/wordcount/input /user/cloudera/wordcount/output
	$hadoop fs -cat /user/cloudera/wordcount/output/*

Step 5: To execute TFIDF.java
	//tempOutput stores intermediate result of 1st MapReduce
	$hadoop fs -rm /user/cloudera/wordcount/output/*
	$hadoop fs -rm /user/cloudera/wordcount/tempOutput/*

	$javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* TFIDF.java -d build -Xlint 
	$jar -cvf TFIDF.jar -C build/ . 
	$hadoop jar TFIDF.jar org.myorg.TFIDF /user/cloudera/wordcount/input /user/cloudera/wordcount/tempOutput /user/cloudera/wordcount/output
	$hadoop fs -cat /user/cloudera/wordcount/output/*

Step 6: To execute Search.java
	//tempOutput1 and tempOutput2 stores intermediate result of 1st MapReduce and 2nd MapReduce
	//"Search Query" can be any string which need to be searched in the documents
	//Please make sure there are total 5 command line arguments
	$hadoop fs -rm /user/cloudera/wordcount/output/*
	$hadoop fs -rm /user/cloudera/wordcount/tempOutput1/*
	$hadoop fs -rm /user/cloudera/wordcount/tempOutput2/*

	$javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Search.java -d build -Xlint 
	$jar -cvf Search.jar -C build/ . 
	$hadoop jar Search.jar org.myorg.TFIDF /user/cloudera/wordcount/input /user/cloudera/wordcount/tempOutput1 /user/cloudera/wordcount/tempOutput2 /user/cloudera/wordcount/output "Search Query"
	$hadoop fs -cat /user/cloudera/wordcount/output/*

Step 6: To execute Rank.java
	//tempOutput1 tempOutput2 and tempOutput3 stores intermediate result of 1st MapReduce, 2nd MapReduce and 3rd MapReduce respectively
	//"Search Query" can be any string which need to be searched in the documents
	//Please make sure there are total 6 command line arguments
	$hadoop fs -rm /user/cloudera/wordcount/output/*
	$hadoop fs -rm /user/cloudera/wordcount/tempOutput1/*
	$hadoop fs -rm /user/cloudera/wordcount/tempOutput2/*
	$hadoop fs -rm /user/cloudera/wordcount/tempOutput3/*

	$javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* Rank.java -d build -Xlint 
	$jar -cvf Rank.jar -C build/ . 
	$hadoop jar Rank.jar org.myorg.Rank /user/cloudera/wordcount/input /user/cloudera/wordcount/tempOutput1 /user/cloudera/wordcount/tempOutput2 /user/cloudera/wordcount/tempOutput3 /user/cloudera/wordcount/output "Search Query"
	$hadoop fs -cat /user/cloudera/wordcount/output/*

