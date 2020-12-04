# Sai Venkatesh Mamillapalli MapReduce

# Executing AmazonTopReviewers.java
**Case - Identify the top 100 Verified Reviewers and their overall average rating**
1. Log into dsba-hadoop.uncc.edu using ssh
2. `https://github.com/saimamilla/MapReduce.git` to clone this repo
3. Go into the repo directory.  In this case: `cd MapReduce`
4. Make a "build" directory (if it does not already exist): `mkdir build`
5. Compile the java code (all one line).  You may see some warnings--that' ok. 
`javac -cp /opt/cloudera/parcels/CDH/lib/hadoop/client/*:/opt/cloudera/parcels/CDH/lib/hbase/* AmazonTopReviewers.java -d build -Xlint`
6. Now we wrap up our code into a Java "jar" file: `jar -cvf process_topReviewers.jar -C build/ .`
7. This is the final step  
 - Note that you will need to delete the output folder if it already exists: `hadoop fs -rm -r /user/smamilla/top_reviewers` otherwise you will get an "Exception in thread "main" org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory hdfs://dsba-nameservice/user/... type of error.
 - Now we execute the map-reduce job: ` HADOOP_CLASSPATH=$(hbase mapredcp):/etc/hbase/conf:process_topReviewers.jar hadoop jar process_topReviewers.jar AmazonTopReviewers '/user/smamilla/top_reviewerss'`
 - Once that job completes, you can concatenate the output across all output files with: `hadoop fs -cat /user/smamilla/top_reviewerss/*
 ` or if you have output that is too big for displaying on the terminal screen you can do `hadoop fs -cat /user/smamilla/top_reviewerss/* | sort -n -k3 -r > overall.txt` 
 8. To get top 100 - ` hadoop fs -cat /user/smamilla/top_reviewerss/* | sort -n -k3 -r | head -n100`
 9. store the output to text  `hadoop fs -cat /user/smamilla/top_reviewerss/* | sort -n -k3 -r | head -n100 >Top100.txt`
 
 The output is: in Top100.txt
