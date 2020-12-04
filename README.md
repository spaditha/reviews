# Swathi Padithala AmazonReviewsWithLength

# Executing AmazonReviewsWithLength.java
**Are negative reviews (1 or 2 star) more common than positive reviews (4 or 5 star)?  Stratify your totals by both verified / unverified and review length (10 evenly distributed buckets)*
1. Log into dsba-hadoop.uncc.edu using ssh
2. `https://github.com/spaditha/reviews.git` to clone this repo
3. Go into the repo directory.  In this case: `cd reviews`
4. Make a "build" directory (if it does not already exist): `mkdir build`
5. Compile the java code (all one line).  You may see some warnings--that' ok. 
`javac -cp /opt/cloudera/parcels/CDH/lib/hadoop/client/*:/opt/cloudera/parcels/CDH/lib/hbase/* AmazonReviewesWithLength.java -d build -Xlint`
6. Now we wrap up our code into a Java "jar" file: `jar -cvf process_Reviewewithlength.jar -C build/ .`
7. This is the final step  
 - Note that you will need to delete the output folder if it already exists: `hadoop fs -rm -r /user/spaditha/AmazonReviewsWithBuckets` otherwise you will get an "Exception in thread "main" org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory hdfs://dsba-nameservice/user/... type of error.
 - Now we execute the map-reduce job: ` HADOOP_CLASSPATH=$(hbase mapredcp):/etc/hbase/conf:process_Reviewewithlength.jar hadoop jar process_Reviewewithlength.jar AmazonReviewsWithLength '/user/spaditha/AmazonReviewsWithBuckets'`
 - Once that job completes, you can concatenate the output across all output files with: `hadoop fs -cat /user/spaditha/AmazonReviewsWithBuckets/*
 8. store the output to text  `hadoop fs -cat /user/spaditha/AmazonReviewsWithBuckets/* > AmazonReviewsWithBuckets.txt`
 
 The output is: in AmazonReviewsWithBuckets.txt
