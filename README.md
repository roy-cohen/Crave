# Crave

Search for the food you crave, and get results of the top dishes in your area!



## Project Summary

Crave is a web based application which incorporates a dish-based rating system and allows a user to search for a specific dish that he/she craves and get results of the top ranked dishes in the user's area based on their search term.

The dish ratings were compiled by extracting sentences from Yelp reviews and tips which include mentions of dish terms and then running a sentiment analysis on those sentences. The averages of each dish per restaurant were computed and saved in a Cassandra database, which could then be queried to meet the following use cases:

  - Display the top dishes in your city (trending dishes) 
  - Search for a dish and display the top rated dishes in your city
  - Display the top rated dishes for a specific restaurant


## The project is divided into 5 parts
1. Java application ([GrubHubScraper](#grubhub)) to acquire dish terms from GrubHub ([code](https://github.com/roy-cohen/Crave/tree/master/GrubHubScraper))
2. [SparkBatch](#sparkbatch) job to run sentiment analysis on reviews and tips and store the results in Cassandra ([code](https://github.com/roy-cohen/Crave/blob/master/SparkBatch))
3. Scala application ([KafkaProducer](#kafkaproducer)) to randomly generate new dish ratings and publish them to Kafka ([code](https://github.com/roy-cohen/Crave/tree/master/KafkaProducer))
4. Spark Streaming application ([KafkaConsumer](#kafkaconsumer)) to consume new dish ratings from Kafka, determine trending dishes, and save them to Cassandra ([code](https://github.com/roy-cohen/Crave/tree/master/KafkaConsumer))
5. Scalatra web application ([SparkWeb](#sparkweb)) which allows users to perform the use cases above via a REST API ([code](https://github.com/roy-cohen/Crave/tree/master/SparkWeb))



## <a name="grubhub"></a> GrubHubScraper
##### Dish Terms
I was able to compile a list of dish terms by making requests to GrubHub with this small Java application called [GrubHubScraper](https://github.com/roy-cohen/Crave/tree/master/GrubHubScraper). The application creates a search request for each letter of the alphabet, and then parses and stores the returned json results.  I then manually did some modifications to the data and added some of my own dish terms as well. The format I came up with was category:dish. You can see the file [here](https://github.com/roy-cohen/Crave/blob/master/SparkBatch/input/dishes.txt). The reason for this format is that when a user does a search for specific term, we want to return results of dishes that are in that category but might be named differently. For example, a search for noodles will return results for chow fun, lo mein, noodle, pad thai, pan fried, ramen, udon, which are types of noodles but don't necessarily have the word noodles in it.  


## <a name="sparkbatch"></a> SparkBatch

This is a batch job which runs on Spark and loads the full dataset from S3, processes it, and stores the results in Cassandra. Here are the dataset specifics:
* 2.2M reviews (1.94GB)
* 591K tips (118.7MB)
* 77K businesses (69MB)

The application first loads up the dish terms into memory. The application then reads the reviews and tips from S3 and process each one of them one by one. It uses the Stanford CoreNLP library to break up each review/tip into sentences and words, and then if one of those words is in the dish terms, then it does a sentiment analysis on the containing sentence, and returns a score from 1 to 5. For each dish per restaurant it computes the average score that it has received. It then joins the reviews and tips with the business data and stores the results in Cassandra.

#### Setup and Configuration
The spark batch job was run on EMR cluster of 20 m4.4xlarge nodes, 16 cores and 64 GB RAM per node. I configured the spark job to use 3 executors per node, 56 total executors, 5 cores per executor, and 19GB memory per executor. 

The full setup can be found [here](https://github.com/roy-cohen/Crave/blob/master/SparkBatch/setup.txt).

#### Cassandra

I used Cassandra for its fast writes because it needs to be able to handle many simultaneous writes coming from different servers. Also, I don't need strong consistency, but I do want high availability, so Cassandra fits.  

<b>business table</b>

|name | type |
|:----|:-----|
|businessId |TEXT|
|name |TEXT|
|full_address| TEXT|
|citystate |TEXT|
|city |TEXT|
|state |TEXT|
|stars |DOUBLE|
|reviewcount| INT|
PRIMARY KEY (businessid, citystate)

Partition key is businessid

Clustering key is citystate



<b>dishes table</b>

|name | type |
|:----|:-----|
|citystate | TEXT|
|dish| TEXT|
|avgrating| DOUBLE|
|businessId| TEXT|
|numreviews| INT|
|totalscore| INT|
|promotext| TEXT|
|promoscore| INT|
PRIMARY KEY (citystate, dish, businessid)

Partition key is citystate

Clustering key is a composite of dish and businessid

Secondary index set up on businessid


citystate is set up as the partition key so that businesses in the same citystate will live on the same node which will make for more efficient queries

A secondary index is created on businessid so that we can query all dishes for a particular business. Without the index, the dish term must be specified.

This schema allows for the following queries:
1. get dishes in a city

SELECT * FROM dishes WHERE citystate = 'MunhallPA';

2. get dishes by dish

SELECT * FROM dishes WHERE citystate = 'MunhallPA' and dish = 'burger';

3. get dishes by restaurant

SELECT * FROM dishes WHERE citystate = 'MunhallPA' and businessid = 'Gei3URlGZSiW2qZdwJ1Sdw';

4. update/insert restaurant-dish with new review

update dishes set totalscore = 100 WHERE businessid = 'Gei3URlGZSiW2qZdwJ1Sdw' AND dish = 'burger' IF EXISTS;

5. get all of the businesses

SELECT businessid, citystate FROM businesses



## <a name="kafkaproducer"></a> KafkaProducer

This application simulates users which have tried dishes and then submitted new ratings for those dishes via a mobile app.

The application chooses a random business and dish from the existing businesses and dishes and then produces a random rating (1 - 5) and publishes this new rating to Kafka. I used Kafka because it is distributed and highly scalable and can handle large amounts of data.



## <a name="kafkaconsumer"></a> KafkaConsumer



## <a name="sparkweb"></a> SparkWeb

SparkWeb is a web application prototype of how the mobile app will look like.  The app currently enables the user to:
* See the top trending dishes by city
* Search for a dish and see top rated dishes in the city based on the search term
* See the top rated dishes for a restaurant

Search results scores are multiplied by a weight factor based on the number of total ratings it has received, and then returned as weighted scores. The algorithm is set up in a way so that the more reviews a dish has, the more accurate the average score is. It doesn't make sense for a dish that has received 1 rating of 5 to be recommended over a dish that has received 100 ratings with an overall average of 4.5.    

This application is a simple web application built on the Scalatra framework. It employs a RESTful API to handle the following requests:

##### getTopDishesByCity
When you select a city from the dropdown, this request will be made to get the top trending dishes in this city.


<font size="2">
example usage:

/crave/getTopDishesByCity.json?city=CharlotteNC


params: 
|name | description |
|-----|:-------------|
|city | City concatenated with the 2 letter state abbreviation, ie: CharlotteNC


output: JSON formatted string with the following elements: 

|name | description |
|:-------|:------------|
| dish     | The name of the dish |
| businessid | The business ID      |
| avgrating  | The average Crave score this dish has received based on sentiment analysis on Yelp reviews/tips and subsequent user ratings submitted through the Crave app|
| dishnumreviews | The number of reviews this dish has received based on sentiment analysis on Yelp reviews/tips and subsequent user ratings submitted through the Crave app |
| promotext | The promotional text which contains the dish term to be displayed on the app |
| address | The business address |
| name | The name of the business |
| restaurantnumreviews | The number of reviews this businesses has received on Yelp |
| stars | The number of stars this businesses has received on Yelp |


example output:
[{"dish":"baklava","businessid":"1kdy1So4IGcVlglJtsfDeA","avgrating":"5.0","dishnumreviews":1,"promotext":"The baklava is delicious!","address":"2215 Ayersley Town Blvd\nSteele Creek\nCharlotte, NC 28273","name":"Olives Mediterranean Grill","restaurantnumreviews":44,"stars":3.5}, {...}]
</font>




<img src="https://raw.githubusercontent.com/roy-cohen/Crave/master/images/trending.png?" width="280" height="500" />

***

##### searchDishes
When you manually enter a dish or select one from the drop down, this request will be made to retreive the top dishes in the selected city which match the search term.


<font size="2">
example usage:

/crave/searchDishes.json?city=CharlotteNC&dish=burger


params: 
|name | description |
|-----|:-------------|
|city | City concatenated with the 2 letter state abbreviation, ie: CharlotteNC |
|dish | The dish term to search for|


output: JSON formatted string with the following elements: 

|name | description |
|:-------|:------------|
| dish     | The name of the dish |
| businessid | The business ID      |
| avgrating  | The average Crave score this dish has received based on sentiment analysis on Yelp reviews/tips and subsequent user ratings submitted through the Crave app|
| dishnumreviews | The number of reviews this dish has received based on sentiment analysis on Yelp reviews/tips and subsequent user ratings submitted through the Crave app |
| promotext | The promotional text which contains the dish term to be displayed on the app |
| address | The business address |
| name | The name of the business |
| restaurantnumreviews | The number of reviews this businesses has received on Yelp |
| stars | The number of stars this businesses has received on Yelp |


example output:

[{"dish":"burger","businessid":"3hWuhwAKxodX8ayAb-fe2g","avgrating":"5.0","dishnumreviews":1,"promotext":"Try the blackened burger...so good!","address":"3419 Toringdon Way\nBallantyne\nCharlotte, NC 28277","name":"BV Pub and Pizzeria","restaurantnumreviews":28,"stars":4.5},{"dish":"burger","businessid":"NnC14ctqyIKsknJqNcng-A","avgrating":"5.0","dishnumreviews":1,"promotext":"Also great hummus and pitas, Italian panini was excellent and they make some delicious burgers.","address":"217 Southside Dr\nCharlotte, NC 28217","name":"OMB's Weihnachtsmarkt - Christmas Market","restaurantnumreviews":7,"stars":4.5}, {...}]



</font>
<img src="https://raw.githubusercontent.com/roy-cohen/Crave/master/images/results.png?" width="280" height="500" />
***

##### getTopDishesForRestaurant
When you click on a restaurant name and reach the restaurant page.


<font size="2">
example usage:

/crave/getTopDishesForRestaurant.json?city=CharlotteNC&id=3hWuhwAKxodX8ayAb-fe2g


params: 
|name | description |
|-----|:-------------|
|city | City concatenated with the 2 letter state abbreviation, ie: CharlotteNC |
|id | The business ID of the business|


output: JSON formatted string with the following elements: 

|name | description |
|:-------|:------------|
| dish     | The name of the dish |
| businessid | The business ID      |
| avgrating  | The average Crave score this dish has received based on sentiment analysis on Yelp reviews/tips and subsequent user ratings submitted through the Crave app|
| dishnumreviews | The number of reviews this dish has received based on sentiment analysis on Yelp reviews/tips and subsequent user ratings submitted through the Crave app |
| promotext | The promotional text which contains the dish term to be displayed on the app |
| address | The business address |
| name | The name of the business |
| restaurantnumreviews | The number of reviews this businesses has received on Yelp |
| stars | The number of stars this businesses has received on Yelp |


example output:

[{"dish":"burger","businessid":"3hWuhwAKxodX8ayAb-fe2g","avgrating":"5.0","dishnumreviews":1,"promotext":"Try the blackened burger...so good!","address":"3419 Toringdon Way\nBallantyne\nCharlotte, NC 28277","name":"BV Pub and Pizzeria","restaurantnumreviews":28,"stars":4.5},{"dish":"salad","businessid":"3hWuhwAKxodX8ayAb-fe2g","avgrating":"5.0","dishnumreviews":1,"promotext":"They have a really nice side salad, its simple but always fresh and feeling.","address":"3419 Toringdon Way\nBallantyne\nCharlotte, NC 28277","name":"BV Pub and Pizzeria","restaurantnumreviews":28,"stars":4.5},{"dish":"dip","businessid":"3hWuhwAKxodX8ayAb-fe2g","avgrating":"4.0","dishnumreviews":2,"promotext":"Great wings and bleu cheese dipping sauce.","address":"3419 Toringdon Way\nBallantyne\nCharlotte, NC 28277","name":"BV Pub and Pizzeria","restaurantnumreviews":28,"stars":4.5}]



</font>

<img src="https://raw.githubusercontent.com/roy-cohen/Crave/master/images/restaurant.png?" width="280" height="500" />




## Future Considerations

In the future, a mobile app will be developed and will make use of the same REST API to make its requests. Here are a few features I would like to add to the app, and a few screenshots of how it would look.
* The ability to retreive and display an image of the specific dish from a restaurant in the search results.
* The ability to order dishes directly from the app.
* The ability to rate a dish after you've ordered it.  


![future](https://raw.githubusercontent.com/roy-cohen/Crave/master/images/future.png)



## Architecture

![System Design](https://raw.githubusercontent.com/roy-cohen/Crave/master/images/systemdesign.png)



## Technical Difficulties
This is a list of some of the problems I ran into, and the steps I took to troubleshoot them.
##### Error: Unable to connect to Cassandra: java.lang.NoSuchMethodError
After looking into the security configurations and making sure that the necessary Cassandra ports were open and that servers can talk with each other, I realized that the problem had nothing to do with Cassandra at all. There was another exception deeper in the stack trace:

<font size="2"> java.lang.NoSuchMethodError: com.google.common.util.concurrent.Futures.withFallback(Lcom/google/common/util/concurrent/</br>ListenableFuture;Lcom/google/common/util/concurrent/FutureFallback;Ljava/util/concurrent/</br>Executor;)Lcom/google/common/util/concurrent/ListenableFuture;</font>

After doing some research, (thanks to this [article](http://ben-tech.blogspot.com/2016/05/how-to-resolve-spark-cassandra.html)) I found out that when you use datastax's Spark Cassandra connector, it depends on the latest version of Guava, 16.0.1, but the EC2 instances that were created have Guava 11.0.2 which do not include some required methods.

In order to resolve this issue, I needed to include guava-16.0.1.jar in the --files option of the spark-shell command so that file is copied to each executor's container, and also add it to the classPath so that the executor will know where to find it.

<font size="2"> 
--files guava-16.0.1.jar  spark.executor.extraClassPath=guava-16.0.1.jar
</font>



##### Error:  java.lang.RuntimeException: java.lang.ClassNotFoundException: Class org.apache.hadoop.fs.s3native.NativeS3FileSystem not found

After fixing the problem above, I suddenly could not access my files on S3 anymore. It seems as though there is a problem with Spark on EMR where setting the extraClassPath as I did above overwrote the classPath rather than appending to it. The fix for this issue was to simply include the class paths for the mising libraries.


<font size="2">
spark.executor.extraClassPath=guava-16.0.1.jar:/etc/hadoop/conf:/usr/lib/hadoop-lzo/lib/ \*:/usr/share/aws/aws-java-sdk/ \*:/usr/share/aws/emr/emrfs/conf:/usr/ share/aws/emr/emrfs/lib/ \*:/usr/share/aws/emr/emrfs/auxlib/*</font>



##### Error: Spark job never completes, tasks fails because of java.lang.OutOfMemoryError errors, and eventually the whole job fails

The original setup I had was with 6 m3.xlarge machines and the job would run for over 20 hours and most times I would usually abort it myself in order to try to make some changes and then run it again. This was obviously not very efficient. I tried with 10 machines as well, but it did not make a significant difference.

Here are some of the steps I took:
* Converted the json datasets into parquet for faster reading and processing
* Added some logic to save intermediate results on S3. If the job fails, I could load up the intermediate results from S3 rather than doing everything from scratch.
* Added another class with logic for splitting text into sentences, and sentences into words. The intention behind this was to reduce the usage of the Stanford CoreNLP library and its annotators, and only use it for sentiment analysis. According to Stanford's [documentation](http://nlp.stanford.edu/software/corenlp-faq.shtml) (number 6), annotators load large model files which use lots of memory. Also, according to this [documentation](http://nlp.stanford.edu/software/parser-faq.shtml) (number 17), memory usage expands roughly with the square of the sentence length. 
* Added a wrapper for Stanford CoreNLP using [this code](https://gist.github.com/thomaspouncy/e620d32d636f764df261) to improve efficiency. The wrapper creates a singleton coreNLP object so that only one instance exists per JVM. Multiple executors can share this instance rather than creating new ones, since they are very memory intensive.
* Added try catch logic around the sentence and word breaking, and the sentiment analysis to see if any exceptions were thrown that I could log.
* Created EMR cluster based on configuration from this [article](http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/), and used the same Spark job configuration in order to take full advantage of resources of each node in the cluster. This significantly increased the speed of the job, and I was able to see all but 2 tasks complete successfully.
* Logged every sentence that was being sent to Stanford CoreNLP's sentiment analysis. I was then able to look at the logs on the servers where the 2 tasks were failing. This is when I figured out the issue. A few Yelp reviewers left some really long reviews without puncutation, which CoreNLP depends on to break text into sentences, as well as the class I created. This was causing the out of memory errors, and the tasks to fail.

I had a few thoughts on how to fix this issue. One was basically to ignore the sentence, either through my own logic or through CoreNLP's -maxLength option.  Another idea was use a separate thread to run the sentiment analysis and use a timeout to kill it if it takes too long. Unfortunately I could not get an example of this idea working as even when I tried to kill the thread, it would still throw an OutOfMemoryError and crash the Spark shell. I was also not able to catch the OutOfMemoryError. At this point, I felt like I was wasting too much time on an issue that wasn't too important. After all, it was only a few reviews out of over 2 million. But being the perfectionist that I am, I still wanted to do the sentiment analysis on any dish terms in those long reviews, so I came up with the following solution. For really long sentences, which included a dish term, I just took 150 words before the term, and 150 words after it, and used that as my sentence. (300 words might be overkill!) That did the trick. I was finally able to get through this part of the job. In fact, once I figured out the next issue, the complete job ran in about 1 hour and 20 minutes.   


##### Error: type mismatch;
found    : RDD[(String, ReviewInfo)]
required : RDD[(String, ReviewInfo)]

Huh? and Whaaaattttt! That was my reaction after finally getting all 2 and half million reviews and tips processed, now I get this?  After digging around a bit, I found out on Databricks' site that is a known issue that they plan to fix. The problem had something to do with a class and function that had the class as a parameter needed to be defined in the same cell to work. No problem, I quickly defined the function in the main cell and got it to work. Since then, i've restructured my program a bit, so this is no longer relevant.



##### Error: After backing up and restoring Cassandra on my local machine, running certain queries on Cassandra returns 0 results. 
It was strange because other queries would return results that matched what the query that returned 0 results should have returned.  Turned out the queries which were returning 0 results were using the secondary index that I had originally created on businessid.  I figured out the command to rebuild the index from the datastax documentation, and that fixed it.



## References
[Stanford CoreNLP](http://stanfordnlp.github.io/CoreNLP/)

[Natural Language Processing with Spark Code](https://gist.github.com/thomaspouncy/e620d32d636f764df261)

[How-to: Tune Your Apache Spark Jobs (Part 2)](http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/)

