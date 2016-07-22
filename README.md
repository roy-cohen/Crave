# Crave

Crave is a dish-based ranking web application which lets a user search for a specific dish that he/she craves and get results of the top ranked dishes in the user's area based on their search term.

Rankings of dishes is done using a Spark job which runs a sentiment analysis (using Stanford CoreNLP library) on Yelp reviews/tips dataset and stores the results in a Cassandra database. Simulation of new dish ratings are done with a scala app to produce new ratings and push them to Kafka. Then a Spark streaming app is used to consume the new ratings from Kafka and store them in Cassandra. A web application built with scala on the scalatra framework enables the user to see top trending dishes in the area, search for specific dishes and see top results, or view a restaurant and see its top dishes. The web app talks directly to Cassandra to get these results.
