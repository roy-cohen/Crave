PKGS=edu.stanford.nlp:stanford-corenlp:3.6.0
PKGS=$PKGS,edu.stanford.nlp:stanford-parser:3.6.0 
PKGS=$PKGS,com.google.protobuf:protobuf-java:2.6.1
PKGS=$PKGS,com.datastax.spark:spark-cassandra-connector_2.10:1.6.0
//PKGS=$PKGS,com.datastax.spark:spark-cassandra-connector_2.10:1.5.0
//PKGS=$PKGS,com.databricks.spark.corenlp:

spark-shell   --packages $PKGS   --jars ./code/stanford-english-corenlp-2016-01-10-models.jar  --conf "spark.cassandra.connection.host=127.0.0.1"
spark-shell   --packages $PKGS   --jars stanford-english-corenlp-2016-01-10-models.jar --conf "spark.cassandra.connection.host=127.0.0.1"
spark-shell   --master local[*] --packages $PKGS   --jars stanford-english-corenlp-2016-01-10-models.jar --conf "spark.cassandra.connection.host=172.31.62.42" 
spark-shell   --master spark://172.31.59.181:7077 --packages $PKGS   --jars stanford-english-corenlp-2016-01-10-models.jar --conf "spark.cassandra.connection.host=172.31.62.42" 
spark-shell   --packages $PKGS   --jars stanford-english-corenlp-2016-01-10-models.jar --conf "spark.cassandra.connection.host=172.31.59.181,spark.executor.extraClassPath=./guava-16.0.1.jar"  --files ./guava-16.0.1.jar --driver-class-path ./guava-16.0.1.jar
spark-shell   --packages $PKGS   --jars stanford-english-corenlp-2016-01-10-models.jar --conf "spark.cassandra.connection.host=172.31.59.181"  --files guava-16.0.1.jar --driver-class-path guava-16.0.1.jar --conf spark.executor.extraClassPath=guava-16.0.1.jar
spark-shell   --packages $PKGS   --jars stanford-english-corenlp-2016-01-10-models.jar --conf "spark.cassandra.connection.host=172.31.59.181"  --files guava-16.0.1.jar --conf spark.executor.extraClassPath=guava-16.0.1.jar
//spark-shell --packages edu.stanford.nlp:stanford-corenlp:3.6.0,com.google.protobuf:protobuf-java:2.6.1 --jars stanford-english-corenlp-2016-01-10-models.jar 
