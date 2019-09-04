rm p.zip
rm out -rf
zip -r p.zip spark-kafka.py
#spark-submit --driver-class-path kafka-clients-2.3.0.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.3 --master local[1] --py-files p.zip spark-kafka.py 
/opt/software/spark-2.4.4-bin-hadoop2.7/bin/spark-submit --driver-class-path kafka-clients-2.3.0.jar --jars mysql-connector-java-5.1.47.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 --master local[1] --py-files p.zip spark-kafka.py 
