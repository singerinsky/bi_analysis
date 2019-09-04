rm p.zip
rm out -rf
zip -r p.zip spark-kafka.py
spark-submit --driver-class-path kafka-clients-2.3.0.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.3 --master local[1] --py-files p.zip spark-kafka.py 
