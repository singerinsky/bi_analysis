cat log.txt | kafka-console-producer.sh --broker-list 127.0.0.1:9092 --sync --topic bidata | > out.txt
