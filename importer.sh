#!/bin/sh

for f in /home/ec2-user/part*
do
	mysql -e "LOAD DATA CONCURRENT LOCAL INFILE '"$f"' INTO TABLE tweets CHARACTER SET utf8mb4;" -uroot purrito
	rm "$f"
	echo "Done: '"$f"' at $(date)" >> log.txt
done
mysql -e "alter table tweets enable keys;" -uroot purrito
echo "Done" >> log.txt

