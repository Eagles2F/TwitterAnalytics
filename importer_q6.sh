#!/bin/sh

import=tweets_q6_
table=tweets_q6_

for i in `seq 1 20`
do
  subtable=$table$i
  subimport=$import$i
  echo "Import Table $subtable"
  echo $subimport
  mysql -e "alter table $subtable disable keys;" -uroot purrito
  mysql -e "LOAD DATA CONCURRENT LOCAL INFILE '"$subimport"' INTO TABLE $subtable CHARACTER SET utf8mb4;" -uroot purrito
  echo "Done: '"$subimport"' at $(date)" >> log.txt
  echo "Done Import $subtable" >> log.txt
  mysql -e "alter table $subtable enable keys;" -uroot purrito
done

