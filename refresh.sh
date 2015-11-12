#!/bin/sh

table=tweets_q2_

for i in `seq 1 10`
do
  subtable=$table$i
  echo "Alter table $subtable"
  mysql -e "alter table $subtable disable keys;" -uroot purrito
  echo "Done disable keys $subtable" >> log.txt
  mysql -e "alter table $subtable enable keys;" -uroot purrito
  echo "Done enable keys $subtable" >> log.txt
done
