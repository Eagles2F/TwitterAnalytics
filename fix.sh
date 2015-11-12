#!/bin/sh

table=tweets_q2_

for i in `seq 1 10`
do
  subtable=$table$i
  echo "repair table $subtable"
  mysql -e "REPAIR table $subtable QUICK;" -uroot purrito
  echo "done repair table $subtable"
  echo "done repair table $subtable" >> log.txt
done
