#!/bin/sh

mkdir files

base="s3://15619etlpurrito/outputq2/part-000"

for i in `seq 0 38`; do
	printf -v cursorDay '%02d\n' $i
	aws s3 cp $base$cursorDay ./files
	echo "downloaded "$base$cursorDay
done

