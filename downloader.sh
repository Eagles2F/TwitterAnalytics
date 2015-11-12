#!/bin/sh

mkdir file4

base="s3://15619etlpurrito/outhbaseq4/part-r-000"

for i in `seq 0 2`; do
	printf -v cursorDay '%02d\n' $i
	aws s3 cp $base$cursorDay ./file4
	echo "downloaded "$base$cursorDay
done

