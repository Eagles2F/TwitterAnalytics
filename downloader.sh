#!/bin/sh

base="s3://purritoetl/output/part-000"
for i in `seq 7 50`; do
	printf -v cursorDay '%02d\n' $i
	aws s3 cp $base$cursorDay .
	echo "downloaded "$base$cursorDay
done
