#!/bin/sh

mkdir file5

base="s3://15619etlpurrito/phase5/countmillion/part-r-000"

for i in `seq 0 34`; do
	printf -v cursorDay '%02d\n' $i
	aws s3 cp $base$cursorDay ./file5
	echo "downloaded "$base$cursorDay
done

