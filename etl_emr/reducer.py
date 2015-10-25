#!/usr/bin/env python
import sys

for line in sys.stdin:
	line = line.strip('\n')
	deli_pos = line.find('\t')
	print line[:deli_pos]+','+line[deli_pos+1:]