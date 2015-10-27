#!/usr/bin/env python
import sys

pre_id = ''
for line in sys.stdin:
	line = line.strip('\n')
	pieces = line.split('\t')
	id_now = pieces[0]
	if not pre_id:
		pre_id = id_now
		print line
	else:
		if id_now == pre_id:
			continue
		else:
			pre_id = id_now
			print line
	# deli_pos = line.find('\t')
	# print line[:deli_pos]+','+line[deli_pos+1:]