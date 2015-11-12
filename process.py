#!/bin/python

of = open("orig")
nf = open("new",'w')
for line in of:
    if len(line) > 8192:
        nf.write(line)

of.close()
nf.close()
