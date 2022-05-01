#!/usr/bin/env python3

import sys

for line in sys.stdin: 
    line = line.strip()
    line = line.split("\t")
    value = ((line[1].lstrip("[")).rstrip("]")).replace(" ","")
    print(value)