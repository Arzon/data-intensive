#!/usr/bin/env python3

import sys

i = 0
for line in sys.stdin: 
    if i == 0:
        print("productId,mean_startRating,num_rate")
        i = i+1
    
    line = ((line.replace("\\t",",")).replace("[","")).replace("]","")
    print(line)