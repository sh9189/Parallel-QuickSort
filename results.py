#!/usr/bin/python
import re
h = open("result.txt")
reg = re.compile('Speedup is (\d+.\d+)')
list = []
for line in h:
       go = reg.search(line)
       if go is not None:
               list.append(go.group(1))
list.sort()
print list

