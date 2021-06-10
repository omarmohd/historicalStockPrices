#!/usr/bin/env python3
import sys

csvIn = sys.stdin
firstLine = True
for line in csvIn:
    if not firstLine:
        line_input = line.strip().split(",")
        print(line_input)
    else:
        firstLine = False

