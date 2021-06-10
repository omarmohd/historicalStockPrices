# script usato solo a fini di test

import csv

with open('./join_object', 'r') as in_file:
    stripped = (line.strip() for line in in_file)
    lines = (line.replace('(', '').replace(')', '').replace('\'', '').replace(' ', '').split(",") for line in stripped if line)
    with open('./join_object.csv', 'w') as out_file:
        writer = csv.writer(out_file)
        writer.writerows(lines)