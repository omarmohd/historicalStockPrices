import time, argparse
from itertools import islice
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument('--output_path', type=str)
parser.add_argument('--input_path', type=str)
args = parser.parse_args()
input_path, output_path = args.input_path, args.output_path

def divideLine(line, index):
    return line.split(",")[index]

def getValue(line, index):
    return float(line.split(",")[index])

start = time.time()
spark = SparkSession.builder.appName('Job3-spark').getOrCreate()
sc = spark.sparkContext

# caricamento file, escludendo prima riga
# riga 28: filtro per anno 2017 e date iniziali/finali
# roga 30: map (azione, mese) -> (giorno, valore)
# riga 31: raggruppo per azione/mese
# riga 32: map (azione) -> (mese, variazione)
# riga 33: raggruppo per azione
# riga 34: filtro per azioni che hanno 12 variazioni(una per ogni mese)
# riga 35: map (azione)-> ([meseA, variazioneA], .., [...]) ordinato
# riga 36: map (1) -> (azione, [mese, variazione], .., []) così posso fare join su key 1
# riga 38: join e filter fra azioni diverse che abbiano variazione non maggiore di 0,5.

hsp = sc.textFile(input_path).cache() \
    .mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)

data = hsp.map(lambda line: ((divideLine(line, 0), divideLine(line, 7).split('-')[0]),
                                   (divideLine(line, 7).split('-')[1], divideLine(line, 7).split('-')[2],
                                    getValue(line, 2)))) \
    .filter(lambda x: x[0][1] == '2017' and (x[1][1] == '01' or x[1][1] == '28' or x[1][1] == '29' \
                                             or x[1][1] == '30' or x[1][1] == '31')) \
    .map(lambda x: ((x[0][0], x[1][0]), (x[1]))) \
    .groupByKey().mapValues(list) \
    .map(lambda x: ((x[0][0]), (x[0][1], (((max(x[1])[2]-min(x[1])[2])/min(x[1])[2])*100)))) \
    .groupByKey().mapValues(list) \
    .filter(lambda x: len(x[1]) == 12) \
    .map(lambda x: ((x[0]), (sorted(x[1], key=lambda x: x[0])))) \
    .map(lambda x: ((1), (x)))

output = data.join(data) \
    .filter(lambda line: line[1][0][0] != line[1][1][0] and
                         (-0.6 < line[1][0][1][0][1] - line[1][1][1][0][1] < 0.6) and
                         (-0.6 < line[1][0][1][1][1] - line[1][1][1][1][1] < 0.6) and
                         (-0.6 < line[1][0][1][2][1] - line[1][1][1][2][1] < 0.6) and
                         (-0.6 < line[1][0][1][3][1] - line[1][1][1][3][1] < 0.6) and
                         (-0.6 < line[1][0][1][4][1] - line[1][1][1][4][1] < 0.6) and
                         (-0.6 < line[1][0][1][5][1] - line[1][1][1][5][1] < 0.6) and
                         (-0.6 < line[1][0][1][6][1] - line[1][1][1][6][1] < 0.6) and
                         (-0.6 < line[1][0][1][7][1] - line[1][1][1][7][1] < 0.6) and
                         (-0.6 < line[1][0][1][8][1] - line[1][1][1][8][1] < 0.6) and
                         (-0.6 < line[1][0][1][9][1] - line[1][1][1][9][1] < 0.6 ) and
                         (-0.6 < line[1][0][1][10][1] - line[1][1][1][10][1] < 0.6) and
                         (-0.6 < line[1][0][1][11][1] - line[1][1][1][11][1] < 0.6)
            )

output.saveAsTextFile(output_path)
end = time.time()
print("execution time is: "+ str((end-start)/60) + " min")