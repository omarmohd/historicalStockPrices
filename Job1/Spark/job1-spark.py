import time, argparse, csv
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument('--output_path', type=str)
parser.add_argument('--input_path', type=str)
args = parser.parse_args()
input_path, output_path = args.input_path, args.output_path

def get_anno(dateAsString):
	return int(dateAsString[:4])

def parse_linea(linea):
	csv_reader = csv.reader([linea], delimiter=',')
	return next(csv_reader)

def get_min(x, y):
	if x[0] > y[0]:
		return y
	else:
		return x

def get_max(x, y):
	if x[0] < y[0]:
		return y
	else:
		return x

start = time.time()
spark = SparkSession.builder.appName('Job1-spark').getOrCreate()
sc = spark.sparkContext

hsp = sc.textFile(input_path).cache() \
	.map(lambda linea: parse_linea(linea)) \
	.filter(lambda linea: linea[0] != "ticker")

min_date_close = hsp \
	.map(lambda linea: (linea[0], (linea[7], linea[2]))) \
	.reduceByKey(lambda x, y: get_min(x, y))

max_date_close = hsp \
	.map(lambda linea: (linea[0], (linea[7], linea[2]))) \
	.reduceByKey(lambda x, y: get_max(x, y))

min_max_date_close = min_date_close.join(max_date_close)

# restituisce (ticker, variazione percentuale)
variazione_percentuale = min_max_date_close \
	.map(lambda linea: (linea[0], round(((float(linea[1][1][1]) - float(linea[1][0][1])) / float(linea[1][0][1])) * 100, 2)))

# restituisce (ticker, massimo tra gli high)
max_high = hsp \
	.map(lambda linea: (linea[0], (linea[5], 0)))\
	.reduceByKey(lambda x, y: get_max(x, y)) \
	.map(lambda linea: (linea[0], linea[1][0]))

# restituisce (ticker, minimo tra i low)
min_low = hsp \
	.map(lambda linea: (linea[0], (linea[4], 0)))\
	.reduceByKey(lambda x, y: get_min(x, y)) \
	.map(lambda linea: (linea[0], linea[1][0]))

output = min_max_date_close.map(lambda linea: (linea[0], (linea[1][0][0], linea[1][1][0]))) \
	.join(variazione_percentuale) \
	.join(max_high) \
	.join(min_low) \
	.map(lambda linea: [linea[0], linea[1][0][0][0][0], linea[1][0][0][0][1], linea[1][0][0][1],
						linea[1][0][1], linea[1][1]]) \
	.sortBy(lambda linea: linea[2], False)

output.saveAsTextFile(output_path)
end = time.time()
print("execution time is: "+ str((end-start)/60) + " min")