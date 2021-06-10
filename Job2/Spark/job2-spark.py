from pyspark import StorageLevel
import csv, time, argparse
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
	if x[1] > y[1]:
		return y
	else:
		return x

def get_max(x, y):
	if x[1] < y[1]:
		return y
	else:
		return x

start = time.time()
spark = SparkSession.builder.appName('Job2-spark').getOrCreate()
sc = spark.sparkContext

hsp = sc.textFile(input_path).cache() \
	.map(lambda linea: parse_linea(linea)) \
	.filter(lambda linea: linea[0] != "ticker") \
	.filter(lambda linea: get_anno(linea[7]) >= 2009 and get_anno(linea[7]) <= 2018)

hs = sc.textFile("/user/omar/input/historical_stocks.csv") \
	.map(lambda linea: parse_linea(linea)) \
	.filter(lambda linea: linea[0] != "ticker") \
	.filter(lambda linea: linea[3] != "N/A")

join_object = hsp \
	.map(lambda linea: (linea[0], (linea[2], linea[6], linea[7]))) \
	.join(hs.map(lambda linea: (linea[0], (linea[3]))))

# restituisce un rdd con ticker, close, volume, data, settore
join_object = join_object \
	.map(lambda linea: (linea[0], float(linea[1][0][0]), float(linea[1][0][1]), linea[1][0][2], linea[1][1]))

# hold the RDD in memory
join_object.persist(StorageLevel.MEMORY_AND_DISK)

# restituisce ((settore, anno, ticker), volume massimo ticker)
volume = join_object \
	.map(lambda linea: ((linea[4], get_anno(linea[3]), linea[0]), linea[2])) \
	.reduceByKey(lambda x, y: (x + y)) \
	.map(lambda linea: ((linea[0][0], linea[0][1]), (linea[0][2], linea[1])))\
	.reduceByKey(lambda x, y: get_max(x, y))

data_close = join_object.map(lambda linea: ((linea[0], linea[4], get_anno(linea[3])), (linea[1], linea[3])))

data_close_min_ticker = data_close.reduceByKey(lambda x, y: get_min(x, y))
data_close_min_tot = data_close_min_ticker \
				 .map(lambda linea: ((linea[0][1], linea[0][2]), linea[1][0])) \
				 .reduceByKey(lambda x, y: x+y)

data_close_max_ticker = data_close.reduceByKey(lambda x, y: get_max(x, y))
data_close_max_tot = data_close_max_ticker \
				 .map(lambda linea: ((linea[0][1], linea[0][2]), linea[1][0])) \
				 .reduceByKey(lambda x, y: x+y)

join_variazione_percentuale_ticker = data_close_min_ticker.join(data_close_max_ticker)
join_variazione_percentuale_tot = data_close_min_tot.join(data_close_max_tot)

variazione_percentuale_ticker = join_variazione_percentuale_ticker\
				.map(lambda linea: ((linea[0][1], linea[0][2]), (linea[0][0], round((linea[1][1][0]-linea[1][0][0])/linea[1][0][0] * 100, 2))))
variazione_percentuale_tot = join_variazione_percentuale_tot\
				.map(lambda linea: (linea[0], round((linea[1][1]-linea[1][0])/linea[1][0] * 100, 2)))

variazione_percentuale_ticker_max = variazione_percentuale_ticker.reduceByKey(lambda x, y: get_max(x, y))

output = variazione_percentuale_tot \
		 .join(volume) \
		 .join(variazione_percentuale_ticker_max) \
		.map(lambda linea: [linea[0][0], linea[0][1], linea[1][0][0], linea[1][1], linea[1][0][1]]) \
		.sortBy(lambda linea: linea[0])

output.saveAsTextFile(output_path)
end = time.time()
print("execution time is: "+ str((end-start)/60) + " min")