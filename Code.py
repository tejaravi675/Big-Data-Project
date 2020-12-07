from pyspark import SparkContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")

#Loading data
df = spark.read.format("csv").option("header","true").load("/user/s2289741/*.csv")
total_records = df.count()

#year wise split
df2004 = df[(df.Year == 2004)]
records_2004 = df2004.count()
df2005 = df[(df.Year == 2005)]
records_2005 = df2005.count()
df2006 = df[(df.Year == 2006)]
records_2006 = df2006.count()
df2007 = df[(df.Year == 2007)]
records_2007 = df2007.count()
df2008 = df[(df.Year == 2008)]
records_2008 = df2008.count()

#cancelled flights : year wise
df2004a = df2004[(df2004.Cancelled == 1)]
cancel_2004 = df2004a.count()
df2005a = df2005[(df2005.Cancelled == 1)]
cancel_2005 = df2005a.count()
df2006a = df2006[(df2006.Cancelled == 1)]
cancel_2006 = df2006a.count()
df2007a = df2007[(df2007.Cancelled == 1)]
cancel_2007 = df2007a.count()
df2008a = df2008[(df2008.Cancelled == 1)]
cancel_2008 = df2008a.count()

#Cancelled due to weather : year wise
df2004b = df2004a[(df2004a.CancellationCode == "B")]
weather_2004 = df2004b.count()
df2005b = df2005a[(df2005a.CancellationCode == "B")]
weather_2005 = df2005b.count()
df2006b = df2006a[(df2006a.CancellationCode == "B")]
weather_2006 = df2006b.count()
df2007b = df2007a[(df2007a.CancellationCode == "B")]
weather_2007 = df2007b.count()
df2008b = df2008a[(df2008a.CancellationCode == "B")]
weather_2008 = df2008b.count()

#Cancelled due to weather : year wise %
weather_perc_2004 = (weather_2004/cancel_2004)*100 #Doubt
weather_perc_2005 = (weather_2005/cancel_2005)*100
weather_perc_2006 = (weather_2006/cancel_2006)*100
weather_perc_2007 = (weather_2007/cancel_2007)*100
weather_perc_2008 = (weather_2008/cancel_2008)*100

#Cancelled due to weather : months of each year
df2004c = df2004b.select(df2004b.Month)
r2004 = df2004c.rdd
r2004a = r2004 \
       .map(lambda x:(x,1)) \
       .reduceByKey(lambda a,b:a+b)
month_2004 = r2004a.top(12, key=lambda t: t[1])

df2005c = df2005b.select(df2005b.Month)
r2005 = df2005c.rdd
r2005a = r2005 \
       .map(lambda x:(x,1)) \
       .reduceByKey(lambda a,b:a+b)
month_2005 = r2005a.top(12, key=lambda t: t[1])

df2006c = df2006b.select(df2006b.Month)
r2006 = df2006c.rdd
r2006a = r2006 \
       .map(lambda x:(x,1)) \
       .reduceByKey(lambda a,b:a+b)
month_2006 = r2006a.top(12, key=lambda t: t[1])

df2007c = df2007b.select(df2007b.Month)
r2007 = df2007c.rdd
r2007a = r2007 \
       .map(lambda x:(x,1)) \
       .reduceByKey(lambda a,b:a+b)
month_2007 = r2007a.top(12, key=lambda t: t[1])

df2008c = df2008b.select(df2008b.Month)
r2008 = df2008c.rdd
r2008a = r2008 \
       .map(lambda x:(x,1)) \
       .reduceByKey(lambda a,b:a+b)
month_2008 = r2008a.top(4, key=lambda t: t[1])

#Cancelled due to weather : overall
dfwc = df[(df.Cancelled == 1) & (df.CancellationCode == "B")]
flights_cancelled_weather = dfwc.count()

#Cancelled due to weather : months overall
dfmonth = dfwc.select(df.Month)
rall = dfmonth.rdd
ralla = rall \
      .map(lambda x:(x,1)) \
      .reduceByKey(lambda a,b:a+b)
month_all = rallc.top(12, key=lambda t: t[1])
for (w,c) in month_all:
  print w,c #doubt