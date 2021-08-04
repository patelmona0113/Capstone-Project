from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType, StringType, StructType, StructField
from pyspark.sql.functions import *
from pyspark.sql.functions import get_json_object, col, from_json, struct, array, udf, struct, format_number
from predict_model import doublevalue, modelprediction
import numpy as np

def predict(a):
    X = np.array([a])
    b = modelprediction(X)
    return b



spark = SparkSession.builder.appName("TestApp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

predict_udf = udf(lambda x: predict(x), StringType())

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-in") \
    .option("startingOffsets", "earliest") \
    .load() \
    .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", "")) \
    .withColumn("value", regexp_replace(col("value"), "^\"|\"$", ""))

df1 = df.selectExpr("CAST(value AS STRING)")

df11 = df1.select(get_json_object(col("value"),"$['tBodyAcc.mean.X.1']").cast("float").alias("tBodyAccmeanX1"),
get_json_object(col("value"),"$['tBodyAcc.mean.Y.2']").cast("float").alias("tBodyAccmeanY2"),
get_json_object(col("value"),"$['tBodyAcc.energy.Y.18']").cast("float").alias("tBodyAccenergyY18"),
get_json_object(col("value"),"$['tBodyAcc.iqr.Z.22']").cast("float").alias("tBodyAcciqrZ22"),
get_json_object(col("value"),"$['tBodyAcc.entropy.X.23']").cast("float").alias("tBodyAccentropyX23"),
get_json_object(col("value"),"$['tBodyAcc.entropy.Y.24']").cast("float").alias("tBodyAccentropyY24"),
get_json_object(col("value"),"$['tBodyAcc.arCoeff.X.2.27']").cast("float").alias("tBodyAccarCoeffX227"),
get_json_object(col("value"),"$['tBodyAcc.arCoeff.X.3.28']").cast("float").alias("tBodyAccarCoeffX328"),
get_json_object(col("value"),"$['tBodyAcc.arCoeff.Y.2.31']").cast("float").alias("tBodyAccarCoeffY231"),
get_json_object(col("value"),"$['tBodyAcc.arCoeff.Y.3.32']").cast("float").alias("tBodyAccarCoeffY332"),
get_json_object(col("value"),"$['tBodyAcc.arCoeff.Z.2.35']").cast("float").alias("tBodyAccarCoeffZ235"),
get_json_object(col("value"),"$['tBodyAcc.arCoeff.Z.3.36']").cast("float").alias("tBodyAccarCoeffZ336"),
get_json_object(col("value"),"$['tBodyAcc.arCoeff.Z.4.37']").cast("float").alias("tBodyAccarCoeffZ437"),
get_json_object(col("value"),"$['tBodyAcc.correlation.X.Y.38']").cast("float").alias("tBodyAcccorrelationXY38"),
get_json_object(col("value"),"$['tBodyAcc.correlation.X.Z.39']").cast("float").alias("tBodyAcccorrelationXZ39"),
get_json_object(col("value"),"$['tGravityAcc.max.Y.51']").cast("float").alias("tGravityAccmaxY51"),
get_json_object(col("value"),"$['tGravityAcc.max.Z.52']").cast("float").alias("tGravityAccmaxZ52"),
get_json_object(col("value"),"$['tGravityAcc.min.X.53']").cast("float").alias("tGravityAccminX53"),
get_json_object(col("value"),"$['tGravityAcc.min.Z.55']").cast("float").alias("tGravityAccminZ55"),
get_json_object(col("value"),"$['tGravityAcc.energy.X.57']").cast("float").alias("tGravityAccenergyX57"),
get_json_object(col("value"),"$['tGravityAcc.energy.Y.58']").cast("float").alias("tGravityAccenergyY58"),
get_json_object(col("value"),"$['tGravityAcc.iqr.X.60']").cast("float").alias("tGravityAcciqrX60"),
get_json_object(col("value"),"$['tGravityAcc.iqr.Y.61']").cast("float").alias("tGravityAcciqrY61"),
get_json_object(col("value"),"$['tGravityAcc.iqr.Z.62']").cast("float").alias("tGravityAcciqrZ62"),
get_json_object(col("value"),"$['tGravityAcc.entropy.X.63']").cast("float").alias("tGravityAccentropyX63"),
get_json_object(col("value"),"$['tGravityAcc.entropy.Y.64']").cast("float").alias("tGravityAccentropyY64"),
get_json_object(col("value"),"$['tGravityAcc.entropy.Z.65']").cast("float").alias("tGravityAccentropyZ65"),
get_json_object(col("value"),"$['tGravityAcc.arCoeff.Y.3.72']").cast("float").alias("tGravityAccarCoeffY372"),
get_json_object(col("value"),"$['tGravityAcc.arCoeff.Z.2.75']").cast("float").alias("tGravityAccarCoeffZ275"),
get_json_object(col("value"),"$['tGravityAcc.arCoeff.Z.4.77']").cast("float").alias("tGravityAccarCoeffZ477"),
get_json_object(col("value"),"$['tGravityAcc.correlation.X.Y.78']").cast("float").alias("tGravityAcccorrelationXY78"),
get_json_object(col("value"),"$['tGravityAcc.correlation.X.Z.79']").cast("float").alias("tGravityAcccorrelationXZ79"),
get_json_object(col("value"),"$['tGravityAcc.correlation.Y.Z.80']").cast("float").alias("tGravityAcccorrelationYZ80"),
get_json_object(col("value"),"$['tBodyAccJerk.mean.X.81']").cast("float").alias("tBodyAccJerkmeanX81"),
get_json_object(col("value"),"$['tBodyAccJerk.mean.Y.82']").cast("float").alias("tBodyAccJerkmeanY82"),
get_json_object(col("value"),"$['tBodyAccJerk.arCoeff.X.1.106']").cast("float").alias("tBodyAccJerkarCoeffX1106"),
get_json_object(col("value"),"$['tBodyAccJerk.arCoeff.X.2.107']").cast("float").alias("tBodyAccJerkarCoeffX2107"),
get_json_object(col("value"),"$['tBodyAccJerk.arCoeff.X.3.108']").cast("float").alias("tBodyAccJerkarCoeffX3108"),
get_json_object(col("value"),"$['tBodyAccJerk.arCoeff.Y.1.110']").cast("float").alias("tBodyAccJerkarCoeffY1110"),
get_json_object(col("value"),"$['tBodyAccJerk.arCoeff.Y.2.111']").cast("float").alias("tBodyAccJerkarCoeffY2111"),
get_json_object(col("value"),"$['tBodyAccJerk.arCoeff.Y.3.112']").cast("float").alias("tBodyAccJerkarCoeffY3112"),
get_json_object(col("value"),"$['tBodyAccJerk.arCoeff.Z.1.114']").cast("float").alias("tBodyAccJerkarCoeffZ1114"),
get_json_object(col("value"),"$['tBodyAccJerk.arCoeff.Z.2.115']").cast("float").alias("tBodyAccJerkarCoeffZ2115"),
get_json_object(col("value"),"$['tBodyAccJerk.arCoeff.Z.3.116']").cast("float").alias("tBodyAccJerkarCoeffZ3116"),
get_json_object(col("value"),"$['tBodyAccJerk.arCoeff.Z.4.117']").cast("float").alias("tBodyAccJerkarCoeffZ4117"),
get_json_object(col("value"),"$['tBodyAccJerk.correlation.X.Y.118']").cast("float").alias("tBodyAccJerkcorrelationXY118"),
get_json_object(col("value"),"$['tBodyAccJerk.correlation.X.Z.119']").cast("float").alias("tBodyAccJerkcorrelationXZ119"),
get_json_object(col("value"),"$['tBodyAccJerk.correlation.Y.Z.120']").cast("float").alias("tBodyAccJerkcorrelationYZ120"),
get_json_object(col("value"),"$['tBodyGyro.mean.X.121']").cast("float").alias("tBodyGyromeanX121"),
get_json_object(col("value"),"$['tBodyGyro.mean.Y.122']").cast("float").alias("tBodyGyromeanY122"),
get_json_object(col("value"),"$['tBodyGyro.iqr.Z.142']").cast("float").alias("tBodyGyroiqrZ142"),
get_json_object(col("value"),"$['tBodyGyro.entropy.X.143']").cast("float").alias("tBodyGyroentropyX143"),
get_json_object(col("value"),"$['tBodyGyro.entropy.Y.144']").cast("float").alias("tBodyGyroentropyY144"),
get_json_object(col("value"),"$['tBodyGyro.arCoeff.X.2.147']").cast("float").alias("tBodyGyroarCoeffX2147"),
get_json_object(col("value"),"$['tBodyGyro.arCoeff.X.3.148']").cast("float").alias("tBodyGyroarCoeffX3148"),
get_json_object(col("value"),"$['tBodyGyro.arCoeff.Y.2.151']").cast("float").alias("tBodyGyroarCoeffY2151"),
get_json_object(col("value"),"$['tBodyGyro.arCoeff.Y.3.152']").cast("float").alias("tBodyGyroarCoeffY3152"),
get_json_object(col("value"),"$['tBodyGyro.arCoeff.Y.4.153']").cast("float").alias("tBodyGyroarCoeffY4153"),
get_json_object(col("value"),"$['tBodyGyro.arCoeff.Z.2.155']").cast("float").alias("tBodyGyroarCoeffZ2155"),
get_json_object(col("value"),"$['tBodyGyro.arCoeff.Z.3.156']").cast("float").alias("tBodyGyroarCoeffZ3156"),
get_json_object(col("value"),"$['tBodyGyro.arCoeff.Z.4.157']").cast("float").alias("tBodyGyroarCoeffZ4157"),
get_json_object(col("value"),"$['tBodyGyro.correlation.X.Y.158']").cast("float").alias("tBodyGyrocorrelationXY158"),
get_json_object(col("value"),"$['tBodyGyro.correlation.X.Z.159']").cast("float").alias("tBodyGyrocorrelationXZ159"),
get_json_object(col("value"),"$['tBodyGyro.correlation.Y.Z.160']").cast("float").alias("tBodyGyrocorrelationYZ160"),
get_json_object(col("value"),"$['tBodyGyroJerk.mean.X.161']").cast("float").alias("tBodyGyroJerkmeanX161"),
get_json_object(col("value"),"$['tBodyGyroJerk.mean.Y.162']").cast("float").alias("tBodyGyroJerkmeanY162"),
get_json_object(col("value"),"$['tBodyGyroJerk.entropy.X.183']").cast("float").alias("tBodyGyroJerkentropyX183"),
get_json_object(col("value"),"$['tBodyGyroJerk.entropy.Z.185']").cast("float").alias("tBodyGyroJerkentropyZ185"),
get_json_object(col("value"),"$['tBodyGyroJerk.arCoeff.X.1.186']").cast("float").alias("tBodyGyroJerkarCoeffX1186"),
get_json_object(col("value"),"$['tBodyGyroJerk.arCoeff.X.2.187']").cast("float").alias("tBodyGyroJerkarCoeffX2187"),
get_json_object(col("value"),"$['tBodyGyroJerk.arCoeff.X.3.188']").cast("float").alias("tBodyGyroJerkarCoeffX3188"),
get_json_object(col("value"),"$['tBodyGyroJerk.arCoeff.X.4.189']").cast("float").alias("tBodyGyroJerkarCoeffX4189"),
get_json_object(col("value"),"$['tBodyGyroJerk.arCoeff.Y.1.190']").cast("float").alias("tBodyGyroJerkarCoeffY1190"),
get_json_object(col("value"),"$['tBodyGyroJerk.arCoeff.Y.3.192']").cast("float").alias("tBodyGyroJerkarCoeffY3192"),
get_json_object(col("value"),"$['tBodyGyroJerk.arCoeff.Z.1.194']").cast("float").alias("tBodyGyroJerkarCoeffZ1194"),
get_json_object(col("value"),"$['tBodyGyroJerk.arCoeff.Z.2.195']").cast("float").alias("tBodyGyroJerkarCoeffZ2195"),
get_json_object(col("value"),"$['tBodyGyroJerk.arCoeff.Z.3.196']").cast("float").alias("tBodyGyroJerkarCoeffZ3196"),
get_json_object(col("value"),"$['tBodyGyroJerk.arCoeff.Z.4.197']").cast("float").alias("tBodyGyroJerkarCoeffZ4197"),
get_json_object(col("value"),"$['tBodyGyroJerk.correlation.X.Y.198']").cast("float").alias("tBodyGyroJerkcorrelationXY198"),
get_json_object(col("value"),"$['tBodyGyroJerk.correlation.X.Z.199']").cast("float").alias("tBodyGyroJerkcorrelationXZ199"),
get_json_object(col("value"),"$['tGravityAccMag.max.217']").cast("float").alias("tGravityAccMagmax217"),
get_json_object(col("value"),"$['tGravityAccMag.entropy.222']").cast("float").alias("tGravityAccMagentropy222"),
get_json_object(col("value"),"$['tGravityAccMag.arCoeff3.225']").cast("float").alias("tGravityAccMagarCoeff3225"),
get_json_object(col("value"),"$['tBodyAccJerkMag.max.230']").cast("float").alias("tBodyAccJerkMagmax230"),
get_json_object(col("value"),"$['tBodyAccJerkMag.entropy.235']").cast("float").alias("tBodyAccJerkMagentropy235"),
get_json_object(col("value"),"$['tBodyAccJerkMag.arCoeff1.236']").cast("float").alias("tBodyAccJerkMagarCoeff1236"),
get_json_object(col("value"),"$['tBodyAccJerkMag.arCoeff2.237']").cast("float").alias("tBodyAccJerkMagarCoeff2237"),
get_json_object(col("value"),"$['tBodyAccJerkMag.arCoeff3.238']").cast("float").alias("tBodyAccJerkMagarCoeff3238"),
get_json_object(col("value"),"$['tBodyGyroMag.max.243']").cast("float").alias("tBodyGyroMagmax243"),
get_json_object(col("value"),"$['tBodyGyroMag.iqr.247']").cast("float").alias("tBodyGyroMagiqr247"),
get_json_object(col("value"),"$['tBodyGyroMag.arCoeff1.249']").cast("float").alias("tBodyGyroMagarCoeff1249"),
get_json_object(col("value"),"$['tBodyGyroMag.arCoeff2.250']").cast("float").alias("tBodyGyroMagarCoeff2250"),
get_json_object(col("value"),"$['tBodyGyroJerkMag.max.256']").cast("float").alias("tBodyGyroJerkMagmax256"),
get_json_object(col("value"),"$['tBodyGyroJerkMag.entropy.261']").cast("float").alias("tBodyGyroJerkMagentropy261"),
get_json_object(col("value"),"$['tBodyGyroJerkMag.arCoeff1.262']").cast("float").alias("tBodyGyroJerkMagarCoeff1262"),
get_json_object(col("value"),"$['tBodyGyroJerkMag.arCoeff2.263']").cast("float").alias("tBodyGyroJerkMagarCoeff2263"),
get_json_object(col("value"),"$['tBodyGyroJerkMag.arCoeff3.264']").cast("float").alias("tBodyGyroJerkMagarCoeff3264"),
get_json_object(col("value"),"$['fBodyAcc.max.Z.277']").cast("float").alias("fBodyAccmaxZ277"),
get_json_object(col("value"),"$['fBodyAcc.min.X.278']").cast("float").alias("fBodyAccminX278"),
get_json_object(col("value"),"$['fBodyAcc.min.Y.279']").cast("float").alias("fBodyAccminY279"),
get_json_object(col("value"),"$['fBodyAcc.entropy.Z.290']").cast("float").alias("fBodyAccentropyZ290"),
get_json_object(col("value"),"$['fBodyAcc.maxInds.X.291']").cast("float").alias("fBodyAccmaxIndsX291"),
get_json_object(col("value"),"$['fBodyAcc.maxInds.Y.292']").cast("float").alias("fBodyAccmaxIndsY292"),
get_json_object(col("value"),"$['fBodyAcc.maxInds.Z.293']").cast("float").alias("fBodyAccmaxIndsZ293"),
get_json_object(col("value"),"$['fBodyAcc.meanFreq.X.294']").cast("float").alias("fBodyAccmeanFreqX294"),
get_json_object(col("value"),"$['fBodyAcc.meanFreq.Y.295']").cast("float").alias("fBodyAccmeanFreqY295"),
get_json_object(col("value"),"$['fBodyAcc.meanFreq.Z.296']").cast("float").alias("fBodyAccmeanFreqZ296"),
get_json_object(col("value"),"$['fBodyAcc.skewness.Y.299']").cast("float").alias("fBodyAccskewnessY299"),
get_json_object(col("value"),"$['fBodyAcc.kurtosis.Y.300']").cast("float").alias("fBodyAcckurtosisY300"),
get_json_object(col("value"),"$['fBodyAcc.bandsEnergy.49.56.309']").cast("float").alias("fBodyAccbandsEnergy4956309"),
get_json_object(col("value"),"$['fBodyAcc.bandsEnergy.25.48.316']").cast("float").alias("fBodyAccbandsEnergy2548316"),
get_json_object(col("value"),"$['fBodyAcc.bandsEnergy.9.16.318']").cast("float").alias("fBodyAccbandsEnergy916318"),
get_json_object(col("value"),"$['fBodyAcc.bandsEnergy.49.56.323']").cast("float").alias("fBodyAccbandsEnergy4956323"),
get_json_object(col("value"),"$['fBodyAcc.bandsEnergy.49.56.337']").cast("float").alias("fBodyAccbandsEnergy4956337"),
get_json_object(col("value"),"$['fBodyAccJerk.max.Z.356']").cast("float").alias("fBodyAccJerkmaxZ356"),
get_json_object(col("value"),"$['fBodyAccJerk.min.X.357']").cast("float").alias("fBodyAccJerkminX357"),
get_json_object(col("value"),"$['fBodyAccJerk.min.Y.358']").cast("float").alias("fBodyAccJerkminY358"),
get_json_object(col("value"),"$['fBodyAccJerk.entropy.Z.369']").cast("float").alias("fBodyAccJerkentropyZ369"),
get_json_object(col("value"),"$['fBodyAccJerk.maxInds.X.370']").cast("float").alias("fBodyAccJerkmaxIndsX370"),
get_json_object(col("value"),"$['fBodyAccJerk.maxInds.Y.371']").cast("float").alias("fBodyAccJerkmaxIndsY371"),
get_json_object(col("value"),"$['fBodyAccJerk.meanFreq.X.373']").cast("float").alias("fBodyAccJerkmeanFreqX373"),
get_json_object(col("value"),"$['fBodyAccJerk.meanFreq.Y.374']").cast("float").alias("fBodyAccJerkmeanFreqY374"),
get_json_object(col("value"),"$['fBodyAccJerk.skewness.X.376']").cast("float").alias("fBodyAccJerkskewnessX376"),
get_json_object(col("value"),"$['fBodyAccJerk.skewness.Y.378']").cast("float").alias("fBodyAccJerkskewnessY378"),
get_json_object(col("value"),"$['fBodyAccJerk.skewness.Z.380']").cast("float").alias("fBodyAccJerkskewnessZ380"),
get_json_object(col("value"),"$['fBodyAccJerk.kurtosis.Z.381']").cast("float").alias("fBodyAccJerkkurtosisZ381"),
get_json_object(col("value"),"$['fBodyAccJerk.bandsEnergy.9.16.383']").cast("float").alias("fBodyAccJerkbandsEnergy916383"),
get_json_object(col("value"),"$['fBodyAccJerk.bandsEnergy.49.56.388']").cast("float").alias("fBodyAccJerkbandsEnergy4956388"),
get_json_object(col("value"),"$['fBodyAccJerk.bandsEnergy.25.48.395']").cast("float").alias("fBodyAccJerkbandsEnergy2548395"),
get_json_object(col("value"),"$['fBodyAccJerk.bandsEnergy.25.32.399']").cast("float").alias("fBodyAccJerkbandsEnergy2532399"),
get_json_object(col("value"),"$['fBodyAccJerk.bandsEnergy.49.56.402']").cast("float").alias("fBodyAccJerkbandsEnergy4956402"),
get_json_object(col("value"),"$['fBodyAccJerk.bandsEnergy.25.48.409']").cast("float").alias("fBodyAccJerkbandsEnergy2548409"),
get_json_object(col("value"),"$['fBodyAccJerk.bandsEnergy.1.8.410']").cast("float").alias("fBodyAccJerkbandsEnergy18410"),
get_json_object(col("value"),"$['fBodyAccJerk.bandsEnergy.25.32.413']").cast("float").alias("fBodyAccJerkbandsEnergy2532413"),
get_json_object(col("value"),"$['fBodyAccJerk.bandsEnergy.49.56.416']").cast("float").alias("fBodyAccJerkbandsEnergy4956416"),
get_json_object(col("value"),"$['fBodyGyro.max.Y.434']").cast("float").alias("fBodyGyromaxY434"),
get_json_object(col("value"),"$['fBodyGyro.max.Z.435']").cast("float").alias("fBodyGyromaxZ435"),
get_json_object(col("value"),"$['fBodyGyro.min.X.436']").cast("float").alias("fBodyGyrominX436"),
get_json_object(col("value"),"$['fBodyGyro.min.Y.437']").cast("float").alias("fBodyGyrominY437"),
get_json_object(col("value"),"$['fBodyGyro.entropy.Z.448']").cast("float").alias("fBodyGyroentropyZ448"),
get_json_object(col("value"),"$['fBodyGyro.maxInds.X.449']").cast("float").alias("fBodyGyromaxIndsX449"),
get_json_object(col("value"),"$['fBodyGyro.maxInds.Y.450']").cast("float").alias("fBodyGyromaxIndsY450"),
get_json_object(col("value"),"$['fBodyGyro.maxInds.Z.451']").cast("float").alias("fBodyGyromaxIndsZ451"),
get_json_object(col("value"),"$['fBodyGyro.meanFreq.X.452']").cast("float").alias("fBodyGyromeanFreqX452"),
get_json_object(col("value"),"$['fBodyGyro.meanFreq.Y.453']").cast("float").alias("fBodyGyromeanFreqY453"),
get_json_object(col("value"),"$['fBodyGyro.skewness.X.455']").cast("float").alias("fBodyGyroskewnessX455"),
get_json_object(col("value"),"$['fBodyGyro.skewness.Y.457']").cast("float").alias("fBodyGyroskewnessY457"),
get_json_object(col("value"),"$['fBodyGyro.skewness.Z.459']").cast("float").alias("fBodyGyroskewnessZ459"),
get_json_object(col("value"),"$['fBodyGyro.kurtosis.Z.460']").cast("float").alias("fBodyGyrokurtosisZ460"),
get_json_object(col("value"),"$['fBodyGyro.bandsEnergy.25.32.464']").cast("float").alias("fBodyGyrobandsEnergy2532464"),
get_json_object(col("value"),"$['fBodyGyro.bandsEnergy.49.56.467']").cast("float").alias("fBodyGyrobandsEnergy4956467"),
get_json_object(col("value"),"$['fBodyGyro.bandsEnergy.25.48.474']").cast("float").alias("fBodyGyrobandsEnergy2548474"),
get_json_object(col("value"),"$['fBodyGyro.bandsEnergy.1.8.475']").cast("float").alias("fBodyGyrobandsEnergy18475"),
get_json_object(col("value"),"$['fBodyGyro.bandsEnergy.9.16.476']").cast("float").alias("fBodyGyrobandsEnergy916476"),
get_json_object(col("value"),"$['fBodyGyro.bandsEnergy.25.32.478']").cast("float").alias("fBodyGyrobandsEnergy2532478"),
get_json_object(col("value"),"$['fBodyGyro.bandsEnergy.49.56.481']").cast("float").alias("fBodyGyrobandsEnergy4956481"),
get_json_object(col("value"),"$['fBodyGyro.bandsEnergy.33.48.485']").cast("float").alias("fBodyGyrobandsEnergy3348485"),
get_json_object(col("value"),"$['fBodyGyro.bandsEnergy.25.48.488']").cast("float").alias("fBodyGyrobandsEnergy2548488"),
get_json_object(col("value"),"$['fBodyGyro.bandsEnergy.1.8.489']").cast("float").alias("fBodyGyrobandsEnergy18489"),
get_json_object(col("value"),"$['fBodyGyro.bandsEnergy.9.16.490']").cast("float").alias("fBodyGyrobandsEnergy916490"),
get_json_object(col("value"),"$['fBodyGyro.bandsEnergy.33.40.493']").cast("float").alias("fBodyGyrobandsEnergy3340493"),
get_json_object(col("value"),"$['fBodyGyro.bandsEnergy.49.56.495']").cast("float").alias("fBodyGyrobandsEnergy4956495"),
get_json_object(col("value"),"$['fBodyAccMag.max.506']").cast("float").alias("fBodyAccMagmax506"),
get_json_object(col("value"),"$['fBodyAccMag.sma.508']").cast("float").alias("fBodyAccMagsma508"),
get_json_object(col("value"),"$['fBodyAccMag.entropy.511']").cast("float").alias("fBodyAccMagentropy511"),
get_json_object(col("value"),"$['fBodyAccMag.maxInds.512']").cast("float").alias("fBodyAccMagmaxInds512"),
get_json_object(col("value"),"$['fBodyAccMag.skewness.514']").cast("float").alias("fBodyAccMagskewness514"),
get_json_object(col("value"),"$['fBodyBodyAccJerkMag.max.519']").cast("float").alias("fBodyBodyAccJerkMagmax519"),
get_json_object(col("value"),"$['fBodyBodyAccJerkMag.entropy.524']").cast("float").alias("fBodyBodyAccJerkMagentropy524"),
get_json_object(col("value"),"$['fBodyBodyAccJerkMag.maxInds.525']").cast("float").alias("fBodyBodyAccJerkMagmaxInds525"),
get_json_object(col("value"),"$['fBodyBodyAccJerkMag.skewness.527']").cast("float").alias("fBodyBodyAccJerkMagskewness527"),
get_json_object(col("value"),"$['fBodyBodyGyroMag.max.532']").cast("float").alias("fBodyBodyGyroMagmax532"),
get_json_object(col("value"),"$['fBodyBodyGyroMag.sma.534']").cast("float").alias("fBodyBodyGyroMagsma534"),
get_json_object(col("value"),"$['fBodyBodyGyroMag.entropy.537']").cast("float").alias("fBodyBodyGyroMagentropy537"),
get_json_object(col("value"),"$['fBodyBodyGyroMag.skewness.540']").cast("float").alias("fBodyBodyGyroMagskewness540"),
get_json_object(col("value"),"$['fBodyBodyGyroJerkMag.max.545']").cast("float").alias("fBodyBodyGyroJerkMagmax545"),
get_json_object(col("value"),"$['fBodyBodyGyroJerkMag.entropy.550']").cast("float").alias("fBodyBodyGyroJerkMagentropy550"),
get_json_object(col("value"),"$['fBodyBodyGyroJerkMag.maxInds.551']").cast("float").alias("fBodyBodyGyroJerkMagmaxInds551"),
get_json_object(col("value"),"$['fBodyBodyGyroJerkMag.skewness.553']").cast("float").alias("fBodyBodyGyroJerkMagskewness553"),
get_json_object(col("value"),"$['fBodyBodyGyroJerkMag.kurtosis.554']").cast("float").alias("fBodyBodyGyroJerkMagkurtosis554"),
get_json_object(col("value"),"$['angle.tBodyAccMean.gravity.555']").cast("float").alias("angletBodyAccMeangravity555"),
get_json_object(col("value"),"$['angle.tBodyAccJerkMean.gravityMean.556']").cast("float").alias("angletBodyAccJerkMeangravityMean556"),
get_json_object(col("value"),"$['angle.tBodyGyroMean.gravityMean.557']").cast("float").alias("angletBodyGyroMeangravityMean557"),
get_json_object(col("value"),"$['angle.Z.gravityMean.561']").cast("float").alias("angleZgravityMean561")
)

df12 = df11.select(predict_udf(array([format_number(df11[col],5) for col in df11.columns])).cast("string").alias("prediction"))

df13 = df12 \
    .selectExpr("CAST(prediction AS STRING) as value") \
    .writeStream.format("kafka") \
    .option("checkpointLocation", ".checkpointloc/spark/") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "test-out") \
    .start()

df13.awaitTermination()
