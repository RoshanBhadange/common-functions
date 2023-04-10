from pyspark.sql.window import Window
import pyspark.sql.functions as F

def latest_record(dataframe, partition_column):
    win = Window.partitionBy(partition_column)

    df = dataframe.withColumn("rnk", F.row_number().over(win)
    .orderBy(F.col(partition_column).desc()))

    df.filter(F.col("rnk")==1).drop("rnk")
    return df