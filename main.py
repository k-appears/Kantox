from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import lag, lead, col, when, monotonically_increasing_id
from pyspark.sql.types import DoubleType


def find_outliers(df_outliers):
    """
    Detect possible outliers using Tukey's method. The method is based on the interquartile range (IQR).
    The IQR is the difference between the 75th and 25th percentiles of the data.
    Any data point that is beyond 1.5 times the IQR of the 75th percentile or below 1.5 times the IQR of the 25th percentile is considered an outlier.
    IRQ for FX Rates taking into account the following columns:
    - High
    - Low
    :param df_outliers:  pyspark.sql.DataFrame
        A PySpark dataframe with the following columns:
        - _c0: integer Sequence
        - Symbol: string Currency, the exchange rate between two currencies
        - StartDate: string Date of the exchange rate
        - StartTime: string Time of the exchange rate
        - EndDate: string Date of the exchange rate
        - EndTime: string Time of the exchange rate
        - Open: double  Exchange rate at the beginning of the period
        - High: double Highest exchange rate during the period
        - Low: double Lowest exchange rate during the period
        - Close: double Exchange rate at the end of the period
        - Average: double   Average exchange rate during the period
    :return:
    """
    # Using the `for` loop to create new columns by identifying the outliers for each feature
    for column in df_outliers.select('High', 'Low').columns:
        # Calculate the first and third quartiles
        first_quartile = df_outliers.approxQuantile(column, [0.25], 0)
        third_quartile = df_outliers.approxQuantile(column, [0.75], 0)
        # Calculate the interquartile range
        iqr = third_quartile[0] - first_quartile[0]
        # Calculate the outlier step
        outlier_step = 1.5 * iqr
        # Identify the outliers for column
        outliers = df_outliers.filter(
            (df_outliers[column] < first_quartile[0] - outlier_step) | (
                        df_outliers[column] > third_quartile[0] + outlier_step))
        print("The outliers for the column {} are:".format(column))
        outliers.show()


def fill_missing_rates(df_param):
    """
    Return a dataset with the missing ids and fill the rest of the columns with null values
    :param df_param: pyspark.sql.DataFrame
        A PySpark dataframe with the following columns:
        - id: integer Sequence from 1 to the numbers of rows
        - Symbol: string Currency, the exchange rate between two currencies
        - StartDate: string Date of the exchange rate
        - StartTime: string Time of the exchange rate
        - EndDate: string Date of the exchange rate
        - EndTime: string Time of the exchange rate
        - Open: double  Exchange rate at the beginning of the period
        - High: double Highest exchange rate during the period
        - Low: double Lowest exchange rate during the period
        - Close: double Exchange rate at the end of the period
        - Average: double   Average exchange rate during the period
    :return: pyspark.sql.DataFrame
        A PySpark dataframe same as the input with the missing rates filled
    """
    # Create a dataframe sequence with the rows from 1 to the number of rows
    df_seq = spark.range(1, df_param.count() + 1).toDF("id")

    # join the two dataframes to get the missing rows
    df_missing = df_seq.join(df_param, df_seq.id == df_param.id, how='left_anti')
    # Add the missing rows to the original dataframe
    df_new = df_param.unionByName(df_missing, allowMissingColumns=True)
    # Sort the dataframe by id
    df_new = df_new.sort("id")

    return df_new




    # Join the two dataframes to get the missing rows and add them to the original dataframe


def remove_unmatching_rate(df_filled):
    """
    Remove the rows that have the same StartDate, StartTime, EndDate, EndTime, Open, High, Low, Close, Average but different Symbol
    :param df_filled:
    :return:
    """
    #  groupBy all columns except id and Symbol and show the count rows
    df_grouped = df_filled.groupBy("StartDate", "StartTime", "EndDate", "EndTime", "Open", "High", "Low", "Close",
                                   "Average") \
        .count().filter(col("count") % 2 != 0)
    # merge df_filled and df_grouped
    df_merged = df_filled.join(df_grouped,
                               ["StartDate", "StartTime", "EndDate", "EndTime", "Open", "High", "Low", "Close",
                                "Average"], how='left_anti')
    #  remove df_merged from df_filled by id
    df_result = df_filled.join(df_merged, ["id"], how='left_anti')
    return df_result


if __name__ == "__main__":
    # Use Pyspark  Read the data from csv file in data folder
    spark = SparkSession.builder.appName("Kantox").getOrCreate()
    df = spark.read.csv("data/FXRates.csv", header=True, inferSchema=True)
    df1 = df.withColumnRenamed("_c0", "id")

    # Print the schema of the data
    # dprintSchema()
    # dhead()
    # find_outliers(df)
    df_filled = fill_missing_rates(df1)
    df_cleaned = remove_unmatching_rate(df_filled)
    # save to parquet file
    df_cleaned.write.parquet("data/FXRates.parquet")

    spark.stop()
