import sys
from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import when
#feel free to def new functions if you need

def create_dataframe(filepath, format, spark):
    """
    Create a spark df given a filepath & format.

    :param filepath: <str>, the filepath
    :param format: <str>, the file format (e.g. "csv" or "json")
    :param spark: <str> the spark session

    :return: the spark df uploaded
    """

    #add your code here
    #spark_df = None #temporary placeholder

    spark_df = spark.read.load(filepath, format=format, header=True)
    spark_df.show()
    return spark_df


def transform_nhis_data(nhis_df):
    """
    Transform df elements

    :param nhis_df: spark df
    :return: spark df, transformed df
    """

    #add your code here
    #transformed_df = None #temporary placeholder
    
    transformed_df = nhis_df.withColumn("AGE_P", 
                                        when((nhis_df.AGE_P >= 18) & (nhis_df.AGE_P <= 24), 1)
                                        .when((nhis_df.AGE_P >= 25) & (nhis_df.AGE_P <= 29), 2)
                                        .when((nhis_df.AGE_P >= 30) & (nhis_df.AGE_P <= 34), 3)
                                        .when((nhis_df.AGE_P >= 35) & (nhis_df.AGE_P <= 39), 4)
                                        .when((nhis_df.AGE_P >= 40) & (nhis_df.AGE_P <= 44), 5)
                                        .when((nhis_df.AGE_P >= 45) & (nhis_df.AGE_P <= 49), 6)
                                        .when((nhis_df.AGE_P >= 50) & (nhis_df.AGE_P <= 54), 7)
                                        .when((nhis_df.AGE_P >= 55) & (nhis_df.AGE_P <= 59), 8)
                                        .when((nhis_df.AGE_P >= 60) & (nhis_df.AGE_P <= 64), 9)
                                        .when((nhis_df.AGE_P >= 65) & (nhis_df.AGE_P <= 69), 10)
                                        .when((nhis_df.AGE_P >= 70) & (nhis_df.AGE_P <= 74), 11)
                                        .when((nhis_df.AGE_P >= 75) & (nhis_df.AGE_P <= 79), 12)
                                        .when((nhis_df.AGE_P >= 80), 13)
                                        .when((nhis_df.AGE_P >= 7) & (nhis_df.AGE_P <= 9), 14)
                                        .otherwise(None))
    transformed_df = transformed_df.withColumn("MRACBPI2", 
                                               when(transformed_df.MRACBPI2 == 3, 4)
                                               .when(transformed_df.MRACBPI2 == 6, 3)
                                               .when(transformed_df.MRACBPI2 == 7, 3)
                                               .when(transformed_df.MRACBPI2 == 12, 3)
                                               .when((transformed_df.MRACBPI2 == 16) | (transformed_df.MRACBPI2 == 17), 6)
                                               .otherwise(transformed_df.MRACBPI2))
    transformed_df = transformed_df.withColumn("MRACBPI2", 
                                               when((transformed_df.HISPAN_I == 0) 
                                                    | (transformed_df.HISPAN_I == 1) 
                                                    | (transformed_df.HISPAN_I == 2) 
                                                    | (transformed_df.HISPAN_I == 3) 
                                                    | (transformed_df.HISPAN_I == 4) 
                                                    | (transformed_df.HISPAN_I == 5) 
                                                    | (transformed_df.HISPAN_I == 6) 
                                                    | (transformed_df.HISPAN_I == 7) 
                                                    | (transformed_df.HISPAN_I == 8) 
                                                    | (transformed_df.HISPAN_I == 9) 
                                                    | (transformed_df.HISPAN_I == 10) 
                                                    | (transformed_df.HISPAN_I == 13), 5)
                                                    .otherwise(transformed_df.MRACBPI2))
    transformed_df = transformed_df.withColumnRenamed("AGE_P", "_AGEG5YR").withColumnRenamed("MRACBPI2", "_IMPRACE")
    #transformed_df.show()
    return transformed_df


def calculate_statistics(joined_df):
    """
    Calculate prevalence statistics

    :param joined_df: the joined df

    :return: None
    """

    #add your code here
    # find prevalence of 1 in DIBEV1 based on MRACBPI2
    race_stat = joined_df.select("_IMPRACE", "DIBEV1", "_LLCPWT").where(joined_df.DIBEV1 == 1).groupBy("_IMPRACE").sum("_LLCPWT").alias("sum(_LLCPWT)")
    race_total = joined_df.select("_IMPRACE", "_LLCPWT").groupBy("_IMPRACE").sum("_LLCPWT").alias("sum(_LLCPWT)")
    race_prevalence = race_stat.join(race_total, race_stat._IMPRACE == race_total._IMPRACE).select(race_stat._IMPRACE, (race_stat["sum(_LLCPWT)"]/race_total["sum(_LLCPWT)"]).alias("Prevalence")).orderBy("Prevalence", ascending=False)
    #race_stat.show()
    #race_total.show()
    race_prevalence.show()

    gender_stat = joined_df.select("SEX", "DIBEV1", "_LLCPWT").where(joined_df.DIBEV1 == 1).groupBy("SEX").sum("_LLCPWT").alias("sum(_LLCPWT)")
    gender_total = joined_df.select("SEX", "_LLCPWT").groupBy("SEX").sum("_LLCPWT").alias("sum(_LLCPWT)")
    gender_prevalence = gender_stat.join(gender_total, gender_stat.SEX == gender_total.SEX).select(gender_stat.SEX, (gender_stat["sum(_LLCPWT)"]/gender_total["sum(_LLCPWT)"]).alias("Prevalence")).orderBy("Prevalence", ascending=False)
    #gender_stat.show()
    #gender_total.show()
    gender_prevalence.show()

    age_stat = joined_df.select("_AGEG5YR", "DIBEV1", "_LLCPWT").where(joined_df.DIBEV1 == 1).groupBy("_AGEG5YR").sum("_LLCPWT").alias("sum(_LLCPWT)")
    age_total = joined_df.select("_AGEG5YR", "_LLCPWT").groupBy("_AGEG5YR").sum("_LLCPWT").alias("sum(_LLCPWT)")
    age_prevalence = age_stat.join(age_total, age_stat._AGEG5YR == age_total._AGEG5YR).select(age_stat._AGEG5YR, (age_stat["sum(_LLCPWT)"]/age_total["sum(_LLCPWT)"]).alias("Prevalence")).orderBy("Prevalence", ascending=False)
    #age_stat.show()
    #age_total.show()
    age_prevalence.show()
    pass

def join_data(brfss_df, nhis_df):
    """
    Join dataframes

    :param brfss_df: spark df
    :param nhis_df: spark df after transformation
    :return: the joined df

    """
    #add your code here
    #joined_df = None ##temporary placeholder

    #joined_df = brfss_df.join(nhis_df).where((brfss_df.SEX == nhis_df.SEX) & (brfss_df._AGEG5YR == nhis_df._AGEG5YR) & (brfss_df._IMPRACE == nhis_df._IMPRACE)).na.drop("all")
    joined_df = brfss_df.join(nhis_df,  ["SEX", "_AGEG5YR", "_IMPRACE"]).na.drop("all").select(brfss_df["*"], nhis_df["DIBEV1"])
    joined_df.show()
    return joined_df

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    arg_parser.add_argument('nhis', type=str, default=None, help="brfss filename")
    arg_parser.add_argument('brfss', type=str, default=None, help="nhis filename")
    arg_parser.add_argument('-o', '--output', type=str, default=None, help="output path(optional)")

    #parse args
    args = arg_parser.parse_args()
    if not args.nhis or not args.brfss:
        arg_parser.usage = arg_parser.format_help()
        arg_parser.print_usage()
    else:
        brfss_filename = args.nhis
        nhis_filename = args.brfss

        # Start spark session
        spark = SparkSession.builder.getOrCreate()

        # load dataframes
        brfss_df = create_dataframe(brfss_filename, 'json', spark)
        nhis_df = create_dataframe(nhis_filename, 'csv', spark)

        # Perform mapping on nhis dataframe
        nhis_df = transform_nhis_data(nhis_df)
        # Join brfss & nhis df
        joined_df = join_data(brfss_df, nhis_df)
        # Calculate statistics
        calculate_statistics(joined_df)

        # Save
        if args.output:
            joined_df.write.csv(args.output, mode='overwrite', header=True)


        # Stop spark session 
        spark.stop()