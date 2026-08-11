from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("CheckSpark")
        .master("local[*]")
        .getOrCreate()
    )

    print("Spark version:", spark.version)

    hadoop_version = (
        spark.sparkContext
        ._jvm
        .org.apache.hadoop.util.VersionInfo
        .getVersion()
    )

    print("Hadoop version:", hadoop_version)

    spark.stop()


if __name__ == "__main__":
    main()

