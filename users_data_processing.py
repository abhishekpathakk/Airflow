from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

def main(input_file, output_file):
    spark = SparkSession.builder.appName("Shuffle Data").getOrCreate()
    
    # Read input data from the CSV file
    df = spark.read.csv(input_file, header=True, inferSchema=True)

    # Perform shuffling operation (example: random sampling)
    shuffled_df = df.orderBy(rand())  # Shuffle the DataFrame randomly

    # Write output data to a new CSV file
    shuffled_df.write.csv(output_file, header=True)

    spark.stop()

if __name__ == "__main__":
    import sys
    main(sys.argv[1], sys.argv[2])
