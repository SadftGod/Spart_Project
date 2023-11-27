def info_view(df):
   print(f"General information about the {df}: ")
   
   df.printSchema()
   df.show(5)
   num_rows = df.count()
   columns_df = df.columns
   num_columns = len(columns_df)
   
   
   print(f"\n Number of rows: {num_rows}")
   print(f"\n Number of columns: {num_columns}")
   
   print(f"\n Columns: {columns_df}")
   
   print("\n")
   
def read(spark, input_directory):
    df = spark.read.json(input_directory)
    return df


def write(df, directory_to_write):
    df.write.csv(directory_to_write, header=True, )
    return