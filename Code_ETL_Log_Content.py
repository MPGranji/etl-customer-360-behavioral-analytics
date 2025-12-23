import os
from datetime import datetime, timedelta 
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import  *
import pyspark.sql.functions as sf
from pyspark.sql.window import Window

spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

def read_json_from_path(path):
    df = spark.read.json(path)
    return df

def select_fields(df):
    """Selecting all fields from the '_source' struct"""
    df = df.select('_source.*')
    return df

def convert_to_datevalue(string):
    """Convert string date (yyyymmdd) to Python date object"""
    date_value=datetime.strptime(string,"%Y%m%d").date()
    return date_value
    
def convert_to_stringvalue(date):
    """Convert Python date object back to string (yyyymmdd)"""
    string_value = date.strftime("%Y%m%d")    
    return string_value

def date_range(start_date,end_date):
    """Generate a list of string dates between start and end dates for file iteration"""
    date_list=[]
    current_date=start_date
    while(current_date<=end_date):
        date_list.append(convert_to_stringvalue(current_date))
        current_date+=timedelta(days=1)
    return date_list

def transform_category(df):
    """Categorize AppName into high-level content types (TV, Movies, Sports, etc.)"""
    df = df.withColumn("Type",
            when(col("AppName").isin('CHANNEL', 'DSHD', 'KPLUS', 'KPlus', 'APP'), "Truyền Hình")
            .when(col("AppName").isin('VOD', 'FIMS_RES', 'BHD_RES', 'VOD_RES', 'FIMS', 'BHD', 'DANET'), "Phim Truyện")
            .when((col("AppName") == 'RELAX'), "Giải Trí")
            .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
            .when((col("AppName") == 'SPORT'), "Thể Thao")
            .otherwise("Error"))
    return df

def calculate_statistics(df, active_status_df):
    """Aggregate total duration per contract and pivot content types into columns"""
    statistics = df.groupBy('Contract', 'Type').agg(sf.sum('TotalDuration').alias('TotalDuration'))
    statistics = statistics.groupBy('Contract').pivot('Type').sum('TotalDuration')
    result = statistics.join(active_status_df, on='Contract', how='left')
    return result

def most_watch(df):
    """Identify the most consumed content category for each contract"""
    df=df.withColumn("MostWatch", greatest(col("Giải Trí"),col("Phim Truyện"),col("Thể Thao"),col("Thiếu Nhi"),col("Truyền Hình")))
    df=df.withColumn("MostWatch",
                     when(col("MostWatch")==col("Truyền Hình"),"Truyền Hình")
                     .when(col("MostWatch")==col("Phim Truyện"),"Phim Truyện")
                     .when(col("MostWatch")==col("Thể Thao"),"Thể Thao")
                     .when(col("MostWatch")==col("Thiếu Nhi"),"Thiếu Nhi")
                     .when(col("MostWatch")==col("Giải Trí"),"Giải Trí"))
    return df 

def customer_taste(df):
    """Concatenate all consumed content types into a single 'Taste' string"""
    df=df.withColumn("Taste", concat_ws("-",
                     when(col("Giải Trí").isNotNull() , lit("Giải Trí"))
                     ,when(col("Phim Truyện").isNotNull(), lit("Phim Truyện"))
                     ,when(col("Thể Thao").isNotNull(), lit("Thể Thao"))
                     ,when(col("Thiếu Nhi").isNotNull(), lit("Thiếu Nhi"))
                     ,when(col("Truyền Hình").isNotNull(), lit("Truyền Hình"))))
    return df

def find_active(df):
    """
    Calculate user activity level based on unique active days.
    Label users as 'High' if they are active for 15+ days.
    """
    active_counts = df.groupBy("Contract").agg(sf.countDistinct("Date").alias("TotalDaysOnline"))
    active_status = active_counts.withColumn("Active", 
                                             sf.when(col("TotalDaysOnline") >= 15, "High")
                                             .otherwise("Low"))
    
    return active_status.select("Contract", "Active")

def save_data(result, save_path):
    """Save the final DataFrame to a single CSV file"""
    (result.repartition(1)
           .write
           .option("header", "true")
           .mode("overwrite")
           .csv(save_path))
    return print("Data Saved Successfully")

def import_to_mysql(df, table_name, mode="overwrite"): 
    """Write the processed data to a MySQL database via JDBC"""
    db_url = "jdbc:mysql://localhost:3306/etl_data?useSSL=false&allowPublicKeyRetrieval=true"
    properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    df.write.jdbc(
        url=db_url,
        table=table_name,
        mode=mode,          
        properties=properties
    )
    
    print(f"Imported into {table_name} (mode: {mode})")

def etl_main(df):
    """Core ETL pipeline orchestration"""
    print('--------------------------------')
    print('Transforming Category')
    print('--------------------------------')
    df = transform_category(df)
    print('--------------------------------')
    print('Find active status (Unique per Contract)')
    print('--------------------------------')
    active_status_df = find_active(df)
    print('--------------------------------')
    print('Calculating Statistics & Joining')
    print('--------------------------------')
    result = calculate_statistics(df, active_status_df)
    print('--------------------------------')
    print('Find most watch')
    print('--------------------------------')
    result = most_watch(result)
    print('--------------------------------')
    print('Find customer taste')
    print('--------------------------------')
    result = customer_taste(result)
    return result

def list_files(path):
    list_files = os.listdir(path)
    print(list_files)
    # print("How many files you want to ETL")
    return list_files

def main_task(input_path, output_path):
    list_files(input_path)
    start_day = (str(input('Please input start_date format yyyymmdd:')))
    start_day = convert_to_datevalue(start_day)

    to_day = str(input("Please input to_date format yyyymmdd: "))
    to_day = convert_to_datevalue(to_day)

    date_list = []
    date_list = date_range(start_day, to_day)

    total_days = (to_day - start_day).days + 1

    print("Total days to ETL: " + str(total_days))
    print(date_list)

    start_time = datetime.now()
    df = read_json_from_path(input_path + date_list[0] + '.json')
    df = select_fields(df)
    df = df.withColumn("Date", to_date(lit(date_list[0]), "yyyyMMdd"))
    for day_path in date_list[1:]:
        print("ETL_TASK " + input_path + day_path + ".json")
        new_df = read_json_from_path(input_path + day_path + '.json')
        new_df = select_fields(new_df)
        new_df = new_df.withColumn("Date", to_date(lit(day_path), "yyyyMMdd"))
        print("Union df with new df")
        df = df.union(new_df)
    print("Calculation on final output")
    result = etl_main(df)

    print('--------------------------------')
    print('Saving csv output')
    print('--------------------------------')
    save_data(result,output_path)
    print('--------------------------------')
    print('Importing to MySQL')
    print('--------------------------------')
    import_to_mysql(result, 'customer_content_consumption_profile', mode='overwrite')

    end_time = datetime.now()
    print((end_time - start_time).total_seconds())

# def input_path():
#     url = str(input('Please provide database source folder:'))
#     return url

# def output_path():
#     url = str(input('Please provide destination folder:'))
#     return url

# input_path = input_path()
# output_path = output_path()

if __name__ == "__main__":
    # Define system paths
    input_path = "E:\\Dataset\\log_content\\"
    output_path = "E:\\project\\final_project\\DataClean\\Log_Content\\"

    df = main_task(input_path, output_path)