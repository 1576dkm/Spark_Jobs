import re
from pyspark.sql.functions import lit, year, col, when, current_timestamp, concat
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DoubleType

from Sal_Expert.Utils_Salary.generic_func_config import schema_corrector
from Sal_Expert.Utils_Salary.metrics_utils import start_end_time
from Sal_Expert.Utils_Salary.reading_utils import read_harvests_collection, read_superadmin_collection, get_data_mysql
from Sal_Expert.Utils_Salary.synon_utils import synonimise_location_df
from Sal_Expert.Utils_Salary.writing_utils import write_superadmin_collection
from Sal_Expert.config_files.JD_ETL_Config import generate_mongo_pipeline


class SalaryExpert(object):
    # <<========================== Getting Start time and end time from mongo =====================>>

    def __init__(self, spark):

        self.metrics_database = "Production_ETL"
        self.metrics_collection = "salary_merged_etl"
        self.mongo_collection = "salary_metrics_etl"
        self.source = "salary_expert"
        self.spark = spark
        self.salary_exp_loc = None
        self.salary_exp_loc_1 = None
        self.past_data = read_superadmin_collection(mongo_database=self.metrics_database,
                                                mongo_collection=self.mongo_collection,
                                                spark=self.spark).filter(
        col("source") == self.source)
        self.start_time, self.end_time = start_end_time(self.spark, self.metrics_database,
                                                    self.metrics_collection, self.mongo_collection,
                                                    self.source).start_end_time()
        self.col_name = "created_at"
        self.mongo_pipeline = generate_mongo_pipeline(self.start_time, self.end_time, self.col_name)
        self.salary_exp = read_harvests_collection(mongo_database="harvests", mongo_collection=self.source,
                                               pipeline=self.mongo_pipeline,
                                               spark=self.spark)

    # <<============== Reading from mongo pipeline and checking whether we got the new data or not ===================>>

    # col_name = "created_at"
    # mongo_pipeline = generate_mongo_pipeline(self.start_time, self.end_time, col_name)
    # salary_exp = read_harvests_collection("harvests", "salary_expert", pipeline=mongo_pipeline)

    def read_from_mongo(self):

        print(self.mongo_pipeline)

    if self.salary_exp.count() == 0:
        return 0

    # <<=================== Getting Currency Conversion data from mysql and correcting the schema ====================>>

    def correcting_schema(self):

        salary_exp = self.salary_exp.withColumn("avg_base_salary", col("avg_base_salary").cast(StringType()))

    # salary_exp = salary_exp.join(past_data, ["data_lake_id", "source"], "leftanti")

    # display(salary_exp)
    currency_data = get_data_mysql("iris1", "iris1_mastercurrency").select("year", "currency_name",
                                                                           "usd_conversion_rate",
                                                                           spark=self.spark)

    mapper_dict = {"data_lake_id": "_id.oid", "job_title": "job_title", "avg_base_salary": "avg_base_salary",
                   "currency_name": "currency", "min_salary": "salary_by_experience.Entry",
                   "max_salary": "salary_by_experience.Senior", "last_modified": "updated_at", "city": "city",
                   "country": "country"}

    salary_exp_new = schema_corrector(salary_exp, mapper_dict).withColumn("count", lit(1)) \
        .withColumn("year", when(col("last_modified").isNotNull(), year(col("last_modified"))).otherwise(2018)) \
        .join(currency_data, ["currency_name", "year"]).withColumn("city", col("city").cast(StringType()))

    salary_exp_new.write.mode("overwrite").parquet("dbfs/FileStore/Prabal/salary_expert_data")

    a = {"data_lake_id": "_id.oid", "job_title": "job_title", "avg_base_salary": "avg_base_salary",
         "currency_name": "currency", "min_salary": "salary_by_experience.Entry",
         "max_salary": "salary_by_experience.Senior", "last_modified": "updated_at", "city": "city",
         "country": "country"}
    temp_1 = []
    for i in a:
        temp_1.append((i, a[i]))
    print(temp_1)
    print(self.spark.read.parquet("dbfs/FileStore/Prabal/salary_expert_data").count())

    ''' <<=========================================== Extracting Salary ==========================================>>'''

    def extract_salary(self):

    def extract_mean_salary(salary, usd_conversion_rate):

        """
        Extract salary from text and converts it to annual USD.
        """
    if not salary:
        return None
    if "/yr" in salary:
        list_salary = re.findall(r"[-+]?\d*\.\d+|\d+", salary.replace(",", ""))
    if list_salary == []:
        return None
    print(list_salary, salary.replace(",", ""))
    salary_new = float(list_salary[0]) * usd_conversion_rate
    if salary_new > 2000:
        return salary_new
    else:
    salary_new = salary_new * 1000
    if salary_new > 4000:
        return salary_new
    else:
    return None
    else:
    list_salary = re.findall(r"[-+]?\d*\.\d+|\d+", salary)
    if not list_salary:
        return None
    salary_new = float(list_salary[0]) * usd_conversion_rate
    if salary_new > 2000:
        return salary_new
    else:
    salary_new = salary_new * 1000
    if salary_new > 4000:
        return salary_new
    else:
    return None

    udf_extract_mean_salary = F.udf(extract_mean_salary, DoubleType())

    salary_exp_new = self.spark.read.parquet("dbfs/FileStore/Prabal/salary_expert_data").repartition(200) \
        .withColumn("mean_salary", udf_extract_mean_salary(col("avg_base_salary"), col("usd_conversion_rate"))) \
        .withColumn("location", concat(col("city"), lit(", "), col("country"))) \
        .withColumn("location",
                    when((col("location").isNull() & col("city").isNull()), col("country")).otherwise(
                        col("location")))

    salary_exp_loc = synonimise_location_df(salary_exp_new, "location")  ######

    # display(salary_exp_loc)

    salary_exp_loc_1 = salary_exp_loc.withColumn("max_salary",
                                                 col("max_salary") * col("usd_conversion_rate")).withColumn(
        "min_salary", col("min_salary") * col("usd_conversion_rate")).filter(
        col("mean_salary").isNotNull()).withColumn(
        "last_modified",
        when(col("last_modified").isNull(), current_timestamp()).otherwise(col("last_modified"))).withColumn(
        "source",
        lit(
            "salary_expert")).filter(
        col("mean_salary") > 1000)

    company_df = read_superadmin_collection(self.metrics_database, "company_synon_JD").filter(
        (col("is_verified") == 1) & (col("is_product_company"))).select("company", "mvp_company_name").withColumn(
        "lower_company", F.lower(col("company"))).drop("company")

    self.salary_exp_loc_1 = self.salary_exp_loc_1.withColumn("lower_company", F.lower(col("company"))).join(
        company_df.distinct(),
        ["lower_company"],
        "left")

    # <<======================================== Writing to mongo ==================================>>

    def write_to_mongo(self):

        to_update = self.salary_exp_loc_1.join(self.past_data.select("_id", "data_lake_id"), ["data_lake_id"])

    salary_exp_loc_1 = self.salary_exp_loc_1.join(self.past_data.select("data_lake_id"), ["data_lake_id"],
                                                  "left_anti")

    write_superadmin_collection(salary_exp_loc_1, self.metrics_database, self.metrics_collection)  ######

    write_superadmin_collection(to_update, self.metrics_database, self.metrics_collection)  #####

    start_end_time(self.spark, self.metrics_database, self.metrics_collection, self.mongo_collection,
                   self.source).write_etl_metrics(
        "Salary_Expert_ETL", self.metrics_database, self.mongo_collection,
        self.start_time,
        self.end_time,
        salary_exp_loc_1.count() + to_update.count(),
        write_superadmin_collection,
        self.source)  #######

    self.salary_exp_loc.repartition(1).write.mode("append").parquet("dbfs/FileStore/Prabal/salary_data_final_new")
    # display(salary_exp_loc_1)

    print(to_update.count())
    print(salary_exp_loc_1.count())


def execute(spark):
    cls = SalaryExpert(spark)
    try:
        if cls.read_from_mongo() != 0:
        cls.correcting_schema()
    cls.extract_salary()
    cls.write_to_mongo()
    else:
    raise Exception("No new data found!!!")
    except Exception as e:
    print(e)
