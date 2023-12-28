from pyspark.sql.functions import when, col
import great_expectations
from pyspark.sql import Window
from pyspark.sql.functions import lit, row_number, current_timestamp
from pyspark.sql.functions import max
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, TimestampType
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

inputDF = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options={"paths": [
        "s3://awsglue-datasets/examples/us-legislators/all/memberships.json"]},
    format="json"
)

inputDF.show(3)

teams_schema = StructType([
    StructField('id', IntegerType(), False),
    StructField('abbreviation', StringType(), True),
    StructField('city', StringType(), True),
    StructField('conference', StringType(), True),
    StructField('division', StringType(), True),
    StructField('full_name', StringType(), True),
    StructField('name', StringType(), True)
])

teams_gold_schema = StructType([
    StructField('id', IntegerType(), False),
    StructField('abbreviation', StringType(), True),
    StructField('city', StringType(), True),
    StructField('conference', StringType(), True),
    StructField('division', StringType(), True),
    StructField('full_name', StringType(), True),
    StructField('name', StringType(), True),
    StructField('is_active', BooleanType(), True),
    StructField('eff_start_date', TimestampType(), True),
    StructField('eff_end_date', TimestampType(), True),
    StructField('team_key', IntegerType(), True)
])


# Teams in gold container
teams_gold_df = None

# Teams in bronze container
teams_df = spark.read.json(
    f'synfs:/{job_id}/mnt/bronze/teams/teams.json', teams_schema)

for table in spark.catalog.listTables('prize_picks_gold'):
    if table.name == 'dim_team':
        teams_gold_df = spark.read.format('delta').load(
            f'synfs:/{job_id}/mnt/gold/dim_team')
    else:
        teams_gold_df = spark.createDataFrame([], teams_gold_schema)

# New teams
teams_diff_df = teams_df.exceptAll(teams_gold_df.drop('is_active').drop(
    'eff_start_date').drop('eff_end_date').drop('team_key')).coalesce(8)


# Get Max team_key From Gold Container
max_value = None

if teams_gold_df.count() > 0:
    max_value_df = teams_gold_df.agg(max(teams_gold_df.team_key))
    max_value = max_value_df.collect()[0][0]

max_team_key = max_value if max_value is not None else 0


# Add Date and key attributes
teams_diff_updated_df = None
spec = Window.orderBy(teams_diff_df.full_name.asc())

teams_diff_updated_df = teams_diff_df \
    .withColumn('is_active', lit(True)) \
    .withColumn('eff_start_date', current_timestamp()) \
    .withColumn('eff_end_date', lit('1900-01-01 00:00:00.000').cast('timestamp')) \
    .withColumn('team_key', row_number().over(spec) + max_team_key)

teams_gold_df = teams_gold_df.withColumnRenamed('team_id', 'id')

combined_teams_df = teams_gold_df.unionByName(teams_diff_updated_df)

max_date_df = combined_teams_df.groupBy(combined_teams_df.id.alias('groupby_id')) \
    .agg(max(combined_teams_df.eff_start_date).alias('max_date'))

teams_scd_df = combined_teams_df.join(max_date_df, (combined_teams_df.id == max_date_df.groupby_id) & (combined_teams_df.eff_start_date == max_date_df.max_date), 'left') \
    .drop(max_date_df.groupby_id)


# Update Dimensions
teams_final_df = teams_scd_df.withColumnRenamed('id', 'team_id') \
    .withColumn('is_active', when(col('max_date').isNull(), lit(False)).otherwise(col('is_active'))) \
    .withColumn('eff_end_date', when(col('max_date').isNull(), current_timestamp()).otherwise(col('eff_end_date'))) \
    .drop('max_date')


# ### Data Quality Checks

# workflow = DataQualityWorkflow(great_expectations)

# validator = workflow.create_validator(teams_final_df)

# workflow.validator_add_type_check(teams_final_df)
# workflow.validator_add_not_null(teams_final_df)

# validator.save_expectation_suite()

# checkpoint = workflow.create_checkpoint("teams_final", validator)
# checkpoint_results = checkpoint.run()


# Write file
container = 'silver'
database = 'prize_picks_silver'
table = 'dim_team'
file_format = 'delta'
merge_condition = 'tgt.team_key == src.team_key'

if checkpoint_results.success:
    spark.sql("DROP TABLE IF EXISTS prize_picks_silver.dim_team;")
    merge_data(teams_final_df, container, database, table,
               file_format, merge_condition=merge_condition)
