import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## @type: DataSource
## @args: [database = "int12102_sh", table_name = "customers"]
## @return: customers
## @inputs: []
customers = glueContext.create_dynamic_frame.from_catalog(database = "int12102_sh", table_name = "customers")

## @type: SelectFields
## @args: [paths = ['cust_id', 'cust_first_name','cust_last_name']]
## @return: customers_fields
## @inputs: [frame = customers]
customers_fields = SelectFields.apply(frame = customers, paths = ['cust_id','cust_first_name','cust_last_name'])

## @type: DataSource
## @args: [database = "int12102_sh", table_name = "supplementary_demographics"]
## @return: demo
## @inputs: []
demo = glueContext.create_dynamic_frame.from_catalog(database = "int12102_sh", table_name = "supplementary_demographics")

## @type: SelectFields
## @args: [paths = ['cust_id', 'education','occupation']]
## @return: demo_fields
## @inputs: [frame = demo]
demo_fields = SelectFields.apply(frame = demo, paths = ['cust_id','education','occupation']).rename_field('cust_id','demo_cust_id')

## @type: DataSource
## @args: [database = "int12102_sh", table_name = "sales"]
## @return: sales
## @inputs: []
sales = glueContext.create_dynamic_frame.from_catalog(database = "int12102_sh", table_name = "sales")

## @type: SelectFields
## @args: [paths = ['cust_id', 'amount_sold']]
## @return: sales_fields
## @inputs: [frame = sales]
sales_fields = SelectFields.apply(frame = sales, paths = ['cust_id','amount_sold']).rename_field('cust_id','sales_cust_id')

## @type: Join
## @args: [keys1 = ['cust_id'], keys2 = ['demo_cust_id']]
## @return: cust_demo
## @inputs: [frame1 = customers_fields, frame2 = demo_fields]
cust_demo = Join.apply(frame1 = customers_fields, frame2 = demo_fields, keys1 = ['cust_id'], keys2 = ['demo_cust_id']).drop_fields(['demo_cust_id'])

## @type: Join
## @args: [keys1 = ['sales_cust_id'], keys2 = ['cust_id']]
## @return: cust_demo_sales
## @inputs: [frame1 = sales_fields, frame2 = cust_demo]
cust_demo_sales = Join.apply(sales_fields, cust_demo, 'sales_cust_id', 'cust_id').drop_fields(['sales_cust_id'])

cust_demo_sales_t = cust_demo_sales.toDF()
cust_demo_sales_t.createOrReplaceTempView("cust_demo_sales_tbl")
cust_demo_sales_sql = spark.sql("SELECT concat(cust_last_name, ', ', cust_first_name) cust_name, education, occupation, amount_sold, avg(amount_sold) OVER (partition by education) avg_by_education, avg(amount_sold) OVER (partition by occupation) avg_by_occupation, avg(amount_sold) OVER () avg_total FROM cust_demo_sales_tbl")
cust_demo_sales_final = DynamicFrame.fromDF(cust_demo_sales_sql, glueContext, "cust_demo_sales_final")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://gluent.backup/user/gluent/mrainey/glue-target/sales_cust_demo"}, format = "parquet"]
## @return: cust_demo_sales_target
## @inputs: [frame = cust_demo_sales_final]
cust_demo_sales_target = glueContext.write_dynamic_frame.from_options(frame = cust_demo_sales_final, connection_type = "s3", connection_options = {"path": "s3://gluent.backup/user/gluent/mrainey/glue-target/sales_cust_demo"}, format = "parquet")

job.commit()
