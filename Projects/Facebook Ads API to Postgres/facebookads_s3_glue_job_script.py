import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

import pg8000
from pyspark.sql.functions import col, when

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'HOST', 'DATABASE', 'USER', 'PASSWORD', 'BUCKET_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Defining PostgreSQL connection parameters:
conn = pg8000.connect(
    host=args['HOST'],
    database=args['DATABASE'],
    user=args['USER'],
    password=args['PASSWORD']
)
cursor = conn.cursor()
bucket_path = args['BUCKET_PATH']

# Script generated for node Relevant Data S3
RelevantDataS3_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [bucket_path], "recurse": True},
    transformation_ctx="RelevantDataS3_node"
)

# Script generated for node Tratar Relevant Data
TratarRelevantData_node = ApplyMapping.apply(
    frame=RelevantDataS3_node,
    mappings=[
        ("date_start", "string", "date_start", "timestamp"),
        ("date_stop", "string", "date_stop", "timestamp"),
        ("ad_name", "string", "ad_name", "string"),
        ("adset_name", "string", "adset_name", "string"),
        ("campaign_name", "string", "campaign_name", "string"),
        ("actions_1_action_type", "string", "actions_1_action_type", "string"),
        ("actions_1_value", "string", "actions_1_value", "int"),
        ("actions_2_action_type", "string", "actions_2_action_type", "string"),
        ("actions_2_value", "string", "actions_2_value", "int"),
        ("actions_3_action_type", "string", "actions_3_action_type", "string"),
        ("actions_3_value", "string", "actions_3_value", "int"),
        ("actions_4_action_type", "string", "actions_4_action_type", "string"),
        ("actions_4_value", "string", "actions_4_value", "int"),
        ("clicks", "string", "clicks", "int"),
        ("cost_per_action_type_1_action_type", "string", "cost_per_action_type_1_action_type", "string"),
        ("cost_per_action_type_1_value", "string", "cost_per_action_type_1_value", "float"),
        ("cost_per_action_type_2_action_type", "string", "cost_per_action_type_2_action_type", "string"),
        ("cost_per_action_type_2_value", "string", "cost_per_action_type_2_value", "float"),
        ("cost_per_action_type_3_action_type", "string", "cost_per_action_type_3_action_type", "string"),
        ("cost_per_action_type_3_value", "string", "cost_per_action_type_3_value", "float"),
        ("cost_per_action_type_4_action_type", "string", "cost_per_action_type_4_action_type", "string"),
        ("cost_per_action_type_4_value", "string", "cost_per_action_type_4_value", "float"),
        ("cpc", "string", "cpc", "float"),
        ("cpm", "string", "cpm", "float"),
        ("cpp", "string", "cpp", "float"),
        ("ctr", "string", "ctr", "float"),
        ("frequency", "string", "frequency", "float"),
        ("full_view_impressions", "string", "full_view_impressions", "int"),
        ("full_view_reach", "string", "full_view_reach", "int"),
        ("impressions", "string", "impressions", "int"),
        ("purchase_roas_1_action_type", "string", "purchase_roas_1_action_type", "string"),
        ("purchase_roas_1_value", "string", "purchase_roas_1_value", "float"),
        ("reach", "string", "reach", "int"),
        ("social_spend", "string", "social_spend", "float"),
        ("spend", "string", "spend", "float"),
        ("unique_identifier", "string", "unique_identifier", "string")
    ],
    transformation_ctx="TratarRelevantData_node"
)

# Converting to DataFrame
dataframe_relevant_data = TratarRelevantData_node.toDF()

# Leads
dataframe_relevant_data = dataframe_relevant_data.withColumn(
    'leads',
    when(col("actions_1_action_type") == 'lead', col("actions_1_value"))
    .when(col("actions_2_action_type") == 'lead', col("actions_2_value"))
    .when(col("actions_3_action_type") == 'lead', col("actions_3_value"))
    .when(col("actions_4_action_type") == 'lead', col("actions_4_value"))
    .otherwise(0)
)

# Landing Page Views
dataframe_relevant_data = dataframe_relevant_data.withColumn(
    'landing_page_views',
    when(col("actions_1_action_type") == 'landing_page_view', col("actions_1_value"))
    .when(col("actions_2_action_type") == 'landing_page_view', col("actions_2_value"))
    .when(col("actions_3_action_type") == 'landing_page_view', col("actions_3_value"))
    .when(col("actions_4_action_type") == 'landing_page_view', col("actions_4_value"))
    .otherwise(0)
)

# Link Clicks
dataframe_relevant_data = dataframe_relevant_data.withColumn(
    'link_clicks',
    when(col("actions_1_action_type") == 'link_click', col("actions_1_value"))
    .when(col("actions_2_action_type") == 'link_click', col("actions_2_value"))
    .when(col("actions_3_action_type") == 'link_click', col("actions_3_value"))
    .when(col("actions_4_action_type") == 'link_click', col("actions_4_value"))  
    .otherwise(0)
)

# Cost Per Leads
dataframe_relevant_data = dataframe_relevant_data.withColumn(
    'cost_per_leads',
    when(col("cost_per_action_type_1_action_type") == 'lead', col("cost_per_action_type_1_value"))
    .when(col("cost_per_action_type_2_action_type") == 'lead', col("cost_per_action_type_2_value"))
    .when(col("cost_per_action_type_3_action_type") == 'lead', col("cost_per_action_type_3_value"))
    .when(col("cost_per_action_type_4_action_type") == 'lead', col("cost_per_action_type_4_value"))
    .otherwise(0)
)

# Cost Per Landing Page Views
dataframe_relevant_data = dataframe_relevant_data.withColumn(
    'cost_per_landing_page_views',
    when(col("cost_per_action_type_1_action_type") == 'landing_page_view', col("cost_per_action_type_1_value"))
    .when(col("cost_per_action_type_2_action_type") == 'landing_page_view', col("cost_per_action_type_2_value"))
    .when(col("cost_per_action_type_3_action_type") == 'landing_page_view', col("cost_per_action_type_3_value"))
    .when(col("cost_per_action_type_4_action_type") == 'landing_page_view', col("cost_per_action_type_4_value"))
    .otherwise(0)
)

# Cost Per Link Clicks
dataframe_relevant_data = dataframe_relevant_data.withColumn(
    'cost_per_link_clicks',
    when(col("cost_per_action_type_1_action_type") == 'link_click', col("cost_per_action_type_1_value"))
    .when(col("cost_per_action_type_2_action_type") == 'link_click', col("cost_per_action_type_2_value"))
    .when(col("cost_per_action_type_3_action_type") == 'link_click', col("cost_per_action_type_3_value"))
    .when(col("cost_per_action_type_4_action_type") == 'link_click', col("cost_per_action_type_4_value"))
    .otherwise(0)
)

# Purchase Roas
dataframe_relevant_data = dataframe_relevant_data.withColumn(
    'purchase_roas',
    when(col("purchase_roas_1_action_type") == 'omni_purchase', col("purchase_roas_1_value"))
    .otherwise(0)
)

# Cost Per Purchase
dataframe_relevant_data = dataframe_relevant_data.withColumn(
    'cost_per_purchase',
    when(col("cost_per_action_type_1_action_type") == 'omni_purchase', col("cost_per_action_type_1_value"))
    .when(col("cost_per_action_type_2_action_type") == 'omni_purchase', col("cost_per_action_type_2_value"))
    .when(col("cost_per_action_type_3_action_type") == 'omni_purchase', col("cost_per_action_type_3_value"))
    .when(col("cost_per_action_type_4_action_type") == 'omni_purchase', col("cost_per_action_type_4_value"))
    .otherwise(0)
)

# Purchases
dataframe_relevant_data = dataframe_relevant_data.withColumn(
    'purchases',
    when(col("actions_1_action_type") == 'omni_purchase', col("actions_1_value"))
    .when(col("actions_2_action_type") == 'omni_purchase', col("actions_2_value"))
    .when(col("actions_3_action_type") == 'omni_purchase', col("actions_3_value"))
    .when(col("actions_4_action_type") == 'omni_purchase', col("actions_4_value"))
    .otherwise(0)
)

# Drop unnecessary columns
columns_to_drop = [
    "actions_1_action_type", "actions_2_action_type", "actions_3_action_type", "actions_4_action_type",
    "actions_1_value", "actions_2_value", "actions_3_value", "actions_4_value",
    "cost_per_action_type_1_action_type", "cost_per_action_type_2_action_type",
    "cost_per_action_type_3_action_type", "cost_per_action_type_4_action_type", 
    "cost_per_action_type_1_value", "cost_per_action_type_2_value",
    "cost_per_action_type_3_value", "cost_per_action_type_4_value"
]

dataframe_relevant_data = dataframe_relevant_data.drop(*columns_to_drop)

# Convert back to DynamicFrame if needed for further Glue transformations
dynamicframe_relevant_data = DynamicFrame.fromDF(dataframe_relevant_data, glueContext, "dynamicframe_relevant_data")

# Collect data for PostgreSQL insertion
batch_data_relevant_data = dataframe_relevant_data.collect()

for row in batch_data_relevant_data:
    transaction = row['unique_identifier']
    cursor.execute("""SELECT * FROM public.tabela_facebook_ads_relevant_data
        WHERE unique_identifier = %s""", (transaction,))
    existing_record = cursor.fetchone()

    if existing_record:
        cursor.execute("""
            UPDATE public.tabela_facebook_ads_relevant_data
            SET
                date_start = %s,
                date_stop = %s,
                ad_name = %s,
                adset_name = %s,
                campaign_name = %s,
                leads = %s,
                cost_per_leads = %s,
                landing_page_views = %s,
                cost_per_landing_page_views = %s,
                link_clicks = %s,
                cost_per_link_clicks = %s,
                clicks = %s,
                cpc = %s,
                cpm = %s,
                cpp = %s,
                ctr = %s,
                frequency = %s,
                full_view_impressions = %s,
                full_view_reach = %s,
                impressions = %s,
                reach = %s,
                social_spend = %s,
                spend = %s,
                purchases = %s,
                cost_per_purchase = %s,
                purchase_roas = %s
            WHERE unique_identifier = %s
            """,
            (
                row['date_start'],
                row['date_stop'],
                row['ad_name'],
                row['adset_name'],
                row['campaign_name'],
                row['leads'],
                row['cost_per_leads'],
                row['landing_page_views'],
                row['cost_per_landing_page_views'],
                row['link_clicks'],
                row['cost_per_link_clicks'],
                row['clicks'],
                row['cpc'],
                row['cpm'],
                row['cpp'],
                row['ctr'],
                row['frequency'],
                row['full_view_impressions'],
                row['full_view_reach'],
                row['impressions'],
                row['reach'],
                row['social_spend'],
                row['spend'],
                row['purchases'],
                row['cost_per_purchase'],
                row['purchase_roas'],
                transaction
            )
        )
    else:
        cursor.execute("""
            INSERT INTO public.tabela_facebook_ads_relevant_data (
                unique_identifier,
                date_start,
                date_stop,
                ad_name,
                adset_name,
                campaign_name,
                leads,
                cost_per_leads,
                landing_page_views,
                cost_per_landing_page_views,
                link_clicks,
                cost_per_link_clicks,
                clicks,
                cpc,
                cpm,
                cpp,
                ctr,
                frequency,
                full_view_impressions,
                full_view_reach,
                impressions,
                reach,
                social_spend,
                spend,
                purchases,
                cost_per_purchase,
                purchase_roas
            ) VALUES (

%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            """,
            (
                transaction,
                row['date_start'],
                row['date_stop'],
                row['ad_name'],
                row['adset_name'],
                row['campaign_name'],
                row['leads'],
                row['cost_per_leads'],
                row['landing_page_views'],
                row['cost_per_landing_page_views'],
                row['link_clicks'],
                row['cost_per_link_clicks'],
                row['clicks'],
                row['cpc'],
                row['cpm'],
                row['cpp'],
                row['ctr'],
                row['frequency'],
                row['full_view_impressions'],
                row['full_view_reach'],
                row['impressions'],
                row['reach'],
                row['social_spend'],
                row['spend'],
                row['purchases'],
                row['cost_per_purchase'],
                row['purchase_roas']
            )
        )
            
        
conn.commit()
cursor.close()
conn.close()
job.commit()
