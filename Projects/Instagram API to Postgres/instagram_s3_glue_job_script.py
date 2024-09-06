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

bucket_paths = args['BUCKET_PATH']
account_bucket_path, content_bucket_path, demographics_bucket_path, calendar_bucket_path = bucket_paths.split(',')

# Script generated for node IG Account S3
IGAccountS3_node = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": [account_bucket_path], "recurse": True}, transformation_ctx="IGAccountS3_node")

# Script generated for node IG Content S3
IGContentS3_node = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": [content_bucket_path], "recurse": True}, transformation_ctx="IGContentS3_node")

 Script generated for node IG Demographics S3
IGDemographicsS3_node = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": [demographics_bucket_path], "recurse": True}, transformation_ctx="IGDemographicsS3_node")

# Script generated for node Calendar S3
CalendarS3_node = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": [calendar_bucket_path], "recurse": True}, transformation_ctx="CalendarS3_node")

# Script generated for node Tratar Account Data
TratarAccountData_node = ApplyMapping.apply(frame=IGAccountS3_node, mappings=[("followers_count", "string", "followers_count", "int"), ("1_name", "string", "1_name", "string"), ("1_values_1_value", "string", "1_values_1_value", "int"), ("timestamp", "string", "date", "timestamp"), ("2_name", "string", "2_name", "string"), ("2_values_1_value", "string", "2_values_1_value", "int"), ("3_name", "string", "3_name", "string"), ("3_values_1_value", "string", "3_values_1_value", "int")], transformation_ctx="TratarAccountData_node")

# Script generated for node Tratar Calendar
TratarCalendar_node = ApplyMapping.apply(frame=CalendarS3_node, mappings=[("date", "string", "date", "timestamp")], transformation_ctx="TratarCalendar_node")

# Converting to DataFrame
dataframe_calendar = TratarCalendar_node.toDF()

# Script generated for node Calendar PostgreSQL
cursor.execute("DELETE FROM public.tabela_calendar")

batch_data_calendar = dataframe_calendar.collect()

for row in batch_data_calendar:
    date = row['date']

    cursor.execute("""
        INSERT INTO public.tabela_calendar (
            date
        ) VALUES (%s)
    """, (date,))
    

# Converting to DataFrame
dataframe_account = TratarAccountData_node.toDF()

# Impressions 
dataframe_account = dataframe_account.withColumn(
    'impressions',
    when(col("1_name") == 'impressions', col("1_values_1_value"))
    .when(col("2_name") == 'impressions', col("2_values_1_value"))
    .when(col("3_name") == 'impressions', col("3_values_1_value"))
)

# Profile Views 
dataframe_account = dataframe_account.withColumn(
    'profile_views',
    when(col("1_name") == 'profile_views', col("1_values_1_value"))
    .when(col("2_name") == 'profile_views', col("2_values_1_value"))
    .when(col("3_name") == 'profile_views', col("3_values_1_value"))
)

# Reach 
dataframe_account = dataframe_account.withColumn(
    'reach',
    when(col("1_name") == 'reach', col("1_values_1_value"))
    .when(col("2_name") == 'reach', col("2_values_1_value"))
    .when(col("3_name") == 'reach', col("3_values_1_value"))
)

# Convert back to DynamicFrame if needed for further Glue transformations
dynamicframe_account = DynamicFrame.fromDF(dataframe_account, glueContext, "dynamicframe_account")


#ALTERED UPSERT SCRIPT THAT IGNORES NEW NULL VALUES, PRESERVING THE PAST NON NULL VALUES OF A ROW:
# Collect data for PostgreSQL insertion
batch_data_account = dataframe_account.collect()

for row in batch_data_account:
    transaction = row['date']
    cursor.execute("""SELECT * FROM public.tabela_instagram_account
        WHERE date = %s""", (transaction,))
    existing_record = cursor.fetchone()

    if existing_record:
        cursor.execute("""
            UPDATE public.tabela_instagram_account
            SET
                impressions = COALESCE(%s, impressions),
                profile_views = COALESCE(%s, profile_views),
                reach = COALESCE(%s, reach),
                followers_count = COALESCE(%s, followers_count)
            WHERE date = %s
            """,
            (
                row['impressions'],
                row['profile_views'],
                row['reach'],
                row['followers_count'],
                transaction
            )
        )
    else:
        cursor.execute("""
            INSERT INTO public.tabela_instagram_account (
                date,
                impressions,
                profile_views,
                reach,
                followers_count
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (
                transaction,
                row['impressions'],
                row['profile_views'],
                row['reach'],
                row['followers_count']
            )
        )


# Script generated for node Tratar Content Data
TratarContentData_node = ApplyMapping.apply(frame=IGContentS3_node, mappings=[("timestamp", "string", "date", "timestamp"), ("content_type", "string", "content_type", "string"), ("media_type", "string", "media_type", "string"), ("permalink", "string", "permalink", "string"), ("thumbnail_url", "string", "thumbnail_url", "string"), ("media_url", "string", "media_url", "string"), ("id", "string", "id", "string"), ("caption", "string", "caption", "string"), ("1_name", "string", "1_name", "string"), ("1_values_1_value", "string", "1_values_1_value", "int"), ("2_name", "string", "2_name", "string"), ("2_values_1_value", "string", "2_values_1_value", "int"), ("3_name", "string", "3_name", "string"), ("3_values_1_value", "string", "3_values_1_value", "int"), ("4_name", "string", "4_name", "string"), ("4_values_1_value", "string", "4_values_1_value", "int"), ("5_name", "string", "5_name", "string"), ("5_values_1_value", "string", "5_values_1_value", "int"), ("6_name", "string", "6_name", "string"), ("6_values_1_value", "string", "6_values_1_value", "int"), ("7_name", "string", "7_name", "string"), ("7_values_1_value", "string", "7_values_1_value", "int"), ("unique_identifier", "string", "unique_identifier", "string")], transformation_ctx="TratarContentData_node")

# Converting to DataFrame
dataframe_content = TratarContentData_node.toDF()

# Impressions 
dataframe_content = dataframe_content.withColumn(
    'impressions',
    when(col("1_name") == 'impressions', col("1_values_1_value"))
    .when(col("2_name") == 'impressions', col("2_values_1_value"))
    .when(col("3_name") == 'impressions', col("3_values_1_value"))
    .when(col("4_name") == 'impressions', col("4_values_1_value"))
    .when(col("5_name") == 'impressions', col("5_values_1_value"))
    .when(col("6_name") == 'impressions', col("6_values_1_value"))
    .when(col("7_name") == 'impressions', col("7_values_1_value"))
)

# Video Views 
dataframe_content = dataframe_content.withColumn(
    'video_views',
    when(col("1_name") == 'video_views', col("1_values_1_value"))
    .when(col("2_name") == 'video_views', col("2_values_1_value"))
    .when(col("3_name") == 'video_views', col("3_values_1_value"))
    .when(col("4_name") == 'video_views', col("4_values_1_value"))
    .when(col("5_name") == 'video_views', col("5_values_1_value"))
    .when(col("6_name") == 'video_views', col("6_values_1_value"))
    .when(col("7_name") == 'video_views', col("7_values_1_value"))
)

# Reach 
dataframe_content = dataframe_content.withColumn(
    'reach',
    when(col("1_name") == 'reach', col("1_values_1_value"))
    .when(col("2_name") == 'reach', col("2_values_1_value"))
    .when(col("3_name") == 'reach', col("3_values_1_value"))
    .when(col("4_name") == 'reach', col("4_values_1_value"))
    .when(col("5_name") == 'reach', col("5_values_1_value"))
    .when(col("6_name") == 'reach', col("6_values_1_value"))
    .when(col("7_name") == 'reach', col("7_values_1_value"))
)

# Shares 
dataframe_content = dataframe_content.withColumn(
    'shares',
    when(col("1_name") == 'shares', col("1_values_1_value"))
    .when(col("2_name") == 'shares', col("2_values_1_value"))
    .when(col("3_name") == 'shares', col("3_values_1_value"))
    .when(col("4_name") == 'shares', col("4_values_1_value"))
    .when(col("5_name") == 'shares', col("5_values_1_value"))
    .when(col("6_name") == 'shares', col("6_values_1_value"))
    .when(col("7_name") == 'shares', col("7_values_1_value"))
)

# Saved 
dataframe_content = dataframe_content.withColumn(
    'saved',
    when(col("1_name") == 'saved', col("1_values_1_value"))
    .when(col("2_name") == 'saved', col("2_values_1_value"))
    .when(col("3_name") == 'saved', col("3_values_1_value"))
    .when(col("4_name") == 'saved', col("4_values_1_value"))
    .when(col("5_name") == 'saved', col("5_values_1_value"))
    .when(col("6_name") == 'saved', col("6_values_1_value"))
    .when(col("7_name") == 'saved', col("7_values_1_value"))
)

# Comments 
dataframe_content = dataframe_content.withColumn(
    'comments',
    when(col("1_name") == 'comments', col("1_values_1_value"))
    .when(col("2_name") == 'comments', col("2_values_1_value"))
    .when(col("3_name") == 'comments', col("3_values_1_value"))
    .when(col("4_name") == 'comments', col("4_values_1_value"))
    .when(col("5_name") == 'comments', col("5_values_1_value"))
    .when(col("6_name") == 'comments', col("6_values_1_value"))
    .when(col("7_name") == 'comments', col("7_values_1_value"))
)

# Likes 
dataframe_content = dataframe_content.withColumn(
    'likes',
    when(col("1_name") == 'likes', col("1_values_1_value"))
    .when(col("2_name") == 'likes', col("2_values_1_value"))
    .when(col("3_name") == 'likes', col("3_values_1_value"))
    .when(col("4_name") == 'likes', col("4_values_1_value"))
    .when(col("5_name") == 'likes', col("5_values_1_value"))
    .when(col("6_name") == 'likes', col("6_values_1_value"))
    .when(col("7_name") == 'likes', col("7_values_1_value"))
)

# Image URL 
dataframe_content = dataframe_content.withColumn(
    'image_url',
    when(col("thumbnail_url") == '', col("media_url"))
    .otherwise(col("thumbnail_url"))
)


# Convert back to DynamicFrame if needed for further Glue transformations
dynamicframe_content = DynamicFrame.fromDF(dataframe_content, glueContext, "dynamicframe_content")

# Collect data for PostgreSQL insertion
batch_data_content = dataframe_content.collect()

for row in batch_data_content:
    transaction = row['unique_identifier']
    cursor.execute("""SELECT * FROM public.tabela_instagram_content
        WHERE unique_identifier = %s""", (transaction,))
    existing_record = cursor.fetchone()

    if existing_record:
        cursor.execute("""
            UPDATE public.tabela_instagram_content
            SET
                date = %s,
                content_type = %s,
                media_type = %s,
                permalink = %s,
                image_url = %s,
                thumbnail_url = %s,
                media_url = %s,
                id = %s,
                caption = %s,
                impressions = %s,
                reach = %s,
                shares = %s,
                saved = %s,
                comments = %s,
                video_views = %s,
                likes = %s
            WHERE unique_identifier = %s
            """,
            (
                row['date'],
                row['content_type'],
                row['media_type'],
                row['permalink'],
                row['image_url'],
                row['thumbnail_url'],
                row['media_url'],
                row['id'],
                row['caption'],
                row['impressions'],
                row['reach'],
                row['shares'],
                row['saved'],
                row['comments'],
                row['video_views'],
                row['likes'],
                transaction
            )
        )
    else:
        cursor.execute("""
            INSERT INTO public.tabela_instagram_content (
                unique_identifier,
                date,
                content_type,
                media_type,
                permalink,
                image_url,
                thumbnail_url,
                media_url,
                id,
                caption,
                impressions,
                reach,
                shares,
                saved,
                comments,
                video_views,
                likes
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                transaction,
                row['date'],
                row['content_type'],
                row['media_type'],
                row['permalink'],
                row['image_url'],
                row['thumbnail_url'],
                row['media_url'],
                row['id'],
                row['caption'],
                row['impressions'],
                row['reach'],
                row['shares'],
                row['saved'],
                row['comments'],
                row['video_views'],
                row['likes']
            )
        )
        
# Script generated for node Tratar Demographics Data
TratarDemographicsData_node = ApplyMapping.apply(frame=IGDemographicsS3_node, mappings=[("country", "string", "country", "string"), ("country_value", "string", "country_value", "int"), ("city", "string", "city", "string"), ("city_value", "string", "city_value", "int"), ("state", "string", "state", "string"), ("state_value", "string", "state_value", "int"), ("gender", "string", "gender", "string"), ("gender_value", "string", "gender_value", "int"), ("age", "string", "age", "string"), ("age_value", "string", "age_value", "int"), ("timestamp", "string", "date", "timestamp"), ("unique_identifier", "string", "unique_identifier", "string")], transformation_ctx="TratarDemographicsData_node")

# Converting to DataFrame
dataframe_demographics = TratarDemographicsData_node.toDF()

# Collect data for PostgreSQL insertion
batch_data_demographics = dataframe_demographics.collect()

for row in batch_data_demographics:
    transaction = row['unique_identifier']
    cursor.execute("""SELECT * FROM public.tabela_instagram_demographics
        WHERE unique_identifier = %s""", (transaction,))
    existing_record = cursor.fetchone()

    if existing_record:
        cursor.execute("""
            UPDATE public.tabela_instagram_demographics
            SET
                country = %s,
                country_value = %s,
                city = %s,
                city_value = %s,
                gender = %s,
                gender_value = %s,
                age = %s,
                age_value = %s,
                date = %s,
                state = %s,
                state_value = %s
            WHERE unique_identifier = %s
            """,
            (
                row['country'],
                row['country_value'],
                row['city'],
                row['city_value'],
                row['gender'],
                row['gender_value'],
                row['age'],
                row['age_value'],
                row['date'],
                row['state'],
                row['state_value'],
                transaction
            )
        )
    else:
        cursor.execute("""
            INSERT INTO public.tabela_instagram_demographics (
                unique_identifier,
                country,
                country_value,
                city,
                city_value,
                gender,
                gender_value,
                age,
                age_value,
                date,
                state,
                state_value
            ) VALUES (

%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            """,
            (
                transaction,
                row['country'],
                row['country_value'],
                row['city'],
                row['city_value'],
                row['gender'],
                row['gender_value'],
                row['age'],
                row['age_value'],
                row['date'],
                row['state'],
                row['state_value']
            )
        )
        

conn.commit()
cursor.close()
conn.close()
job.commit()
