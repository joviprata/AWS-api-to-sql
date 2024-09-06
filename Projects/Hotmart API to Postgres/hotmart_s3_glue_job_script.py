import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_to_timestamp
from awsglue.dynamicframe import DynamicFrame
import gs_derived

#from pyspark.sql import SQLContext
import pg8000
from pyspark.sql.functions import col, when

args = getResolvedOptions(sys.argv, ['JOB_NAME',
'HOST', 'DATABASE', 'USER', 'PASSWORD', 'BUCKET_PATHS', 'COMMISSIONS_MAIN_EMAIL'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Defining PostgreSQL connection parameters:

conn = pg8000.connect(
    host = args['HOST'],
    database = args['DATABASE'],
    user = args['USER'],
    password = args['PASSWORD']
)
cursor = conn.cursor()

commissions_main_email = args['COMMISSIONS_MAIN_EMAIL']
bucket_paths = args['BUCKET_PATHS']
price_details_bucket_path, commissions_bucket_path, summary_bucket_path, history_bucket_path, users_bucket_path, relevant_data_bucket_path = bucket_paths.split(',')

# Script generated for node Price Details S3
PriceDetailsS3_node = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": [price_details_bucket_path]}, transformation_ctx="PriceDetailsS3_node")

# Script generated for node Commissions S3
CommissionsS3_node = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": [commissions_bucket_path]}, transformation_ctx="CommissionsS3_node")

# Script generated for node Summary S3
SummaryS3_node = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": [summary_bucket_path]}, transformation_ctx="SummaryS3_node")

# Script generated for node History S3
HistoryS3_node = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": [history_bucket_path]}, transformation_ctx="HistoryS3_node")

# Script generated for node Users S3
UsersS3_node = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": [users_bucket_path]}, transformation_ctx="UsersS3_node")

# Script generated for node Tratar Commissions
TratarCommissions_node = ApplyMapping.apply(frame=CommissionsS3_node, mappings=[("commissions_1_commission_value", "string", "commissions_1_commission_value", "float"), ("commissions_1_commission_currency_code", "string", "commissions_1_commission_currency_code", "string"), ("commissions_1_source", "string", "commissions_1_source", "string"), ("commissions_1_user_email", "string", "commissions_1_user_email", "string"), ("commissions_1_user_name", "string", "commissions_1_user_name", "string"), ("commissions_1_user_ucode", "string", "commissions_1_user_ucode", "string"), ("commissions_2_commission_value", "string", "commissions_2_commission_value", "float"), ("commissions_2_source", "string", "commissions_2_source", "string"), ("commissions_2_user_email", "string", "commissions_2_user_email", "string"), ("commissions_2_user_name", "string", "commissions_2_user_name", "string"), ("commissions_2_user_ucode", "string", "commissions_2_user_ucode", "string"), ("commissions_3_commission_value", "string", "commissions_3_commission_value", "float"), ("commissions_3_commission_currency_code", "string", "commissions_3_commission_currency_code", "string"), ("commissions_3_source", "string", "commissions_3_source", "string"), ("commissions_3_user_email", "string", "commissions_3_user_email", "string"), ("commissions_3_user_name", "string", "commissions_3_user_name", "string"), ("commissions_3_user_ucode", "string", "commissions_3_user_ucode", "string"), ("exchange_rate_currency_payout", "string", "exchange_rate_currency_payout", "float"), ("product_name", "string", "product_name", "string"), ("product_id", "string", "product_id", "string"), ("transaction", "string", "transaction", "string"), ("commissions_2_commission_currency_code", "string", "commissions_2_commission_currency_code", "string"), ("commissions_4_commission_value", "string", "commissions_4_commission_value", "float"), ("commissions_4_commission_currency_code", "string", "commissions_4_commission_currency_code", "string"), ("commissions_4_source", "string", "commissions_4_source", "string"), ("commissions_4_user_name", "string", "commissions_4_user_name", "string"), ("commissions_4_user_ucode", "string", "commissions_4_user_ucode", "string"), ("commissions_4_user_email", "string", "commissions_4_user_email", "string"), ("commissions_5_commission_value", "string", "commissions_5_commission_value", "float"), ("commissions_5_commission_currency_code", "string", "commissions_5_commission_currency_code", "string"), ("commissions_5_source", "string", "commissions_5_source", "string"), ("commissions_5_user_name", "string", "commissions_5_user_name", "string"), ("commissions_5_user_ucode", "string", "commissions_5_user_ucode", "string"), ("commissions_5_user_email", "string", "commissions_5_user_email", "string"), ("commissions_6_commission_value", "string", "commissions_6_commission_value", "float"), ("commissions_6_commission_currency_code", "string", "commissions_6_commission_currency_code", "string"), ("commissions_6_source", "string", "commissions_6_source", "string"), ("commissions_6_user_name", "string", "commissions_6_user_name", "string"), ("commissions_6_user_ucode", "string", "commissions_6_user_ucode", "string"), ("commissions_6_user_email", "string", "commissions_6_user_email", "string"), ("commissions_7_commission_value", "string", "commissions_7_commission_value", "float"), ("commissions_7_commission_currency_code", "string", "commissions_7_commission_currency_code", "string"), ("commissions_7_source", "string", "commissions_7_source", "string"), ("commissions_7_user_name", "string", "commissions_7_user_name", "string"), ("commissions_7_user_ucode", "string", "commissions_7_user_ucode", "string"), ("commissions_7_user_email", "string", "commissions_7_user_email", "string"), ("commissions_8_commission_value", "string", "commissions_8_commission_value", "float"), ("commissions_8_commission_currency_code", "string", "commissions_8_commission_currency_code", "string"), ("commissions_8_source", "string", "commissions_8_source", "string"), ("commissions_8_user_name", "string", "commissions_8_user_name", "string"), ("commissions_8_user_ucode", "string", "commissions_8_user_ucode", "string"), ("commissions_8_user_email", "string", "commissions_8_user_email", "string"), ("commissions_9_commission_value", "string", "commissions_9_commission_value", "float"), ("commissions_9_commission_currency_code", "string", "commissions_9_commission_currency_code", "string"), ("commissions_9_source", "string", "commissions_9_source", "string"), ("commissions_9_user_name", "string", "commissions_9_user_name", "string"), ("commissions_9_user_ucode", "string", "commissions_9_user_ucode", "string"), ("commissions_9_user_email", "string", "commissions_9_user_email", "string"), ("commissions_10_commission_value", "string", "commissions_10_commission_value", "float"), ("commissions_10_commission_currency_code", "string", "commissions_10_commission_currency_code", "string"), ("commissions_10_source", "string", "commissions_10_source", "string"), ("commissions_10_user_name", "string", "commissions_10_user_name", "string"), ("commissions_10_user_ucode", "string", "commissions_10_user_ucode", "string"), ("commissions_10_user_email", "string", "commissions_10_user_email", "string"), ("purchase_status", "string", "purchase_status", "string")], transformation_ctx="TratarCommissions_node")

# Script generated for node Tratar Summary
TratarSummary_node = ApplyMapping.apply(frame=SummaryS3_node, mappings=[("total_value_value", "string", "total_value_value", "float"), ("total_value_currency_code", "string", "total_value_currency_code", "string"), ("total_items", "string", "total_items", "int"), ("purchase_status", "string", "purchase_status", "string")], transformation_ctx="TratarSummary_node")

# Script generated for node Tratar Datas History
TratarDatasHistory_node = ApplyMapping.apply(frame=HistoryS3_node, mappings=[("purchase_is_subscription", "string", "purchase_is_subscription", "boolean"), ("purchase_warranty_expire_date", "string", "purchase_warranty_expire_date", "bigint"), ("purchase_approved_date", "string", "purchase_approved_date", "bigint"), ("purchase_tracking_source_sck", "string", "purchase_tracking_source_sck", "string"), ("purchase_tracking_source", "string", "purchase_tracking_source", "string"), ("purchase_tracking_external_code", "string", "purchase_tracking_external_code", "string"), ("purchase_recurrency_number", "string", "purchase_recurrency_number", "int"), ("purchase_offer_payment_mode", "string", "purchase_offer_payment_mode", "string"), ("purchase_offer_code", "string", "purchase_offer_code", "string"), ("purchase_commission_as", "string", "purchase_commission_as", "string"), ("purchase_order_date", "string", "purchase_order_date", "bigint"), ("purchase_price_value", "string", "purchase_price_value", "float"), ("purchase_price_currency_code", "string", "purchase_price_currency_code", "string"), ("purchase_status", "string", "purchase_status", "string"), ("purchase_payment_installments_number", "string", "purchase_payment_installments_number", "int"), ("purchase_payment_type", "string", "purchase_payment_type", "string"), ("purchase_payment_method", "string", "purchase_payment_method", "string"), ("purchase_hotmart_fee_percentage", "string", "purchase_hotmart_fee_percentage", "float"), ("purchase_hotmart_fee_currency_code", "string", "purchase_hotmart_fee_currency_code", "string"), ("purchase_hotmart_fee_total", "string", "purchase_hotmart_fee_total", "float"), ("purchase_hotmart_fee_base", "string", "purchase_hotmart_fee_base", "float"), ("purchase_hotmart_fee_fixed", "string", "purchase_hotmart_fee_fixed", "float"), ("purchase_transaction", "string", "purchase_transaction", "string"), ("producer_name", "string", "producer_name", "string"), ("producer_ucode", "string", "producer_ucode", "string"), ("product_id", "string", "product_id", "string"), ("product_name", "string", "product_name", "string"), ("buyer_name", "string", "buyer_name", "string"), ("buyer_ucode", "string", "buyer_ucode", "string"), ("buyer_email", "string", "buyer_email", "string")], transformation_ctx="TratarDatasHistory_node")

# Script generated for node Tratar Users
TratarUsers_node = ApplyMapping.apply(frame=UsersS3_node, mappings=[("product_name", "string", "product_name", "string"), ("product_id", "string", "product_id", "string"), ("transaction", "string", "transaction", "string"), ("users_1_role", "string", "users_1_role", "string"), ("users_1_user_ucode", "string", "users_1_user_ucode", "string"), ("users_1_user_locale", "string", "users_1_user_locale", "string"), ("users_1_user_name", "string", "users_1_user_name", "string"), ("users_1_user_trade_name", "string", "users_1_user_trade_name", "string"), ("users_1_user_cellphone", "string", "users_1_user_cellphone", "string"), ("users_1_user_phone", "string", "users_1_user_phone", "string"), ("users_1_user_email", "string", "users_1_user_email", "string"), ("users_1_user_documents_1_value", "string", "users_1_user_documents_1_value", "string"), ("users_1_user_documents_1_type", "string", "users_1_user_documents_1_type", "string"), ("users_1_user_documents_2_value", "string", "users_1_user_documents_2_value", "string"), ("users_1_user_documents_2_type", "string", "users_1_user_documents_2_type", "string"), ("users_1_user_address_city", "string", "users_1_user_address_city", "string"), ("users_1_user_address_state", "string", "users_1_user_address_state", "string"), ("users_1_user_address_country", "string", "users_1_user_address_country", "string"), ("users_1_user_address_zip_code", "string", "users_1_user_address_zip_code", "string"), ("users_1_user_address_address", "string", "users_1_user_address_address", "string"), ("users_1_user_address_complement", "string", "users_1_user_address_complement", "string"), ("users_1_user_address_neighborhood", "string", "users_1_user_address_neighborhood", "string"), ("users_1_user_address_number", "string", "users_1_user_address_number", "string"), ("users_2_role", "string", "users_2_role", "string"), ("users_2_user_ucode", "string", "users_2_user_ucode", "string"), ("users_2_user_locale", "string", "users_2_user_locale", "string"), ("users_2_user_name", "string", "users_2_user_name", "string"), ("users_2_user_trade_name", "string", "users_2_user_trade_name", "string"), ("users_2_user_cellphone", "string", "users_2_user_cellphone", "string"), ("users_2_user_phone", "string", "users_2_user_phone", "string"), ("users_2_user_email", "string", "users_2_user_email", "string"), ("users_2_user_documents_1_value", "string", "users_2_user_documents_1_value", "string"), ("users_2_user_documents_1_type", "string", "users_2_user_documents_1_type", "string"), ("users_2_user_documents_2_value", "string", "users_2_user_documents_2_value", "string"), ("users_2_user_documents_2_type", "string", "users_2_user_documents_2_type", "string"), ("users_2_user_address_city", "string", "users_2_user_address_city", "string"), ("users_2_user_address_state", "string", "users_2_user_address_state", "string"), ("users_2_user_address_country", "string", "users_2_user_address_country", "string"), ("users_2_user_address_zip_code", "string", "users_2_user_address_zip_code", "string"), ("users_2_user_address_address", "string", "users_2_user_address_address", "string"), ("users_2_user_address_complement", "string", "users_2_user_address_complement", "string"), ("users_2_user_address_neighborhood", "string", "users_2_user_address_neighborhood", "string"), ("users_2_user_address_number", "string", "users_2_user_address_number", "string"), ("users_3_role", "string", "users_3_role", "string"), ("users_3_user_ucode", "string", "users_3_user_ucode", "string"), ("users_3_user_locale", "string", "users_3_user_locale", "string"), ("users_3_user_name", "string", "users_3_user_name", "string"), ("users_3_user_trade_name", "string", "users_3_user_trade_name", "string"), ("users_3_user_cellphone", "string", "users_3_user_cellphone", "string"), ("users_3_user_phone", "string", "users_3_user_phone", "string"), ("users_3_user_email", "string", "users_3_user_email", "string"), ("users_3_user_documents_1_value", "string", "users_3_user_documents_1_value", "string"), ("users_3_user_documents_1_type", "string", "users_3_user_documents_1_type", "string"), ("users_3_user_documents_2_value", "string", "users_3_user_documents_2_value", "string"), ("users_3_user_documents_2_type", "string", "users_3_user_documents_2_type", "string"), ("users_3_user_address_city", "string", "users_3_user_address_city", "string"), ("users_3_user_address_state", "string", "users_3_user_address_state", "string"), ("users_3_user_address_country", "string", "users_3_user_address_country", "string"), ("users_3_user_address_zip_code", "string", "users_3_user_address_zip_code", "string"), ("users_3_user_address_address", "string", "users_3_user_address_address", "string"), ("users_3_user_address_complement", "string", "users_3_user_address_complement", "string"), ("users_3_user_address_neighborhood", "string", "users_3_user_address_neighborhood", "string"), ("users_3_user_address_number", "string", "users_3_user_address_number", "string"), ("users_4_role", "string", "users_4_role", "string"), ("users_4_user_ucode", "string", "users_4_user_ucode", "string"), ("users_4_user_locale", "string", "users_4_user_locale", "string"), ("users_4_user_name", "string", "users_4_user_name", "string"), ("users_4_user_trade_name", "string", "users_4_user_trade_name", "string"), ("users_4_user_cellphone", "string", "users_4_user_cellphone", "string"), ("users_4_user_phone", "string", "users_4_user_phone", "string"), ("users_4_user_email", "string", "users_4_user_email", "string"), ("users_4_user_documents_1_value", "string", "users_4_user_documents_1_value", "string"), ("users_4_user_documents_1_type", "string", "users_4_user_documents_1_type", "string"), ("users_4_user_documents_2_value", "string", "users_4_user_documents_2_value", "string"), ("users_4_user_documents_2_type", "string", "users_4_user_documents_2_type", "string"), ("users_4_user_address_city", "string", "users_4_user_address_city", "string"), ("users_4_user_address_state", "string", "users_4_user_address_state", "string"), ("users_4_user_address_country", "string", "users_4_user_address_country", "string"), ("users_4_user_address_zip_code", "string", "users_4_user_address_zip_code", "string"), ("users_4_user_address_address", "string", "users_4_user_address_address", "string"), ("users_4_user_address_complement", "string", "users_4_user_address_complement", "string"), ("users_4_user_address_neighborhood", "string", "users_4_user_address_neighborhood", "string"), ("users_4_user_address_number", "string", "users_4_user_address_number", "string"), ("users_5_role", "string", "users_5_role", "string"), ("users_5_user_ucode", "string", "users_5_user_ucode", "string"), ("users_5_user_locale", "string", "users_5_user_locale", "string"), ("users_5_user_name", "string", "users_5_user_name", "string"), ("users_5_user_trade_name", "string", "users_5_user_trade_name", "string"), ("users_5_user_cellphone", "string", "users_5_user_cellphone", "string"), ("users_5_user_phone", "string", "users_5_user_phone", "string"), ("users_5_user_email", "string", "users_5_user_email", "string"), ("users_5_user_documents_1_value", "string", "users_5_user_documents_1_value", "string"), ("users_5_user_documents_1_type", "string", "users_5_user_documents_1_type", "string"), ("users_5_user_documents_2_value", "string", "users_5_user_documents_2_value", "string"), ("users_5_user_documents_2_type", "string", "users_5_user_documents_2_type", "string"), ("users_5_user_address_city", "string", "users_5_user_address_city", "string"), ("users_5_user_address_state", "string", "users_5_user_address_state", "string"), ("users_5_user_address_country", "string", "users_5_user_address_country", "string"), ("users_5_user_address_zip_code", "string", "users_5_user_address_zip_code", "string"), ("users_5_user_address_address", "string", "users_5_user_address_address", "string"), ("users_5_user_address_complement", "string", "users_5_user_address_complement", "string"), ("users_5_user_address_neighborhood", "string", "users_5_user_address_neighborhood", "string"), ("users_5_user_address_number", "string", "users_5_user_address_number", "string"), ("users_6_role", "string", "users_6_role", "string"), ("users_6_user_ucode", "string", "users_6_user_ucode", "string"), ("users_6_user_locale", "string", "users_6_user_locale", "string"), ("users_6_user_name", "string", "users_6_user_name", "string"), ("users_6_user_trade_name", "string", "users_6_user_trade_name", "string"), ("users_6_user_cellphone", "string", "users_6_user_cellphone", "string"), ("users_6_user_phone", "string", "users_6_user_phone", "string"), ("users_6_user_email", "string", "users_6_user_email", "string"), ("users_6_user_documents_1_value", "string", "users_6_user_documents_1_value", "string"), ("users_6_user_documents_1_type", "string", "users_6_user_documents_1_type", "string"), ("users_6_user_documents_2_value", "string", "users_6_user_documents_2_value", "string"), ("users_6_user_documents_2_type", "string", "users_6_user_documents_2_type", "string"), ("users_6_user_address_city", "string", "users_6_user_address_city", "string"), ("users_6_user_address_state", "string", "users_6_user_address_state", "string"), ("users_6_user_address_country", "string", "users_6_user_address_country", "string"), ("users_6_user_address_zip_code", "string", "users_6_user_address_zip_code", "string"), ("users_6_user_address_address", "string", "users_6_user_address_address", "string"), ("users_6_user_address_complement", "string", "users_6_user_address_complement", "string"), ("users_6_user_address_neighborhood", "string", "users_6_user_address_neighborhood", "string"), ("users_6_user_address_number", "string", "users_6_user_address_number", "string"), ("users_7_role", "string", "users_7_role", "string"), ("users_7_user_ucode", "string", "users_7_user_ucode", "string"), ("users_7_user_locale", "string", "users_7_user_locale", "string"), ("users_7_user_name", "string", "users_7_user_name", "string"), ("users_7_user_trade_name", "string", "users_7_user_trade_name", "string"), ("users_7_user_cellphone", "string", "users_7_user_cellphone", "string"), ("users_7_user_phone", "string", "users_7_user_phone", "string"), ("users_7_user_email", "string", "users_7_user_email", "string"), ("users_7_user_documents_1_value", "string", "users_7_user_documents_1_value", "string"), ("users_7_user_documents_1_type", "string", "users_7_user_documents_1_type", "string"), ("users_7_user_documents_2_value", "string", "users_7_user_documents_2_value", "string"), ("users_7_user_documents_2_type", "string", "users_7_user_documents_2_type", "string"), ("users_7_user_address_city", "string", "users_7_user_address_city", "string"), ("users_7_user_address_state", "string", "users_7_user_address_state", "string"), ("users_7_user_address_country", "string", "users_7_user_address_country", "string"), ("users_7_user_address_zip_code", "string", "users_7_user_address_zip_code", "string"), ("users_7_user_address_address", "string", "users_7_user_address_address", "string"), ("users_7_user_address_complement", "string", "users_7_user_address_complement", "string"), ("users_7_user_address_neighborhood", "string", "users_7_user_address_neighborhood", "string"), ("users_7_user_address_number", "string", "users_7_user_address_number", "string"), ("users_8_role", "string", "users_8_role", "string"), ("users_8_user_ucode", "string", "users_8_user_ucode", "string"), ("users_8_user_locale", "string", "users_8_user_locale", "string"), ("users_8_user_name", "string", "users_8_user_name", "string"), ("users_8_user_trade_name", "string", "users_8_user_trade_name", "string"), ("users_8_user_cellphone", "string", "users_8_user_cellphone", "string"), ("users_8_user_phone", "string", "users_8_user_phone", "string"), ("users_8_user_email", "string", "users_8_user_email", "string"), ("users_8_user_documents_1_value", "string", "users_8_user_documents_1_value", "string"), ("users_8_user_documents_1_type", "string", "users_8_user_documents_1_type", "string"), ("users_8_user_documents_2_value", "string", "users_8_user_documents_2_value", "string"), ("users_8_user_documents_2_type", "string", "users_8_user_documents_2_type", "string"), ("users_8_user_address_city", "string", "users_8_user_address_city", "string"), ("users_8_user_address_state", "string", "users_8_user_address_state", "string"), ("users_8_user_address_country", "string", "users_8_user_address_country", "string"), ("users_8_user_address_zip_code", "string", "users_8_user_address_zip_code", "string"), ("users_8_user_address_address", "string", "users_8_user_address_address", "string"), ("users_8_user_address_complement", "string", "users_8_user_address_complement", "string"), ("users_8_user_address_neighborhood", "string", "users_8_user_address_neighborhood", "string"), ("users_8_user_address_number", "string", "users_8_user_address_number", "string"), ("users_9_role", "string", "users_9_role", "string"), ("users_9_user_ucode", "string", "users_9_user_ucode", "string"), ("users_9_user_locale", "string", "users_9_user_locale", "string"), ("users_9_user_name", "string", "users_9_user_name", "string"), ("users_9_user_trade_name", "string", "users_9_user_trade_name", "string"), ("users_9_user_cellphone", "string", "users_9_user_cellphone", "string"), ("users_9_user_phone", "string", "users_9_user_phone", "string"), ("users_9_user_email", "string", "users_9_user_email", "string"), ("users_9_user_documents_1_value", "string", "users_9_user_documents_1_value", "string"), ("users_9_user_documents_1_type", "string", "users_9_user_documents_1_type", "string"), ("users_9_user_documents_2_value", "string", "users_9_user_documents_2_value", "string"), ("users_9_user_documents_2_type", "string", "users_9_user_documents_2_type", "string"), ("users_9_user_address_city", "string", "users_9_user_address_city", "string"), ("users_9_user_address_state", "string", "users_9_user_address_state", "string"), ("users_9_user_address_country", "string", "users_9_user_address_country", "string"), ("users_9_user_address_zip_code", "string", "users_9_user_address_zip_code", "string"), ("users_9_user_address_address", "string", "users_9_user_address_address", "string"), ("users_9_user_address_complement", "string", "users_9_user_address_complement", "string"), ("users_9_user_address_neighborhood", "string", "users_9_user_address_neighborhood", "string"), ("users_9_user_address_number", "string", "users_9_user_address_number", "string"), ("users_10_role", "string", "users_10_role", "string"), ("users_10_user_ucode", "string", "users_10_user_ucode", "string"), ("users_10_user_locale", "string", "users_10_user_locale", "string"), ("users_10_user_name", "string", "users_10_user_name", "string"), ("users_10_user_trade_name", "string", "users_10_user_trade_name", "string"), ("users_10_user_cellphone", "string", "users_10_user_cellphone", "string"), ("users_10_user_phone", "string", "users_10_user_phone", "string"), ("users_10_user_email", "string", "users_10_user_email", "string"), ("users_10_user_documents_1_value", "string", "users_10_user_documents_1_value", "string"), ("users_10_user_documents_1_type", "string", "users_10_user_documents_1_type", "string"), ("users_10_user_documents_2_value", "string", "users_10_user_documents_2_value", "string"), ("users_10_user_documents_2_type", "string", "users_10_user_documents_2_type", "string"), ("users_10_user_address_city", "string", "users_10_user_address_city", "string"), ("users_10_user_address_state", "string", "users_10_user_address_state", "string"), ("users_10_user_address_country", "string", "users_10_user_address_country", "string"), ("users_10_user_address_zip_code", "string", "users_10_user_address_zip_code", "string"), ("users_10_user_address_address", "string", "users_10_user_address_address", "string"), ("users_10_user_address_complement", "string", "users_10_user_address_complement", "string"), ("users_10_user_address_neighborhood", "string", "users_10_user_address_neighborhood", "string"), ("users_10_user_address_number", "string", "users_10_user_address_number", "string"), ("purchase_status", "string", "purchase_status", "string")], transformation_ctx="TratarUsers_node")

# Script generated for node Price Dts + Comms
PriceDetailsS3_nodeDF = PriceDetailsS3_node.toDF()
TratarCommissions_nodeDF = TratarCommissions_node.toDF()
PriceDtsComms_node = DynamicFrame.fromDF(PriceDetailsS3_nodeDF.join(TratarCommissions_nodeDF, (PriceDetailsS3_nodeDF['transaction'] == TratarCommissions_nodeDF['transaction']) & (PriceDetailsS3_nodeDF['product_id'] == TratarCommissions_nodeDF['product_id']) & (PriceDetailsS3_nodeDF['product_name'] == TratarCommissions_nodeDF['product_name']), "outer"), glueContext, "PriceDtsComms_node")

# Script generated for node To Timestamp
ToTimestamp_node = TratarDatasHistory_node.gs_to_timestamp(colName="purchase_warranty_expire_date", colType="milliseconds")

# Script generated for node Criar Coluna total_value_converted
CriarColunatotal_value_converted_node = PriceDtsComms_node.gs_derived(colName="total_value_converted", expr="total_value * exchange_rate_currency_payout")

# Script generated for node Criar Coluna base_value_converted
CriarColunabase_value_converted_node = CriarColunatotal_value_converted_node.gs_derived(colName="base_value_converted", expr="base_value * exchange_rate_currency_payout")

# Script generated for node Derived Column
DerivedColumn_node = ToTimestamp_node.gs_derived(colName="purchase_warranty_expire_date", expr="coalesce(purchase_warranty_expire_date, '') as purchase_warranty_expire_date")

# Script generated for node Tratar Price Dts
TratarPriceDts_node = ApplyMapping.apply(frame=CriarColunabase_value_converted_node, mappings=[("real_conversion_rate", "string", "real_conversion_rate", "float"), ("vat_value", "string", "vat_value", "float"), ("vat_currency_code", "string", "vat_currency_code", "string"), ("product_name", "string", "product_name", "string"), ("product_id", "string", "product_id", "string"), ("transaction", "string", "transaction", "string"), ("base_value", "string", "base_value", "float"), ("base_currency_code", "string", "base_currency_code", "string"), ("total_value", "string", "total_value", "float"), ("total_currency_code", "string", "total_currency_code", "string"), ("coupon_code", "string", "coupon_code", "string"), ("coupon_value", "string", "coupon_value", "float"), ("fee_value", "string", "fee_value", "float"), ("fee_currency_code", "string", "fee_currency_code", "string"), ("purchase_status", "string", "purchase_status", "string"), ("commissions_1_commission_currency_code", "string", "commissions_currency_code", "string"), ("exchange_rate_currency_payout", "float", "exchange_rate_currency_payout", "float"), ("base_value_converted", "double", "base_value_converted", "float"), ("total_value_converted", "double", "total_value_converted", "float")], transformation_ctx="TratarPriceDts_node")

# Script generated for node Converter UTC-GMT
ConverterUTCGMT_node = DerivedColumn_node.gs_derived(colName="purchase_approved_date", expr="purchase_approved_date - 10800000")

# Script generated for node To Timestamp
ToTimestamp_node = ConverterUTCGMT_node.gs_to_timestamp(colName="purchase_approved_date", colType="milliseconds")

# Script generated for node Derived Column
DerivedColumn_node = ToTimestamp_node.gs_derived(colName="purchase_approved_date", expr="coalesce(purchase_approved_date, '') as purchase_approved_date")

# Script generated for node Converter UTC-GMT
ConverterUTCGMT_node = DerivedColumn_node.gs_derived(colName="purchase_order_date", expr="purchase_order_date - 10800000")

# Script generated for node To Timestamp
ToTimestamp_node = ConverterUTCGMT_node.gs_to_timestamp(colName="purchase_order_date", colType="milliseconds")

# Script generated for node Derived Column
DerivedColumn_node = ToTimestamp_node.gs_derived(colName="purchase_order_date", expr="coalesce(purchase_order_date, '') as purchase_order_date")

# Script generated for node Percentage
Percentage_node = DerivedColumn_node.gs_derived(colName="purchase_hotmart_fee_percentage", expr="purchase_hotmart_fee_percentage / 100")

# Script generated for node Tratar History
TratarHistory_node = ApplyMapping.apply(frame=Percentage_node, mappings=[("purchase_is_subscription", "boolean", "purchase_is_subscription", "boolean"), ("purchase_warranty_expire_date", "string", "purchase_warranty_expire_date", "timestamp"), ("purchase_approved_date", "string", "purchase_approved_date", "timestamp"), ("purchase_tracking_source_sck", "string", "purchase_tracking_source_sck", "string"), ("purchase_tracking_source", "string", "purchase_tracking_source", "string"), ("purchase_tracking_external_code", "string", "purchase_tracking_external_code", "string"), ("purchase_recurrency_number", "int", "purchase_recurrency_number", "int"), ("purchase_offer_payment_mode", "string", "purchase_offer_payment_mode", "string"), ("purchase_offer_code", "string", "purchase_offer_code", "string"), ("purchase_commission_as", "string", "purchase_commission_as", "string"), ("purchase_order_date", "string", "purchase_order_date", "timestamp"), ("purchase_price_value", "float", "purchase_price_value", "float"), ("purchase_price_currency_code", "string", "purchase_price_currency_code", "string"), ("purchase_status", "string", "purchase_status", "string"), ("purchase_payment_installments_number", "int", "purchase_payment_installments_number", "int"), ("purchase_payment_type", "string", "purchase_payment_type", "string"), ("purchase_payment_method", "string", "purchase_payment_method", "string"), ("purchase_hotmart_fee_percentage", "double", "purchase_hotmart_fee_percentage", "float"), ("purchase_hotmart_fee_currency_code", "string", "purchase_hotmart_fee_currency_code", "string"), ("purchase_hotmart_fee_total", "float", "purchase_hotmart_fee_total", "float"), ("purchase_hotmart_fee_base", "float", "purchase_hotmart_fee_base", "float"), ("purchase_hotmart_fee_fixed", "float", "purchase_hotmart_fee_fixed", "float"), ("purchase_transaction", "string", "purchase_transaction", "string"), ("producer_name", "string", "producer_name", "string"), ("producer_ucode", "string", "producer_ucode", "string"), ("product_id", "string", "product_id", "string"), ("product_name", "string", "product_name", "string"), ("buyer_name", "string", "buyer_name", "string"), ("buyer_ucode", "string", "buyer_ucode", "string"), ("buyer_email", "string", "buyer_email", "string")], transformation_ctx="TratarHistory_node")


#CODE MADE BY ME:

# Script generated for node Summary PostgreSQL
cursor.execute("DELETE FROM public.tabela_hotmart_summary")

dataframe_summary = TratarSummary_node.toDF()
batch_data_summary = dataframe_summary.collect()

for row in batch_data_summary:
    total_value_value = row['total_value_value']
    total_value_currency_code = row['total_value_currency_code']
    total_items = row['total_items']
    purchase_status = row['purchase_status']

    cursor.execute("""
        INSERT INTO public.tabela_hotmart_summary (
            total_value_value,
            total_value_currency_code,
            total_items,
            purchase_status
        ) VALUES (%s, %s, %s, %s)
    """, (total_value_value, total_value_currency_code, total_items, purchase_status))
#SummaryPostgreSQL_node = glueContext.write_dynamic_frame.from_catalog(frame=TratarSummary_node, database="hotmart_db", table_name="postgres_public_tabela_hotmart_summary", transformation_ctx="SummaryPostgreSQL_node")
    
# Script generated for node History PostgreSQL
dataframe_history = TratarHistory_node.toDF()
batch_data_history = dataframe_history.collect()

for row in batch_data_history:
    transaction = row['purchase_transaction']
    cursor.execute("""
    SELECT * FROM public.tabela_hotmart_history
        WHERE purchase_transaction = %s""", (transaction,))
    existing_record = cursor.fetchone()
    
    if existing_record:
        cursor.execute("""
            UPDATE public.tabela_hotmart_history 
            SET 
                purchase_is_subscription = %s,
                purchase_warranty_expire_date = %s,
                purchase_approved_date = %s,
                purchase_tracking_source_sck = %s,
                purchase_tracking_source = %s,
                purchase_tracking_external_code = %s,
                purchase_recurrency_number = %s,
                purchase_offer_payment_mode = %s,
                purchase_offer_code = %s,
                purchase_commission_as = %s,
                purchase_order_date = %s,
                purchase_price_value = %s,
                purchase_price_currency_code = %s,
                purchase_status = %s,
                purchase_payment_installments_number = %s,
                purchase_payment_type = %s,
                purchase_payment_method = %s,
                purchase_hotmart_fee_percentage = %s,
                purchase_hotmart_fee_currency_code = %s,
                purchase_hotmart_fee_total = %s,
                purchase_hotmart_fee_base = %s,
                purchase_hotmart_fee_fixed = %s,
                producer_name = %s,
                producer_ucode = %s,
                product_id = %s,
                product_name = %s,
                buyer_name = %s,
                buyer_ucode = %s,
                buyer_email = %s
            WHERE purchase_transaction = %s
            """,
            (
                row['purchase_is_subscription'],
                row['purchase_warranty_expire_date'],
                row['purchase_approved_date'],
                row['purchase_tracking_source_sck'],
                row['purchase_tracking_source'],
                row['purchase_tracking_external_code'],
                row['purchase_recurrency_number'],
                row['purchase_offer_payment_mode'],
                row['purchase_offer_code'],
                row['purchase_commission_as'],
                row['purchase_order_date'],
                row['purchase_price_value'],
                row['purchase_price_currency_code'],
                row['purchase_status'],
                row['purchase_payment_installments_number'],
                row['purchase_payment_type'],
                row['purchase_payment_method'],
                row['purchase_hotmart_fee_percentage'],
                row['purchase_hotmart_fee_currency_code'],
                row['purchase_hotmart_fee_total'],
                row['purchase_hotmart_fee_base'],
                row['purchase_hotmart_fee_fixed'],
                row['producer_name'],
                row['producer_ucode'],
                row['product_id'],
                row['product_name'],
                row['buyer_name'],
                row['buyer_ucode'],
                row['buyer_email'],
                transaction
            )
        )
    else:
        cursor.execute("""
            INSERT INTO public.tabela_hotmart_history (
                purchase_transaction,
                purchase_is_subscription,
                purchase_warranty_expire_date,
                purchase_approved_date,
                purchase_tracking_source_sck,
                purchase_tracking_source,
                purchase_tracking_external_code,
                purchase_recurrency_number,
                purchase_offer_payment_mode,
                purchase_offer_code,
                purchase_commission_as,
                purchase_order_date,
                purchase_price_value,
                purchase_price_currency_code,
                purchase_status,
                purchase_payment_installments_number,
                purchase_payment_type,
                purchase_payment_method,
                purchase_hotmart_fee_percentage,
                purchase_hotmart_fee_currency_code,
                purchase_hotmart_fee_total,
                purchase_hotmart_fee_base,
                purchase_hotmart_fee_fixed,
                producer_name,
                producer_ucode,
                product_id,
                product_name,
                buyer_name,
                buyer_ucode,
                buyer_email
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                transaction,
                row['purchase_is_subscription'],
                row['purchase_warranty_expire_date'],
                row['purchase_approved_date'],
                row['purchase_tracking_source_sck'],
                row['purchase_tracking_source'],
                row['purchase_tracking_external_code'],
                row['purchase_recurrency_number'],
                row['purchase_offer_payment_mode'],
                row['purchase_offer_code'],
                row['purchase_commission_as'],
                row['purchase_order_date'],
                row['purchase_price_value'],
                row['purchase_price_currency_code'],
                row['purchase_status'],
                row['purchase_payment_installments_number'],
                row['purchase_payment_type'],
                row['purchase_payment_method'],
                row['purchase_hotmart_fee_percentage'],
                row['purchase_hotmart_fee_currency_code'],
                row['purchase_hotmart_fee_total'],
                row['purchase_hotmart_fee_base'],
                row['purchase_hotmart_fee_fixed'],
                row['producer_name'],
                row['producer_ucode'],
                row['product_id'],
                row['product_name'],
                row['buyer_name'],
                row['buyer_ucode'],
                row['buyer_email']
            )
        )
#HistoryPostgreSQL_node = glueContext.write_dynamic_frame.from_catalog(frame=TratarHistory_node, database="hotmart_db", table_name="postgres_public_tabela_hotmart_history", transformation_ctx="HistoryPostgreSQL_node")


# Script generated for node Commissions PostgreSQL
dataframe_commissions = TratarCommissions_node.toDF()
batch_data_commissions = dataframe_commissions.collect()

for row in batch_data_commissions:
    transaction = row['transaction']
    cursor.execute("""SELECT * FROM public.tabela_hotmart_commissions
        WHERE transaction = %s""", (transaction,))
    existing_record = cursor.fetchone()

    if existing_record:
        cursor.execute("""
            UPDATE public.tabela_hotmart_commissions
            SET
                commissions_1_commission_value = %s,
                commissions_1_commission_currency_code = %s,
                commissions_1_source = %s,
                commissions_1_user_email = %s,
                commissions_1_user_name = %s,
                commissions_1_user_ucode = %s,
                commissions_2_commission_value = %s,
                commissions_2_source = %s,
                commissions_2_user_email = %s,
                commissions_2_user_name = %s,
                commissions_2_user_ucode = %s,
                commissions_3_commission_value = %s,
                commissions_3_commission_currency_code = %s,
                commissions_3_source = %s,
                commissions_3_user_email = %s,
                commissions_3_user_name = %s,
                commissions_3_user_ucode = %s,
                exchange_rate_currency_payout = %s,
                product_name = %s,
                product_id = %s,
                commissions_2_commission_currency_code = %s,
                commissions_4_commission_value = %s,
                commissions_4_commission_currency_code = %s,
                commissions_4_source = %s,
                commissions_4_user_name = %s,
                commissions_4_user_ucode = %s,
                commissions_4_user_email = %s,
                commissions_5_commission_value = %s,
                commissions_5_commission_currency_code = %s,
                commissions_5_source = %s,
                commissions_5_user_name = %s,
                commissions_5_user_ucode = %s,
                commissions_5_user_email = %s,
                commissions_6_commission_value = %s,
                commissions_6_commission_currency_code = %s,
                commissions_6_source = %s,
                commissions_6_user_name = %s,
                commissions_6_user_ucode = %s,
                commissions_6_user_email = %s,
                commissions_7_commission_value = %s,
                commissions_7_commission_currency_code = %s,
                commissions_7_source = %s,
                commissions_7_user_name = %s,
                commissions_7_user_ucode = %s,
                commissions_7_user_email = %s,
                commissions_8_commission_value = %s,
                commissions_8_commission_currency_code = %s,
                commissions_8_source = %s,
                commissions_8_user_name = %s,
                commissions_8_user_ucode = %s,
                commissions_8_user_email = %s,
                commissions_9_commission_value = %s,
                commissions_9_commission_currency_code = %s,
                commissions_9_source = %s,
                commissions_9_user_name = %s,
                commissions_9_user_ucode = %s,
                commissions_9_user_email = %s,
                commissions_10_commission_value = %s,
                commissions_10_commission_currency_code = %s,
                commissions_10_source = %s,
                commissions_10_user_name = %s,
                commissions_10_user_ucode = %s,
                commissions_10_user_email = %s,
                purchase_status = %s
            WHERE transaction = %s
            """,
            (
                row['commissions_1_commission_value'],
                row['commissions_1_commission_currency_code'],
                row['commissions_1_source'],
                row['commissions_1_user_email'],
                row['commissions_1_user_name'],
                row['commissions_1_user_ucode'],
                row['commissions_2_commission_value'],
                row['commissions_2_source'],
                row['commissions_2_user_email'],
                row['commissions_2_user_name'],
                row['commissions_2_user_ucode'],
                row['commissions_3_commission_value'],
                row['commissions_3_commission_currency_code'],
                row['commissions_3_source'],
                row['commissions_3_user_email'],
                row['commissions_3_user_name'],
                row['commissions_3_user_ucode'],
                row['exchange_rate_currency_payout'],
                row['product_name'],
                row['product_id'],
                row['commissions_2_commission_currency_code'],
                row['commissions_4_commission_value'],
                row['commissions_4_commission_currency_code'],
                row['commissions_4_source'],
                row['commissions_4_user_name'],
                row['commissions_4_user_ucode'],
                row['commissions_4_user_email'],
                row['commissions_5_commission_value'],
                row['commissions_5_commission_currency_code'],
                row['commissions_5_source'],
                row['commissions_5_user_name'],
                row['commissions_5_user_ucode'],
                row['commissions_5_user_email'],
                row['commissions_6_commission_value'],
                row['commissions_6_commission_currency_code'],
                row['commissions_6_source'],
                row['commissions_6_user_name'],
                row['commissions_6_user_ucode'],
                row['commissions_6_user_email'],
                row['commissions_7_commission_value'],
                row['commissions_7_commission_currency_code'],
                row['commissions_7_source'],
                row['commissions_7_user_name'],
                row['commissions_7_user_ucode'],
                row['commissions_7_user_email'],
                row['commissions_8_commission_value'],
                row['commissions_8_commission_currency_code'],
                row['commissions_8_source'],
                row['commissions_8_user_name'],
                row['commissions_8_user_ucode'],
                row['commissions_8_user_email'],
                row['commissions_9_commission_value'],
                row['commissions_9_commission_currency_code'],
                row['commissions_9_source'],
                row['commissions_9_user_name'],
                row['commissions_9_user_ucode'],
                row['commissions_9_user_email'],
                row['commissions_10_commission_value'],
                row['commissions_10_commission_currency_code'],
                row['commissions_10_source'],
                row['commissions_10_user_name'],
                row['commissions_10_user_ucode'],
                row['commissions_10_user_email'],
                row['purchase_status'],
                row['transaction']
            )
        )
    else:
        cursor.execute("""
            INSERT INTO public.tabela_hotmart_commissions (
                commissions_1_commission_value,
                commissions_1_commission_currency_code,
                commissions_1_source,
                commissions_1_user_email,
                commissions_1_user_name,
                commissions_1_user_ucode,
                commissions_2_commission_value,
                commissions_2_source,
                commissions_2_user_email,
                commissions_2_user_name,
                commissions_2_user_ucode,
                commissions_3_commission_value,
                commissions_3_commission_currency_code,
                commissions_3_source,
                commissions_3_user_email,
                commissions_3_user_name,
                commissions_3_user_ucode,
                exchange_rate_currency_payout,
                product_name,
                product_id,
                transaction,
                commissions_2_commission_currency_code,
                commissions_4_commission_value,
                commissions_4_commission_currency_code,
                commissions_4_source,
                commissions_4_user_name,
                commissions_4_user_ucode,
                commissions_4_user_email,
                commissions_5_commission_value,
                commissions_5_commission_currency_code,
                commissions_5_source,
                commissions_5_user_name,
                commissions_5_user_ucode,
                commissions_5_user_email,
                commissions_6_commission_value,
                commissions_6_commission_currency_code,
                commissions_6_source,
                commissions_6_user_name,
                commissions_6_user_ucode,
                commissions_6_user_email,
                commissions_7_commission_value,
                commissions_7_commission_currency_code,
                commissions_7_source,
                commissions_7_user_name,
                commissions_7_user_ucode,
                commissions_7_user_email,
                commissions_8_commission_value,
                commissions_8_commission_currency_code,
                commissions_8_source,
                commissions_8_user_name,
                commissions_8_user_ucode,
                commissions_8_user_email,
                commissions_9_commission_value,
                commissions_9_commission_currency_code,
                commissions_9_source,
                commissions_9_user_name,
                commissions_9_user_ucode,
                commissions_9_user_email,
                commissions_10_commission_value,
                commissions_10_commission_currency_code,
                commissions_10_source,
                commissions_10_user_name,
                commissions_10_user_ucode,
                commissions_10_user_email,
                purchase_status
                ) VALUES (

%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            """,
            (
                row['commissions_1_commission_value'],
                row['commissions_1_commission_currency_code'],
                row['commissions_1_source'],
                row['commissions_1_user_email'],
                row['commissions_1_user_name'],
                row['commissions_1_user_ucode'],
                row['commissions_2_commission_value'],
                row['commissions_2_source'],
                row['commissions_2_user_email'],
                row['commissions_2_user_name'],
                row['commissions_2_user_ucode'],
                row['commissions_3_commission_value'],
                row['commissions_3_commission_currency_code'],
                row['commissions_3_source'],
                row['commissions_3_user_email'],
                row['commissions_3_user_name'],
                row['commissions_3_user_ucode'],
                row['exchange_rate_currency_payout'],
                row['product_name'],
                row['product_id'],
                row['transaction'],
                row['commissions_2_commission_currency_code'],
                row['commissions_4_commission_value'],
                row['commissions_4_commission_currency_code'],
                row['commissions_4_source'],
                row['commissions_4_user_name'],
                row['commissions_4_user_ucode'],
                row['commissions_4_user_email'],
                row['commissions_5_commission_value'],
                row['commissions_5_commission_currency_code'],
                row['commissions_5_source'],
                row['commissions_5_user_name'],
                row['commissions_5_user_ucode'],
                row['commissions_5_user_email'],
                row['commissions_6_commission_value'],
                row['commissions_6_commission_currency_code'],
                row['commissions_6_source'],
                row['commissions_6_user_name'],
                row['commissions_6_user_ucode'],
                row['commissions_6_user_email'],
                row['commissions_7_commission_value'],
                row['commissions_7_commission_currency_code'],
                row['commissions_7_source'],
                row['commissions_7_user_name'],
                row['commissions_7_user_ucode'],
                row['commissions_7_user_email'],
                row['commissions_8_commission_value'],
                row['commissions_8_commission_currency_code'],
                row['commissions_8_source'],
                row['commissions_8_user_name'],
                row['commissions_8_user_ucode'],
                row['commissions_8_user_email'],
                row['commissions_9_commission_value'],
                row['commissions_9_commission_currency_code'],
                row['commissions_9_source'],
                row['commissions_9_user_name'],
                row['commissions_9_user_ucode'],
                row['commissions_9_user_email'],
                row['commissions_10_commission_value'],
                row['commissions_10_commission_currency_code'],
                row['commissions_10_source'],
                row['commissions_10_user_name'],
                row['commissions_10_user_ucode'],
                row['commissions_10_user_email'],
                row['purchase_status']
            )
        )
#CommissionsPostgreSQL_node = glueContext.write_dynamic_frame.from_catalog(frame=TratarCommissions_node, database="hotmart_db", table_name="postgres_public_tabela_hotmart_commissions", transformation_ctx="CommissionsPostgreSQL_node")

# Script generated for node Users PostgreSQL
dataframe_users = TratarUsers_node.toDF()
batch_data_users = dataframe_users.collect()

for row in batch_data_users:
    transaction = row['transaction']
    cursor.execute("""SELECT * FROM public.tabela_hotmart_users
        WHERE transaction = %s""", (transaction,))
    existing_record = cursor.fetchone()

    if existing_record:
        cursor.execute("""
            UPDATE public.tabela_hotmart_users
            SET
                product_name = %s,
                product_id = %s,
                users_1_role = %s,
                users_1_user_ucode = %s,
                users_1_user_locale = %s,
                users_1_user_name = %s,
                users_1_user_trade_name = %s,
                users_1_user_cellphone = %s,
                users_1_user_phone = %s,
                users_1_user_email = %s,
                users_1_user_documents_1_value = %s,
                users_1_user_documents_1_type = %s,
                users_1_user_documents_2_value = %s,
                users_1_user_documents_2_type = %s,
                users_1_user_address_city = %s,
                users_1_user_address_state = %s,
                users_1_user_address_country = %s,
                users_1_user_address_zip_code = %s,
                users_1_user_address_address = %s,
                users_1_user_address_complement = %s,
                users_1_user_address_neighborhood = %s,
                users_1_user_address_number = %s,
                users_2_role = %s,
                users_2_user_ucode = %s,
                users_2_user_locale = %s,
                users_2_user_name = %s,
                users_2_user_trade_name = %s,
                users_2_user_cellphone = %s,
                users_2_user_phone = %s,
                users_2_user_email = %s,
                users_2_user_documents_1_value = %s,
                users_2_user_documents_1_type = %s,
                users_2_user_documents_2_value = %s,
                users_2_user_documents_2_type = %s,
                users_2_user_address_city = %s,
                users_2_user_address_state = %s,
                users_2_user_address_country = %s,
                users_2_user_address_zip_code = %s,
                users_2_user_address_address = %s,
                users_2_user_address_complement = %s,
                users_2_user_address_neighborhood = %s,
                users_2_user_address_number = %s,
                users_3_role = %s,
                users_3_user_ucode = %s,
                users_3_user_locale = %s,
                users_3_user_name = %s,
                users_3_user_trade_name = %s,
                users_3_user_cellphone = %s,
                users_3_user_phone = %s,
                users_3_user_email = %s,
                users_3_user_documents_1_value = %s,
                users_3_user_documents_1_type = %s,
                users_3_user_documents_2_value = %s,
                users_3_user_documents_2_type = %s,
                users_3_user_address_city = %s,
                users_3_user_address_state = %s,
                users_3_user_address_country = %s,
                users_3_user_address_zip_code = %s,
                users_3_user_address_address = %s,
                users_3_user_address_complement = %s,
                users_3_user_address_neighborhood = %s,
                users_3_user_address_number = %s,
                users_4_role = %s,
                users_4_user_ucode = %s,
                users_4_user_locale = %s,
                users_4_user_name = %s,
                users_4_user_trade_name = %s,
                users_4_user_cellphone = %s,
                users_4_user_phone = %s,
                users_4_user_email = %s,
                users_4_user_documents_1_value = %s,
                users_4_user_documents_1_type = %s,
                users_4_user_documents_2_value = %s,
                users_4_user_documents_2_type = %s,
                users_4_user_address_city = %s,
                users_4_user_address_state = %s,
                users_4_user_address_country = %s,
                users_4_user_address_zip_code = %s,
                users_4_user_address_address = %s,
                users_4_user_address_complement = %s,
                users_4_user_address_neighborhood = %s,
                users_4_user_address_number = %s,
                users_5_role = %s,
                users_5_user_ucode = %s,
                users_5_user_locale = %s,
                users_5_user_name = %s,
                users_5_user_trade_name = %s,
                users_5_user_cellphone = %s,
                users_5_user_phone = %s,
                users_5_user_email = %s,
                users_5_user_documents_1_value = %s,
                users_5_user_documents_1_type = %s,
                users_5_user_documents_2_value = %s,
                users_5_user_documents_2_type = %s,
                users_5_user_address_city = %s,
                users_5_user_address_state = %s,
                users_5_user_address_country = %s,
                users_5_user_address_zip_code = %s,
                users_5_user_address_address = %s,
                users_5_user_address_complement = %s,
                users_5_user_address_neighborhood = %s,
                users_5_user_address_number = %s,
                users_6_role = %s,
                users_6_user_ucode = %s,
                users_6_user_locale = %s,
                users_6_user_name = %s,
                users_6_user_trade_name = %s,
                users_6_user_cellphone = %s,
                users_6_user_phone = %s,
                users_6_user_email = %s,
                users_6_user_documents_1_value = %s,
                users_6_user_documents_1_type = %s,
                users_6_user_documents_2_value = %s,
                users_6_user_documents_2_type = %s,
                users_6_user_address_city = %s,
                users_6_user_address_state = %s,
                users_6_user_address_country = %s,
                users_6_user_address_zip_code = %s,
                users_6_user_address_address = %s,
                users_6_user_address_complement = %s,
                users_6_user_address_neighborhood = %s,
                users_6_user_address_number = %s,
                users_7_role = %s,
                users_7_user_ucode = %s,
                users_7_user_locale = %s,
                users_7_user_name = %s,
                users_7_user_trade_name = %s,
                users_7_user_cellphone = %s,
                users_7_user_phone = %s,
                users_7_user_email = %s,
                users_7_user_documents_1_value = %s,
                users_7_user_documents_1_type = %s,
                users_7_user_documents_2_value = %s,
                users_7_user_documents_2_type = %s,
                users_7_user_address_city = %s,
                users_7_user_address_state = %s,
                users_7_user_address_country = %s,
                users_7_user_address_zip_code = %s,
                users_7_user_address_address = %s,
                users_7_user_address_complement = %s,
                users_7_user_address_neighborhood = %s,
                users_7_user_address_number = %s,
                users_8_role = %s,
                users_8_user_ucode = %s,
                users_8_user_locale = %s,
                users_8_user_name = %s,
                users_8_user_trade_name = %s,
                users_8_user_cellphone = %s,
                users_8_user_phone = %s,
                users_8_user_email = %s,
                users_8_user_documents_1_value = %s,
                users_8_user_documents_1_type = %s,
                users_8_user_documents_2_value = %s,
                users_8_user_documents_2_type = %s,
                users_8_user_address_city = %s,
                users_8_user_address_state = %s,
                users_8_user_address_country = %s,
                users_8_user_address_zip_code = %s,
                users_8_user_address_address = %s,
                users_8_user_address_complement = %s,
                users_8_user_address_neighborhood = %s,
                users_8_user_address_number = %s,
                users_9_role = %s,
                users_9_user_ucode = %s,
                users_9_user_locale = %s,
                users_9_user_name = %s,
                users_9_user_trade_name = %s,
                users_9_user_cellphone = %s,
                users_9_user_phone = %s,
                users_9_user_email = %s,
                users_9_user_documents_1_value = %s,
                users_9_user_documents_1_type = %s,
                users_9_user_documents_2_value = %s,
                users_9_user_documents_2_type = %s,
                users_9_user_address_city = %s,
                users_9_user_address_state = %s,
                users_9_user_address_country = %s,
                users_9_user_address_zip_code = %s,
                users_9_user_address_address = %s,
                users_9_user_address_complement = %s,
                users_9_user_address_neighborhood = %s,
                users_9_user_address_number = %s,
                users_10_role = %s,
                users_10_user_ucode = %s,
                users_10_user_locale = %s,
                users_10_user_name = %s,
                users_10_user_trade_name = %s,
                users_10_user_cellphone = %s,
                users_10_user_phone = %s,
                users_10_user_email = %s,
                users_10_user_documents_1_value = %s,
                users_10_user_documents_1_type = %s,
                users_10_user_documents_2_value = %s,
                users_10_user_documents_2_type = %s,
                users_10_user_address_city = %s,
                users_10_user_address_state = %s,
                users_10_user_address_country = %s,
                users_10_user_address_zip_code = %s,
                users_10_user_address_address = %s,
                users_10_user_address_complement = %s,
                users_10_user_address_neighborhood = %s,
                users_10_user_address_number = %s,
                purchase_status = %s
            WHERE transaction = %s
            """,
            (
                row['product_name'],
                row['product_id'],
                row['users_1_role'],
                row['users_1_user_ucode'],
                row['users_1_user_locale'],
                row['users_1_user_name'],
                row['users_1_user_trade_name'],
                row['users_1_user_cellphone'],
                row['users_1_user_phone'],
                row['users_1_user_email'],
                row['users_1_user_documents_1_value'],
                row['users_1_user_documents_1_type'],
                row['users_1_user_documents_2_value'],
                row['users_1_user_documents_2_type'],
                row['users_1_user_address_city'],
                row['users_1_user_address_state'],
                row['users_1_user_address_country'],
                row['users_1_user_address_zip_code'],
                row['users_1_user_address_address'],
                row['users_1_user_address_complement'],
                row['users_1_user_address_neighborhood'],
                row['users_1_user_address_number'],
                row['users_2_role'],
                row['users_2_user_ucode'],
                row['users_2_user_locale'],
                row['users_2_user_name'],
                row['users_2_user_trade_name'],
                row['users_2_user_cellphone'],
                row['users_2_user_phone'],
                row['users_2_user_email'],
                row['users_2_user_documents_1_value'],
                row['users_2_user_documents_1_type'],
                row['users_2_user_documents_2_value'],
                row['users_2_user_documents_2_type'],
                row['users_2_user_address_city'],
                row['users_2_user_address_state'],
                row['users_2_user_address_country'],
                row['users_2_user_address_zip_code'],
                row['users_2_user_address_address'],
                row['users_2_user_address_complement'],
                row['users_2_user_address_neighborhood'],
                row['users_2_user_address_number'],
                row['users_3_role'],
                row['users_3_user_ucode'],
                row['users_3_user_locale'],
                row['users_3_user_name'],
                row['users_3_user_trade_name'],
                row['users_3_user_cellphone'],
                row['users_3_user_phone'],
                row['users_3_user_email'],
                row['users_3_user_documents_1_value'],
                row['users_3_user_documents_1_type'],
                row['users_3_user_documents_2_value'],
                row['users_3_user_documents_2_type'],
                row['users_3_user_address_city'],
                row['users_3_user_address_state'],
                row['users_3_user_address_country'],
                row['users_3_user_address_zip_code'],
                row['users_3_user_address_address'],
                row['users_3_user_address_complement'],
                row['users_3_user_address_neighborhood'],
                row['users_3_user_address_number'],
                row['users_4_role'],
                row['users_4_user_ucode'],
                row['users_4_user_locale'],
                row['users_4_user_name'],
                row['users_4_user_trade_name'],
                row['users_4_user_cellphone'],
                row['users_4_user_phone'],
                row['users_4_user_email'],
                row['users_4_user_documents_1_value'],
                row['users_4_user_documents_1_type'],
                row['users_4_user_documents_2_value'],
                row['users_4_user_documents_2_type'],
                row['users_4_user_address_city'],
                row['users_4_user_address_state'],
                row['users_4_user_address_country'],
                row['users_4_user_address_zip_code'],
                row['users_4_user_address_address'],
                row['users_4_user_address_complement'],
                row['users_4_user_address_neighborhood'],
                row['users_4_user_address_number'],
                row['users_5_role'],
                row['users_5_user_ucode'],
                row['users_5_user_locale'],
                row['users_5_user_name'],
                row['users_5_user_trade_name'],
                row['users_5_user_cellphone'],
                row['users_5_user_phone'],
                row['users_5_user_email'],
                row['users_5_user_documents_1_value'],
                row['users_5_user_documents_1_type'],
                row['users_5_user_documents_2_value'],
                row['users_5_user_documents_2_type'],
                row['users_5_user_address_city'],
                row['users_5_user_address_state'],
                row['users_5_user_address_country'],
                row['users_5_user_address_zip_code'],
                row['users_5_user_address_address'],
                row['users_5_user_address_complement'],
                row['users_5_user_address_neighborhood'],
                row['users_5_user_address_number'],
                row['users_6_role'],
                row['users_6_user_ucode'],
                row['users_6_user_locale'],
                row['users_6_user_name'],
                row['users_6_user_trade_name'],
                row['users_6_user_cellphone'],
                row['users_6_user_phone'],
                row['users_6_user_email'],
                row['users_6_user_documents_1_value'],
                row['users_6_user_documents_1_type'],
                row['users_6_user_documents_2_value'],
                row['users_6_user_documents_2_type'],
                row['users_6_user_address_city'],
                row['users_6_user_address_state'],
                row['users_6_user_address_country'],
                row['users_6_user_address_zip_code'],
                row['users_6_user_address_address'],
                row['users_6_user_address_complement'],
                row['users_6_user_address_neighborhood'],
                row['users_6_user_address_number'],
                row['users_7_role'],
                row['users_7_user_ucode'],
                row['users_7_user_locale'],
                row['users_7_user_name'],
                row['users_7_user_trade_name'],
                row['users_7_user_cellphone'],
                row['users_7_user_phone'],
                row['users_7_user_email'],
                row['users_7_user_documents_1_value'],
                row['users_7_user_documents_1_type'],
                row['users_7_user_documents_2_value'],
                row['users_7_user_documents_2_type'],
                row['users_7_user_address_city'],
                row['users_7_user_address_state'],
                row['users_7_user_address_country'],
                row['users_7_user_address_zip_code'],
                row['users_7_user_address_address'],
                row['users_7_user_address_complement'],
                row['users_7_user_address_neighborhood'],
                row['users_7_user_address_number'],
                row['users_8_role'],
                row['users_8_user_ucode'],
                row['users_8_user_locale'],
                row['users_8_user_name'],
                row['users_8_user_trade_name'],
                row['users_8_user_cellphone'],
                row['users_8_user_phone'],
                row['users_8_user_email'],
                row['users_8_user_documents_1_value'],
                row['users_8_user_documents_1_type'],
                row['users_8_user_documents_2_value'],
                row['users_8_user_documents_2_type'],
                row['users_8_user_address_city'],
                row['users_8_user_address_state'],
                row['users_8_user_address_country'],
                row['users_8_user_address_zip_code'],
                row['users_8_user_address_address'],
                row['users_8_user_address_complement'],
                row['users_8_user_address_neighborhood'],
                row['users_8_user_address_number'],
                row['users_9_role'],
                row['users_9_user_ucode'],
                row['users_9_user_locale'],
                row['users_9_user_name'],
                row['users_9_user_trade_name'],
                row['users_9_user_cellphone'],
                row['users_9_user_phone'],
                row['users_9_user_email'],
                row['users_9_user_documents_1_value'],
                row['users_9_user_documents_1_type'],
                row['users_9_user_documents_2_value'],
                row['users_9_user_documents_2_type'],
                row['users_9_user_address_city'],
                row['users_9_user_address_state'],
                row['users_9_user_address_country'],
                row['users_9_user_address_zip_code'],
                row['users_9_user_address_address'],
                row['users_9_user_address_complement'],
                row['users_9_user_address_neighborhood'],
                row['users_9_user_address_number'],
                row['users_10_role'],
                row['users_10_user_ucode'],
                row['users_10_user_locale'],
                row['users_10_user_name'],
                row['users_10_user_trade_name'],
                row['users_10_user_cellphone'],
                row['users_10_user_phone'],
                row['users_10_user_email'],
                row['users_10_user_documents_1_value'],
                row['users_10_user_documents_1_type'],
                row['users_10_user_documents_2_value'],
                row['users_10_user_documents_2_type'],
                row['users_10_user_address_city'],
                row['users_10_user_address_state'],
                row['users_10_user_address_country'],
                row['users_10_user_address_zip_code'],
                row['users_10_user_address_address'],
                row['users_10_user_address_complement'],
                row['users_10_user_address_neighborhood'],
                row['users_10_user_address_number'],
                row['purchase_status'],
                transaction
            )
        )
    else:
        cursor.execute("""
            INSERT INTO public.tabela_hotmart_users (
                transaction,
                product_name,
                product_id,
                users_1_role,
                users_1_user_ucode,
                users_1_user_locale,
                users_1_user_name,
                users_1_user_trade_name,
                users_1_user_cellphone,
                users_1_user_phone,
                users_1_user_email,
                users_1_user_documents_1_value,
                users_1_user_documents_1_type,
                users_1_user_documents_2_value,
                users_1_user_documents_2_type,
                users_1_user_address_city,
                users_1_user_address_state,
                users_1_user_address_country,
                users_1_user_address_zip_code,
                users_1_user_address_address,
                users_1_user_address_complement,
                users_1_user_address_neighborhood,
                users_1_user_address_number,
                users_2_role,
                users_2_user_ucode,
                users_2_user_locale,
                users_2_user_name,
                users_2_user_trade_name,
                users_2_user_cellphone,
                users_2_user_phone,
                users_2_user_email,
                users_2_user_documents_1_value,
                users_2_user_documents_1_type,
                users_2_user_documents_2_value,
                users_2_user_documents_2_type,
                users_2_user_address_city,
                users_2_user_address_state,
                users_2_user_address_country,
                users_2_user_address_zip_code,
                users_2_user_address_address,
                users_2_user_address_complement,
                users_2_user_address_neighborhood,
                users_2_user_address_number,
                users_3_role,
                users_3_user_ucode,
                users_3_user_locale,
                users_3_user_name,
                users_3_user_trade_name,
                users_3_user_cellphone,
                users_3_user_phone,
                users_3_user_email,
                users_3_user_documents_1_value,
                users_3_user_documents_1_type,
                users_3_user_documents_2_value,
                users_3_user_documents_2_type,
                users_3_user_address_city,
                users_3_user_address_state,
                users_3_user_address_country,
                users_3_user_address_zip_code,
                users_3_user_address_address,
                users_3_user_address_complement,
                users_3_user_address_neighborhood,
                users_3_user_address_number,
                users_4_role,
                users_4_user_ucode,
                users_4_user_locale,
                users_4_user_name,
                users_4_user_trade_name,
                users_4_user_cellphone,
                users_4_user_phone,
                users_4_user_email,
                users_4_user_documents_1_value,
                users_4_user_documents_1_type,
                users_4_user_documents_2_value,
                users_4_user_documents_2_type,
                users_4_user_address_city,
                users_4_user_address_state,
                users_4_user_address_country,
                users_4_user_address_zip_code,
                users_4_user_address_address,
                users_4_user_address_complement,
                users_4_user_address_neighborhood,
                users_4_user_address_number,
                users_5_role,
                users_5_user_ucode,
                users_5_user_locale,
                users_5_user_name,
                users_5_user_trade_name,
                users_5_user_cellphone,
                users_5_user_phone,
                users_5_user_email,
                users_5_user_documents_1_value,
                users_5_user_documents_1_type,
                users_5_user_documents_2_value,
                users_5_user_documents_2_type,
                users_5_user_address_city,
                users_5_user_address_state,
                users_5_user_address_country,
                users_5_user_address_zip_code,
                users_5_user_address_address,
                users_5_user_address_complement,
                users_5_user_address_neighborhood,
                users_5_user_address_number,
                users_6_role,
                users_6_user_ucode,
                users_6_user_locale,
                users_6_user_name,
                users_6_user_trade_name,
                users_6_user_cellphone,
                users_6_user_phone,
                users_6_user_email,
                users_6_user_documents_1_value,
                users_6_user_documents_1_type,
                users_6_user_documents_2_value,
                users_6_user_documents_2_type,
                users_6_user_address_city,
                users_6_user_address_state,
                users_6_user_address_country,
                users_6_user_address_zip_code,
                users_6_user_address_address,
                users_6_user_address_complement,
                users_6_user_address_neighborhood,
                users_6_user_address_number,
                users_7_role,
                users_7_user_ucode,
                users_7_user_locale,
                users_7_user_name,
                users_7_user_trade_name,
                users_7_user_cellphone,
                users_7_user_phone,
                users_7_user_email,
                users_7_user_documents_1_value,
                users_7_user_documents_1_type,
                users_7_user_documents_2_value,
                users_7_user_documents_2_type,
                users_7_user_address_city,
                users_7_user_address_state,
                users_7_user_address_country,
                users_7_user_address_zip_code,
                users_7_user_address_address,
                users_7_user_address_complement,
                users_7_user_address_neighborhood,
                users_7_user_address_number,
                users_8_role,
                users_8_user_ucode,
                users_8_user_locale,
                users_8_user_name,
                users_8_user_trade_name,
                users_8_user_cellphone,
                users_8_user_phone,
                users_8_user_email,
                users_8_user_documents_1_value,
                users_8_user_documents_1_type,
                users_8_user_documents_2_value,
                users_8_user_documents_2_type,
                users_8_user_address_city,
                users_8_user_address_state,
                users_8_user_address_country,
                users_8_user_address_zip_code,
                users_8_user_address_address,
                users_8_user_address_complement,
                users_8_user_address_neighborhood,
                users_8_user_address_number,
                users_9_role,
                users_9_user_ucode,
                users_9_user_locale,
                users_9_user_name,
                users_9_user_trade_name,
                users_9_user_cellphone,
                users_9_user_phone,
                users_9_user_email,
                users_9_user_documents_1_value,
                users_9_user_documents_1_type,
                users_9_user_documents_2_value,
                users_9_user_documents_2_type,
                users_9_user_address_city,
                users_9_user_address_state,
                users_9_user_address_country,
                users_9_user_address_zip_code,
                users_9_user_address_address,
                users_9_user_address_complement,
                users_9_user_address_neighborhood,
                users_9_user_address_number,
                users_10_role,
                users_10_user_ucode,
                users_10_user_locale,
                users_10_user_name,
                users_10_user_trade_name,
                users_10_user_cellphone,
                users_10_user_phone,
                users_10_user_email,
                users_10_user_documents_1_value,
                users_10_user_documents_1_type,
                users_10_user_documents_2_value,
                users_10_user_documents_2_type,
                users_10_user_address_city,
                users_10_user_address_state,
                users_10_user_address_country,
                users_10_user_address_zip_code,
                users_10_user_address_address,
                users_10_user_address_complement,
                users_10_user_address_neighborhood,
                users_10_user_address_number,
                purchase_status
            ) VALUES (

%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            """,
            (
                transaction,
                row['product_name'],
                row['product_id'],
                row['users_1_role'],
                row['users_1_user_ucode'],
                row['users_1_user_locale'],
                row['users_1_user_name'],
                row['users_1_user_trade_name'],
                row['users_1_user_cellphone'],
                row['users_1_user_phone'],
                row['users_1_user_email'],
                row['users_1_user_documents_1_value'],
                row['users_1_user_documents_1_type'],
                row['users_1_user_documents_2_value'],
                row['users_1_user_documents_2_type'],
                row['users_1_user_address_city'],
                row['users_1_user_address_state'],
                row['users_1_user_address_country'],
                row['users_1_user_address_zip_code'],
                row['users_1_user_address_address'],
                row['users_1_user_address_complement'],
                row['users_1_user_address_neighborhood'],
                row['users_1_user_address_number'],
                row['users_2_role'],
                row['users_2_user_ucode'],
                row['users_2_user_locale'],
                row['users_2_user_name'],
                row['users_2_user_trade_name'],
                row['users_2_user_cellphone'],
                row['users_2_user_phone'],
                row['users_2_user_email'],
                row['users_2_user_documents_1_value'],
                row['users_2_user_documents_1_type'],
                row['users_2_user_documents_2_value'],
                row['users_2_user_documents_2_type'],
                row['users_2_user_address_city'],
                row['users_2_user_address_state'],
                row['users_2_user_address_country'],
                row['users_2_user_address_zip_code'],
                row['users_2_user_address_address'],
                row['users_2_user_address_complement'],
                row['users_2_user_address_neighborhood'],
                row['users_2_user_address_number'],
                row['users_3_role'],
                row['users_3_user_ucode'],
                row['users_3_user_locale'],
                row['users_3_user_name'],
                row['users_3_user_trade_name'],
                row['users_3_user_cellphone'],
                row['users_3_user_phone'],
                row['users_3_user_email'],
                row['users_3_user_documents_1_value'],
                row['users_3_user_documents_1_type'],
                row['users_3_user_documents_2_value'],
                row['users_3_user_documents_2_type'],
                row['users_3_user_address_city'],
                row['users_3_user_address_state'],
                row['users_3_user_address_country'],
                row['users_3_user_address_zip_code'],
                row['users_3_user_address_address'],
                row['users_3_user_address_complement'],
                row['users_3_user_address_neighborhood'],
                row['users_3_user_address_number'],
                row['users_4_role'],
                row['users_4_user_ucode'],
                row['users_4_user_locale'],
                row['users_4_user_name'],
                row['users_4_user_trade_name'],
                row['users_4_user_cellphone'],
                row['users_4_user_phone'],
                row['users_4_user_email'],
                row['users_4_user_documents_1_value'],
                row['users_4_user_documents_1_type'],
                row['users_4_user_documents_2_value'],
                row['users_4_user_documents_2_type'],
                row['users_4_user_address_city'],
                row['users_4_user_address_state'],
                row['users_4_user_address_country'],
                row['users_4_user_address_zip_code'],
                row['users_4_user_address_address'],
                row['users_4_user_address_complement'],
                row['users_4_user_address_neighborhood'],
                row['users_4_user_address_number'],
                row['users_5_role'],
                row['users_5_user_ucode'],
                row['users_5_user_locale'],
                row['users_5_user_name'],
                row['users_5_user_trade_name'],
                row['users_5_user_cellphone'],
                row['users_5_user_phone'],
                row['users_5_user_email'],
                row['users_5_user_documents_1_value'],
                row['users_5_user_documents_1_type'],
                row['users_5_user_documents_2_value'],
                row['users_5_user_documents_2_type'],
                row['users_5_user_address_city'],
                row['users_5_user_address_state'],
                row['users_5_user_address_country'],
                row['users_5_user_address_zip_code'],
                row['users_5_user_address_address'],
                row['users_5_user_address_complement'],
                row['users_5_user_address_neighborhood'],
                row['users_5_user_address_number'],
                row['users_6_role'],
                row['users_6_user_ucode'],
                row['users_6_user_locale'],
                row['users_6_user_name'],
                row['users_6_user_trade_name'],
                row['users_6_user_cellphone'],
                row['users_6_user_phone'],
                row['users_6_user_email'],
                row['users_6_user_documents_1_value'],
                row['users_6_user_documents_1_type'],
                row['users_6_user_documents_2_value'],
                row['users_6_user_documents_2_type'],
                row['users_6_user_address_city'],
                row['users_6_user_address_state'],
                row['users_6_user_address_country'],
                row['users_6_user_address_zip_code'],
                row['users_6_user_address_address'],
                row['users_6_user_address_complement'],
                row['users_6_user_address_neighborhood'],
                row['users_6_user_address_number'],
                row['users_7_role'],
                row['users_7_user_ucode'],
                row['users_7_user_locale'],
                row['users_7_user_name'],
                row['users_7_user_trade_name'],
                row['users_7_user_cellphone'],
                row['users_7_user_phone'],
                row['users_7_user_email'],
                row['users_7_user_documents_1_value'],
                row['users_7_user_documents_1_type'],
                row['users_7_user_documents_2_value'],
                row['users_7_user_documents_2_type'],
                row['users_7_user_address_city'],
                row['users_7_user_address_state'],
                row['users_7_user_address_country'],
                row['users_7_user_address_zip_code'],
                row['users_7_user_address_address'],
                row['users_7_user_address_complement'],
                row['users_7_user_address_neighborhood'],
                row['users_7_user_address_number'],
                row['users_8_role'],
                row['users_8_user_ucode'],
                row['users_8_user_locale'],
                row['users_8_user_name'],
                row['users_8_user_trade_name'],
                row['users_8_user_cellphone'],
                row['users_8_user_phone'],
                row['users_8_user_email'],
                row['users_8_user_documents_1_value'],
                row['users_8_user_documents_1_type'],
                row['users_8_user_documents_2_value'],
                row['users_8_user_documents_2_type'],
                row['users_8_user_address_city'],
                row['users_8_user_address_state'],
                row['users_8_user_address_country'],
                row['users_8_user_address_zip_code'],
                row['users_8_user_address_address'],
                row['users_8_user_address_complement'],
                row['users_8_user_address_neighborhood'],
                row['users_8_user_address_number'],
                row['users_9_role'],
                row['users_9_user_ucode'],
                row['users_9_user_locale'],
                row['users_9_user_name'],
                row['users_9_user_trade_name'],
                row['users_9_user_cellphone'],
                row['users_9_user_phone'],
                row['users_9_user_email'],
                row['users_9_user_documents_1_value'],
                row['users_9_user_documents_1_type'],
                row['users_9_user_documents_2_value'],
                row['users_9_user_documents_2_type'],
                row['users_9_user_address_city'],
                row['users_9_user_address_state'],
                row['users_9_user_address_country'],
                row['users_9_user_address_zip_code'],
                row['users_9_user_address_address'],
                row['users_9_user_address_complement'],
                row['users_9_user_address_neighborhood'],
                row['users_9_user_address_number'],
                row['users_10_role'],
                row['users_10_user_ucode'],
                row['users_10_user_locale'],
                row['users_10_user_name'],
                row['users_10_user_trade_name'],
                row['users_10_user_cellphone'],
                row['users_10_user_phone'],
                row['users_10_user_email'],
                row['users_10_user_documents_1_value'],
                row['users_10_user_documents_1_type'],
                row['users_10_user_documents_2_value'],
                row['users_10_user_documents_2_type'],
                row['users_10_user_address_city'],
                row['users_10_user_address_state'],
                row['users_10_user_address_country'],
                row['users_10_user_address_zip_code'],
                row['users_10_user_address_address'],
                row['users_10_user_address_complement'],
                row['users_10_user_address_neighborhood'],
                row['users_10_user_address_number'],
                row['purchase_status']
            )
        )

# Script generated for node Price Dts PostgreSQL
dataframe_price_details = TratarPriceDts_node.toDF()
batch_data_price_details = dataframe_price_details.collect()

for row in batch_data_price_details:
    transaction = row['transaction']
    cursor.execute("""SELECT * FROM public.tabela_hotmart_price_details
        WHERE transaction = %s""", (transaction,))
    existing_record = cursor.fetchone()

    if existing_record:
        cursor.execute("""
            UPDATE public.tabela_hotmart_price_details
            SET
                real_conversion_rate = %s,
                vat_value = %s,
                vat_currency_code = %s,
                product_name = %s,
                product_id = %s,
                base_value = %s,
                base_currency_code = %s,
                total_value = %s,
                total_currency_code = %s,
                coupon_code = %s,
                coupon_value = %s,
                fee_value = %s,
                fee_currency_code = %s,
                purchase_status = %s,
                commissions_currency_code = %s,
                exchange_rate_currency_payout = %s,
                base_value_converted = %s,
                total_value_converted = %s
            WHERE transaction = %s
            """,
            (
                row['real_conversion_rate'],
                row['vat_value'],
                row['vat_currency_code'],
                row['product_name'],
                row['product_id'],
                row['base_value'],
                row['base_currency_code'],
                row['total_value'],
                row['total_currency_code'],
                row['coupon_code'],
                row['coupon_value'],
                row['fee_value'],
                row['fee_currency_code'],
                row['purchase_status'],
                row['commissions_currency_code'],
                row['exchange_rate_currency_payout'],
                row['base_value_converted'],
                row['total_value_converted'],
                transaction
            )
        )
    else:
        cursor.execute("""
            INSERT INTO public.tabela_hotmart_price_details (
                real_conversion_rate,
                vat_value,
                vat_currency_code,
                product_name,
                product_id,
                transaction,
                base_value,
                base_currency_code,
                total_value,
                total_currency_code,
                coupon_code,
                coupon_value,
                fee_value,
                fee_currency_code,
                purchase_status,
                commissions_currency_code,
                exchange_rate_currency_payout,
                base_value_converted,
                total_value_converted
            ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            """,
            (
                row['real_conversion_rate'],
                row['vat_value'],
                row['vat_currency_code'],
                row['product_name'],
                row['product_id'],
                row['transaction'],
                row['base_value'],
                row['base_currency_code'],
                row['total_value'],
                row['total_currency_code'],
                row['coupon_code'],
                row['coupon_value'],
                row['fee_value'],
                row['fee_currency_code'],
                row['purchase_status'],
                row['commissions_currency_code'],
                row['exchange_rate_currency_payout'],
                row['base_value_converted'],
                row['total_value_converted']
            )
        )
#PriceDtsPostgreSQL_node = glueContext.write_dynamic_frame.from_catalog(frame=TratarPriceDts_node, database="hotmart_db", table_name="postgres_public_tabela_hotmart_price_details", transformation_ctx="PriceDtsPostgreSQL_node")


#CODE GENERATED BY VISUAL ETL GLUE JOB:
# Script generated for node Relevant Data S3
RelevantDataS3_node = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": [relevant_data_bucket_path], "recurse": True}, transformation_ctx="RelevantDataS3_node")

# Script generated for node Drop Fields
DropFields_node = DropFields.apply(frame=RelevantDataS3_node, paths=["product_name_USERS_DUPLICATE", "product_id_USERS_DUPLICATE", "purchase_transaction", "purchase_status_USERS_DUPLICATE", "product_name_COMMISSIONS_DUPLICATE", "product_id_COMMISSIONS_DUPLICATE", "transaction_COMMISSIONS_DUPLICATE", "purchase_status_COMMISSIONS_DUPLICATE", "product_id_PRICE_DETAILS_DUPLICATE", "product_name_PRICE_DETAILS_DUPLICATE", "transaction_PRICE_DETAILS_DUPLICATE", "purchase_status_PRICE_DETAILS_DUPLICATE"], transformation_ctx="DropFields_node")

# Script generated for node Tratamento Inicial Relevant Data
TratamentoInicialRelevantData_node = ApplyMapping.apply(frame=DropFields_node, mappings=[("purchase_payment_method", "string", "purchase_payment_method", "string"), ("purchase_payment_type", "string", "purchase_payment_type", "string"), ("purchase_payment_installments_number", "string", "purchase_payment_installments_number", "int"), ("purchase_hotmart_fee_currency_code", "string", "purchase_hotmart_fee_currency_code", "string"), ("purchase_hotmart_fee_base", "string", "purchase_hotmart_fee_base", "float"), ("purchase_hotmart_fee_total", "string", "purchase_hotmart_fee_total", "float"), ("purchase_hotmart_fee_percentage", "string", "purchase_hotmart_fee_percentage", "float"), ("purchase_hotmart_fee_fixed", "string", "purchase_hotmart_fee_fixed", "float"), ("purchase_is_subscription", "string", "purchase_is_subscription", "boolean"), ("purchase_warranty_expire_date", "string", "purchase_warranty_expire_date", "bigint"), ("purchase_approved_date", "string", "purchase_approved_date", "bigint"), ("purchase_tracking_source", "string", "purchase_tracking_source", "string"), ("purchase_tracking_external_code", "string", "purchase_tracking_external_code", "string"), ("purchase_tracking_source_sck", "string", "purchase_tracking_source_sck", "string"), ("purchase_price_value", "string", "purchase_price_value", "float"), ("purchase_price_currency_code", "string", "purchase_price_currency_code", "string"), ("purchase_commission_as", "string", "purchase_commission_as", "string"), ("purchase_recurrency_number", "string", "purchase_recurrency_number", "int"), ("purchase_offer_code", "string", "purchase_offer_code", "string"), ("purchase_offer_payment_mode", "string", "purchase_offer_payment_mode", "string"), ("purchase_order_date", "string", "purchase_order_date", "bigint"), ("purchase_status", "string", "purchase_status", "string"), ("buyer_name", "string", "buyer_name", "string"), ("buyer_email", "string", "buyer_email", "string"), ("buyer_ucode", "string", "buyer_ucode", "string"), ("producer_name", "string", "producer_name", "string"), ("producer_ucode", "string", "producer_ucode", "string"), ("product_id", "string", "product_id", "string"), ("product_name", "string", "product_name", "string"), ("transaction", "string", "transaction", "string"), ("users_1_role", "string", "users_1_role", "string"), ("users_1_user_ucode", "string", "users_1_user_ucode", "string"), ("users_1_user_locale", "string", "users_1_user_locale", "string"), ("users_1_user_name", "string", "users_1_user_name", "string"), ("users_1_user_trade_name", "string", "users_1_user_trade_name", "string"), ("users_1_user_cellphone", "string", "users_1_user_cellphone", "string"), ("users_1_user_phone", "string", "users_1_user_phone", "string"), ("users_1_user_email", "string", "users_1_user_email", "string"), ("users_1_user_documents_1_value", "string", "users_1_user_documents_1_value", "string"), ("users_1_user_documents_1_type", "string", "users_1_user_documents_1_type", "string"), ("users_1_user_documents_2_value", "string", "users_1_user_documents_2_value", "string"), ("users_1_user_documents_2_type", "string", "users_1_user_documents_2_type", "string"), ("users_1_user_address_city", "string", "users_1_user_address_city", "string"), ("users_1_user_address_state", "string", "users_1_user_address_state", "string"), ("users_1_user_address_country", "string", "users_1_user_address_country", "string"), ("users_1_user_address_zip_code", "string", "users_1_user_address_zip_code", "string"), ("users_1_user_address_address", "string", "users_1_user_address_address", "string"), ("users_1_user_address_complement", "string", "users_1_user_address_complement", "string"), ("users_1_user_address_neighborhood", "string", "users_1_user_address_neighborhood", "string"), ("users_1_user_address_number", "string", "users_1_user_address_number", "string"), ("users_2_role", "string", "users_2_role", "string"), ("users_2_user_ucode", "string", "users_2_user_ucode", "string"), ("users_2_user_locale", "string", "users_2_user_locale", "string"), ("users_2_user_name", "string", "users_2_user_name", "string"), ("users_2_user_trade_name", "string", "users_2_user_trade_name", "string"), ("users_2_user_cellphone", "string", "users_2_user_cellphone", "string"), ("users_2_user_phone", "string", "users_2_user_phone", "string"), ("users_2_user_email", "string", "users_2_user_email", "string"), ("users_2_user_documents_1_value", "string", "users_2_user_documents_1_value", "string"), ("users_2_user_documents_1_type", "string", "users_2_user_documents_1_type", "string"), ("users_2_user_documents_2_value", "string", "users_2_user_documents_2_value", "string"), ("users_2_user_documents_2_type", "string", "users_2_user_documents_2_type", "string"), ("users_2_user_address_city", "string", "users_2_user_address_city", "string"), ("users_2_user_address_state", "string", "users_2_user_address_state", "string"), ("users_2_user_address_country", "string", "users_2_user_address_country", "string"), ("users_2_user_address_zip_code", "string", "users_2_user_address_zip_code", "string"), ("users_2_user_address_address", "string", "users_2_user_address_address", "string"), ("users_2_user_address_complement", "string", "users_2_user_address_complement", "string"), ("users_2_user_address_neighborhood", "string", "users_2_user_address_neighborhood", "string"), ("users_2_user_address_number", "string", "users_2_user_address_number", "string"), ("users_3_role", "string", "users_3_role", "string"), ("users_3_user_ucode", "string", "users_3_user_ucode", "string"), ("users_3_user_locale", "string", "users_3_user_locale", "string"), ("users_3_user_name", "string", "users_3_user_name", "string"), ("users_3_user_trade_name", "string", "users_3_user_trade_name", "string"), ("users_3_user_cellphone", "string", "users_3_user_cellphone", "string"), ("users_3_user_phone", "string", "users_3_user_phone", "string"), ("users_3_user_email", "string", "users_3_user_email", "string"), ("users_3_user_documents_1_value", "string", "users_3_user_documents_1_value", "string"), ("users_3_user_documents_1_type", "string", "users_3_user_documents_1_type", "string"), ("users_3_user_documents_2_value", "string", "users_3_user_documents_2_value", "string"), ("users_3_user_documents_2_type", "string", "users_3_user_documents_2_type", "string"), ("users_3_user_address_city", "string", "users_3_user_address_city", "string"), ("users_3_user_address_state", "string", "users_3_user_address_state", "string"), ("users_3_user_address_country", "string", "users_3_user_address_country", "string"), ("users_3_user_address_zip_code", "string", "users_3_user_address_zip_code", "string"), ("users_3_user_address_address", "string", "users_3_user_address_address", "string"), ("users_3_user_address_complement", "string", "users_3_user_address_complement", "string"), ("users_3_user_address_neighborhood", "string", "users_3_user_address_neighborhood", "string"), ("users_3_user_address_number", "string", "users_3_user_address_number", "string"), ("users_4_role", "string", "users_4_role", "string"), ("users_4_user_ucode", "string", "users_4_user_ucode", "string"), ("users_4_user_locale", "string", "users_4_user_locale", "string"), ("users_4_user_name", "string", "users_4_user_name", "string"), ("users_4_user_trade_name", "string", "users_4_user_trade_name", "string"), ("users_4_user_cellphone", "string", "users_4_user_cellphone", "string"), ("users_4_user_phone", "string", "users_4_user_phone", "string"), ("users_4_user_email", "string", "users_4_user_email", "string"), ("users_4_user_documents_1_value", "string", "users_4_user_documents_1_value", "string"), ("users_4_user_documents_1_type", "string", "users_4_user_documents_1_type", "string"), ("users_4_user_documents_2_value", "string", "users_4_user_documents_2_value", "string"), ("users_4_user_documents_2_type", "string", "users_4_user_documents_2_type", "string"), ("users_4_user_address_city", "string", "users_4_user_address_city", "string"), ("users_4_user_address_state", "string", "users_4_user_address_state", "string"), ("users_4_user_address_country", "string", "users_4_user_address_country", "string"), ("users_4_user_address_zip_code", "string", "users_4_user_address_zip_code", "string"), ("users_4_user_address_address", "string", "users_4_user_address_address", "string"), ("users_4_user_address_complement", "string", "users_4_user_address_complement", "string"), ("users_4_user_address_neighborhood", "string", "users_4_user_address_neighborhood", "string"), ("users_4_user_address_number", "string", "users_4_user_address_number", "string"), ("users_5_role", "string", "users_5_role", "string"), ("users_5_user_ucode", "string", "users_5_user_ucode", "string"), ("users_5_user_locale", "string", "users_5_user_locale", "string"), ("users_5_user_name", "string", "users_5_user_name", "string"), ("users_5_user_trade_name", "string", "users_5_user_trade_name", "string"), ("users_5_user_cellphone", "string", "users_5_user_cellphone", "string"), ("users_5_user_phone", "string", "users_5_user_phone", "string"), ("users_5_user_email", "string", "users_5_user_email", "string"), ("users_5_user_documents_1_value", "string", "users_5_user_documents_1_value", "string"), ("users_5_user_documents_1_type", "string", "users_5_user_documents_1_type", "string"), ("users_5_user_documents_2_value", "string", "users_5_user_documents_2_value", "string"), ("users_5_user_documents_2_type", "string", "users_5_user_documents_2_type", "string"), ("users_5_user_address_city", "string", "users_5_user_address_city", "string"), ("users_5_user_address_state", "string", "users_5_user_address_state", "string"), ("users_5_user_address_country", "string", "users_5_user_address_country", "string"), ("users_5_user_address_zip_code", "string", "users_5_user_address_zip_code", "string"), ("users_5_user_address_address", "string", "users_5_user_address_address", "string"), ("users_5_user_address_complement", "string", "users_5_user_address_complement", "string"), ("users_5_user_address_neighborhood", "string", "users_5_user_address_neighborhood", "string"), ("users_5_user_address_number", "string", "users_5_user_address_number", "string"), ("users_6_role", "string", "users_6_role", "string"), ("users_6_user_ucode", "string", "users_6_user_ucode", "string"), ("users_6_user_locale", "string", "users_6_user_locale", "string"), ("users_6_user_name", "string", "users_6_user_name", "string"), ("users_6_user_trade_name", "string", "users_6_user_trade_name", "string"), ("users_6_user_cellphone", "string", "users_6_user_cellphone", "string"), ("users_6_user_phone", "string", "users_6_user_phone", "string"), ("users_6_user_email", "string", "users_6_user_email", "string"), ("users_6_user_documents_1_value", "string", "users_6_user_documents_1_value", "string"), ("users_6_user_documents_1_type", "string", "users_6_user_documents_1_type", "string"), ("users_6_user_documents_2_value", "string", "users_6_user_documents_2_value", "string"), ("users_6_user_documents_2_type", "string", "users_6_user_documents_2_type", "string"), ("users_6_user_address_city", "string", "users_6_user_address_city", "string"), ("users_6_user_address_state", "string", "users_6_user_address_state", "string"), ("users_6_user_address_country", "string", "users_6_user_address_country", "string"), ("users_6_user_address_zip_code", "string", "users_6_user_address_zip_code", "string"), ("users_6_user_address_address", "string", "users_6_user_address_address", "string"), ("users_6_user_address_complement", "string", "users_6_user_address_complement", "string"), ("users_6_user_address_neighborhood", "string", "users_6_user_address_neighborhood", "string"), ("users_6_user_address_number", "string", "users_6_user_address_number", "string"), ("users_7_role", "string", "users_7_role", "string"), ("users_7_user_ucode", "string", "users_7_user_ucode", "string"), ("users_7_user_locale", "string", "users_7_user_locale", "string"), ("users_7_user_name", "string", "users_7_user_name", "string"), ("users_7_user_trade_name", "string", "users_7_user_trade_name", "string"), ("users_7_user_cellphone", "string", "users_7_user_cellphone", "string"), ("users_7_user_phone", "string", "users_7_user_phone", "string"), ("users_7_user_email", "string", "users_7_user_email", "string"), ("users_7_user_documents_1_value", "string", "users_7_user_documents_1_value", "string"), ("users_7_user_documents_1_type", "string", "users_7_user_documents_1_type", "string"), ("users_7_user_documents_2_value", "string", "users_7_user_documents_2_value", "string"), ("users_7_user_documents_2_type", "string", "users_7_user_documents_2_type", "string"), ("users_7_user_address_city", "string", "users_7_user_address_city", "string"), ("users_7_user_address_state", "string", "users_7_user_address_state", "string"), ("users_7_user_address_country", "string", "users_7_user_address_country", "string"), ("users_7_user_address_zip_code", "string", "users_7_user_address_zip_code", "string"), ("users_7_user_address_address", "string", "users_7_user_address_address", "string"), ("users_7_user_address_complement", "string", "users_7_user_address_complement", "string"), ("users_7_user_address_neighborhood", "string", "users_7_user_address_neighborhood", "string"), ("users_7_user_address_number", "string", "users_7_user_address_number", "string"), ("users_8_role", "string", "users_8_role", "string"), ("users_8_user_ucode", "string", "users_8_user_ucode", "string"), ("users_8_user_locale", "string", "users_8_user_locale", "string"), ("users_8_user_name", "string", "users_8_user_name", "string"), ("users_8_user_trade_name", "string", "users_8_user_trade_name", "string"), ("users_8_user_cellphone", "string", "users_8_user_cellphone", "string"), ("users_8_user_phone", "string", "users_8_user_phone", "string"), ("users_8_user_email", "string", "users_8_user_email", "string"), ("users_8_user_documents_1_value", "string", "users_8_user_documents_1_value", "string"), ("users_8_user_documents_1_type", "string", "users_8_user_documents_1_type", "string"), ("users_8_user_documents_2_value", "string", "users_8_user_documents_2_value", "string"), ("users_8_user_documents_2_type", "string", "users_8_user_documents_2_type", "string"), ("users_8_user_address_city", "string", "users_8_user_address_city", "string"), ("users_8_user_address_state", "string", "users_8_user_address_state", "string"), ("users_8_user_address_country", "string", "users_8_user_address_country", "string"), ("users_8_user_address_zip_code", "string", "users_8_user_address_zip_code", "string"), ("users_8_user_address_address", "string", "users_8_user_address_address", "string"), ("users_8_user_address_complement", "string", "users_8_user_address_complement", "string"), ("users_8_user_address_neighborhood", "string", "users_8_user_address_neighborhood", "string"), ("users_8_user_address_number", "string", "users_8_user_address_number", "string"), ("users_9_role", "string", "users_9_role", "string"), ("users_9_user_ucode", "string", "users_9_user_ucode", "string"), ("users_9_user_locale", "string", "users_9_user_locale", "string"), ("users_9_user_name", "string", "users_9_user_name", "string"), ("users_9_user_trade_name", "string", "users_9_user_trade_name", "string"), ("users_9_user_cellphone", "string", "users_9_user_cellphone", "string"), ("users_9_user_phone", "string", "users_9_user_phone", "string"), ("users_9_user_email", "string", "users_9_user_email", "string"), ("users_9_user_documents_1_value", "string", "users_9_user_documents_1_value", "string"), ("users_9_user_documents_1_type", "string", "users_9_user_documents_1_type", "string"), ("users_9_user_documents_2_value", "string", "users_9_user_documents_2_value", "string"), ("users_9_user_documents_2_type", "string", "users_9_user_documents_2_type", "string"), ("users_9_user_address_city", "string", "users_9_user_address_city", "string"), ("users_9_user_address_state", "string", "users_9_user_address_state", "string"), ("users_9_user_address_country", "string", "users_9_user_address_country", "string"), ("users_9_user_address_zip_code", "string", "users_9_user_address_zip_code", "string"), ("users_9_user_address_address", "string", "users_9_user_address_address", "string"), ("users_9_user_address_complement", "string", "users_9_user_address_complement", "string"), ("users_9_user_address_neighborhood", "string", "users_9_user_address_neighborhood", "string"), ("users_9_user_address_number", "string", "users_9_user_address_number", "string"), ("users_10_role", "string", "users_10_role", "string"), ("users_10_user_ucode", "string", "users_10_user_ucode", "string"), ("users_10_user_locale", "string", "users_10_user_locale", "string"), ("users_10_user_name", "string", "users_10_user_name", "string"), ("users_10_user_trade_name", "string", "users_10_user_trade_name", "string"), ("users_10_user_cellphone", "string", "users_10_user_cellphone", "string"), ("users_10_user_phone", "string", "users_10_user_phone", "string"), ("users_10_user_email", "string", "users_10_user_email", "string"), ("users_10_user_documents_1_value", "string", "users_10_user_documents_1_value", "string"), ("users_10_user_documents_1_type", "string", "users_10_user_documents_1_type", "string"), ("users_10_user_documents_2_value", "string", "users_10_user_documents_2_value", "string"), ("users_10_user_documents_2_type", "string", "users_10_user_documents_2_type", "string"), ("users_10_user_address_city", "string", "users_10_user_address_city", "string"), ("users_10_user_address_state", "string", "users_10_user_address_state", "string"), ("users_10_user_address_country", "string", "users_10_user_address_country", "string"), ("users_10_user_address_zip_code", "string", "users_10_user_address_zip_code", "string"), ("users_10_user_address_address", "string", "users_10_user_address_address", "string"), ("users_10_user_address_complement", "string", "users_10_user_address_complement", "string"), ("users_10_user_address_neighborhood", "string", "users_10_user_address_neighborhood", "string"), ("users_10_user_address_number", "string", "users_10_user_address_number", "string"), ("exchange_rate_currency_payout", "string", "exchange_rate_currency_payout", "float"), ("commissions_1_source", "string", "commissions_1_source", "string"), ("commissions_1_commission_value", "string", "commissions_1_commission_value", "float"), ("commissions_1_commission_currency_code", "string", "commissions_1_commission_currency_code", "string"), ("commissions_1_user_ucode", "string", "commissions_1_user_ucode", "string"), ("commissions_1_user_email", "string", "commissions_1_user_email", "string"), ("commissions_1_user_name", "string", "commissions_1_user_name", "string"), ("commissions_2_source", "string", "commissions_2_source", "string"), ("commissions_2_commission_value", "string", "commissions_2_commission_value", "float"), ("commissions_2_user_ucode", "string", "commissions_2_user_ucode", "string"), ("commissions_2_user_email", "string", "commissions_2_user_email", "string"), ("commissions_2_user_name", "string", "commissions_2_user_name", "string"), ("commissions_3_source", "string", "commissions_3_source", "string"), ("commissions_3_commission_value", "string", "commissions_3_commission_value", "float"), ("commissions_3_commission_currency_code", "string", "commissions_3_commission_currency_code", "string"), ("commissions_3_user_ucode", "string", "commissions_3_user_ucode", "string"), ("commissions_3_user_email", "string", "commissions_3_user_email", "string"), ("commissions_3_user_name", "string", "commissions_3_user_name", "string"), ("commissions_2_commission_currency_code", "string", "commissions_2_commission_currency_code", "string"), ("commissions_4_commission_value", "string", "commissions_4_commission_value", "float"), ("commissions_4_commission_currency_code", "string", "commissions_4_commission_currency_code", "string"), ("commissions_4_user_ucode", "string", "commissions_4_user_ucode", "string"), ("commissions_4_user_email", "string", "commissions_4_user_email", "string"), ("commissions_4_user_name", "string", "commissions_4_user_name", "string"), ("commissions_4_source", "string", "commissions_4_source", "string"), ("commissions_5_commission_value", "string", "commissions_5_commission_value", "float"), ("commissions_5_commission_currency_code", "string", "commissions_5_commission_currency_code", "string"), ("commissions_5_user_ucode", "string", "commissions_5_user_ucode", "string"), ("commissions_5_user_email", "string", "commissions_5_user_email", "string"), ("commissions_5_user_name", "string", "commissions_5_user_name", "string"), ("commissions_5_source", "string", "commissions_5_source", "string"), ("commissions_6_commission_value", "string", "commissions_6_commission_value", "float"), ("commissions_6_commission_currency_code", "string", "commissions_6_commission_currency_code", "string"), ("commissions_6_user_ucode", "string", "commissions_6_user_ucode", "string"), ("commissions_6_user_email", "string", "commissions_6_user_email", "string"), ("commissions_6_user_name", "string", "commissions_6_user_name", "string"), ("commissions_6_source", "string", "commissions_6_source", "string"), ("commissions_7_commission_value", "string", "commissions_7_commission_value", "float"), ("commissions_7_commission_currency_code", "string", "commissions_7_commission_currency_code", "string"), ("commissions_7_user_ucode", "string", "commissions_7_user_ucode", "string"), ("commissions_7_user_email", "string", "commissions_7_user_email", "string"), ("commissions_7_user_name", "string", "commissions_7_user_name", "string"), ("commissions_7_source", "string", "commissions_7_source", "string"), ("commissions_8_commission_value", "string", "commissions_8_commission_value", "string"), ("commissions_8_commission_currency_code", "string", "commissions_8_commission_currency_code", "string"), ("commissions_8_user_ucode", "string", "commissions_8_user_ucode", "string"), ("commissions_8_user_email", "string", "commissions_8_user_email", "string"), ("commissions_8_user_name", "string", "commissions_8_user_name", "string"), ("commissions_8_source", "string", "commissions_8_source", "string"), ("commissions_9_commission_value", "string", "commissions_9_commission_value", "float"), ("commissions_9_commission_currency_code", "string", "commissions_9_commission_currency_code", "string"), ("commissions_9_user_ucode", "string", "commissions_9_user_ucode", "string"), ("commissions_9_user_email", "string", "commissions_9_user_email", "string"), ("commissions_9_user_name", "string", "commissions_9_user_name", "string"), ("commissions_9_source", "string", "commissions_9_source", "string"), ("commissions_10_commission_value", "string", "commissions_10_commission_value", "float"), ("commissions_10_commission_currency_code", "string", "commissions_10_commission_currency_code", "string"), ("commissions_10_user_ucode", "string", "commissions_10_user_ucode", "string"), ("commissions_10_user_email", "string", "commissions_10_user_email", "string"), ("commissions_10_user_name", "string", "commissions_10_user_name", "string"), ("commissions_10_source", "string", "commissions_10_source", "string"), ("coupon_code", "string", "coupon_code", "string"), ("coupon_value", "string", "coupon_value", "float"), ("fee_value", "string", "fee_value", "float"), ("fee_currency_code", "string", "fee_currency_code", "string"), ("real_conversion_rate", "string", "real_conversion_rate", "float"), ("vat_value", "string", "vat_value", "float"), ("vat_currency_code", "string", "vat_currency_code", "string"), ("total_value", "string", "total_value", "float"), ("total_currency_code", "string", "total_currency_code", "string"), ("base_value", "string", "base_value", "float"), ("base_currency_code", "string", "base_currency_code", "string")], transformation_ctx="TratamentoInicialRelevantData_node")

# Script generated for node RD Criar Coluna base_value_converted
RDCriarColunabase_value_converted_node = TratamentoInicialRelevantData_node.gs_derived(colName="base_value_converted", expr="base_value * exchange_rate_currency_payout")

# Script generated for node RD Criar Coluna total_value_converted
RDCriarColunatotal_value_converted_node = RDCriarColunabase_value_converted_node.gs_derived(colName="total_value_converted", expr="total_value * exchange_rate_currency_payout")

# Script generated for node RD To Timestamp
RDToTimestamp_node = RDCriarColunatotal_value_converted_node.gs_to_timestamp(colName="purchase_warranty_expire_date", colType="milliseconds")

# Script generated for node RD Derived Column
RDDerivedColumn_node = RDToTimestamp_node.gs_derived(colName="purchase_warranty_expire_date", expr="coalesce(purchase_warranty_expire_date, '') as purchase_warranty_expire_date")

# Script generated for node RD Converter UTC-GMT
RDConverterUTCGMT_node = RDDerivedColumn_node.gs_derived(colName="purchase_approved_date", expr="purchase_approved_date - 10800000")

# Script generated for node RD To Timestamp
RDToTimestamp_node = RDConverterUTCGMT_node.gs_to_timestamp(colName="purchase_approved_date", colType="milliseconds")

# Script generated for node RD Derived Column
RDDerivedColumn_node = RDToTimestamp_node.gs_derived(colName="purchase_approved_date", expr="coalesce(purchase_approved_date, '') as purchase_approved_date")

# Script generated for node RD Converter UTC-GMT
RDConverterUTCGMT_node = RDDerivedColumn_node.gs_derived(colName="purchase_order_date", expr="purchase_order_date - 10800000")

# Script generated for node RD To Timestamp
RDToTimestamp_node = RDConverterUTCGMT_node.gs_to_timestamp(colName="purchase_order_date", colType="milliseconds")

# Script generated for node RD Derived Column
RDDerivedColumn_node = RDToTimestamp_node.gs_derived(colName="purchase_order_date", expr="coalesce(purchase_order_date, '') as purchase_order_date")

# Script generated for node RD Percentage
RDPercentage_node = RDDerivedColumn_node.gs_derived(colName="purchase_hotmart_fee_percentage", expr="purchase_hotmart_fee_percentage / 100")

# Script generated for node Tratar Relevant Data
TratarRelevantData_node = ApplyMapping.apply(frame=RDPercentage_node, mappings=[("purchase_payment_type", "string", "purchase_payment_type", "string"), ("purchase_payment_installments_number", "int", "purchase_payment_installments_number", "int"), ("purchase_hotmart_fee_total", "float", "purchase_hotmart_fee_total", "float"), ("purchase_hotmart_fee_percentage", "double", "purchase_hotmart_fee_percentage", "float"), ("purchase_warranty_expire_date", "string", "purchase_warranty_expire_date", "timestamp"), ("purchase_tracking_source", "string", "purchase_tracking_source", "string"), ("purchase_recurrency_number", "int", "purchase_recurrency_number", "int"), ("purchase_offer_payment_mode", "string", "purchase_offer_payment_mode", "string"), ("purchase_order_date", "string", "purchase_order_date", "timestamp"), ("purchase_status", "string", "purchase_status", "string"), ("buyer_email", "string", "buyer_email", "string"), ("product_name", "string", "product_name", "string"), ("transaction", "string", "transaction", "string"), ("users_1_user_name", "string", "users_1_user_name", "string"), ("users_2_user_name", "string", "users_2_user_name", "string"), ("users_2_user_address_city", "string", "users_2_user_address_city", "string"), ("users_2_user_address_state", "string", "users_2_user_address_state", "string"), ("users_2_user_address_country", "string", "users_2_user_address_country", "string"), ("commissions_1_source", "string", "commissions_1_source", "string"), ("commissions_1_commission_value", "float", "commissions_1_commission_value", "float"), ("commissions_1_commission_currency_code", "string", "commissions_1_commission_currency_code", "string"), ("commissions_1_user_email", "string", "commissions_1_user_email", "string"), ("commissions_2_source", "string", "commissions_2_source", "string"), ("commissions_2_commission_value", "float", "commissions_2_commission_value", "float"), ("commissions_2_user_email", "string", "commissions_2_user_email", "string"), ("commissions_3_source", "string", "commissions_3_source", "string"), ("commissions_3_commission_value", "float", "commissions_3_commission_value", "float"), ("commissions_3_commission_currency_code", "string", "commissions_3_commission_currency_code", "string"), ("commissions_3_user_email", "string", "commissions_3_user_email", "string"), ("commissions_2_commission_currency_code", "string", "commissions_2_commission_currency_code", "string"), ("commissions_4_commission_value", "float", "commissions_4_commission_value", "float"), ("commissions_4_commission_currency_code", "string", "commissions_4_commission_currency_code", "string"), ("commissions_4_user_email", "string", "commissions_4_user_email", "string"), ("commissions_4_source", "string", "commissions_4_source", "string"), ("commissions_5_commission_value", "float", "commissions_5_commission_value", "float"), ("commissions_5_commission_currency_code", "string", "commissions_5_commission_currency_code", "string"), ("commissions_5_user_email", "string", "commissions_5_user_email", "string"), ("commissions_5_source", "string", "commissions_5_source", "string"), ("commissions_6_commission_value", "float", "commissions_6_commission_value", "float"), ("commissions_6_commission_currency_code", "string", "commissions_6_commission_currency_code", "string"), ("commissions_6_user_email", "string", "commissions_6_user_email", "string"), ("commissions_6_source", "string", "commissions_6_source", "string"), ("commissions_7_commission_value", "float", "commissions_7_commission_value", "float"), ("commissions_7_commission_currency_code", "string", "commissions_7_commission_currency_code", "string"), ("commissions_7_user_email", "string", "commissions_7_user_email", "string"), ("commissions_7_source", "string", "commissions_7_source", "string"), ("commissions_8_commission_value", "string", "commissions_8_commission_value", "float"), ("commissions_8_commission_currency_code", "string", "commissions_8_commission_currency_code", "string"), ("commissions_8_user_email", "string", "commissions_8_user_email", "string"), ("commissions_8_source", "string", "commissions_8_source", "string"), ("commissions_9_commission_value", "float", "commissions_9_commission_value", "float"), ("commissions_9_commission_currency_code", "string", "commissions_9_commission_currency_code", "string"), ("commissions_9_user_email", "string", "commissions_9_user_email", "string"), ("commissions_9_source", "string", "commissions_9_source", "string"), ("commissions_10_commission_value", "float", "commissions_10_commission_value", "float"), ("commissions_10_commission_currency_code", "string", "commissions_10_commission_currency_code", "string"), ("commissions_10_user_email", "string", "commissions_10_user_email", "string"), ("commissions_10_source", "string", "commissions_10_source", "string"), ("base_currency_code", "string", "base_currency_code", "string"), ("base_value_converted", "float", "base_value_converted", "float"), ("total_value_converted", "float", "total_value_converted", "float")], transformation_ctx="TratarRelevantData_node")


#CODE FOR CUSTOM COLUMNS:
#Preço da Oferta (Valor da Comissão):
dataframe_relevant_data = TratarRelevantData_node.toDF()
dataframe_relevant_data = dataframe_relevant_data.withColumn(
    'purchase_offer_price_main_user',
    when(col("commissions_1_user_email") == commissions_main_email, col("commissions_1_commission_value"))
    .when(col("commissions_2_user_email") == commissions_main_email, col("commissions_2_commission_value"))
    .when(col("commissions_3_user_email") == commissions_main_email, col("commissions_3_commission_value"))
    .when(col("commissions_4_user_email") == commissions_main_email, col("commissions_4_commission_value"))
    .when(col("commissions_5_user_email") == commissions_main_email, col("commissions_5_commission_value"))
    .when(col("commissions_6_user_email") == commissions_main_email, col("commissions_6_commission_value"))
    .when(col("commissions_7_user_email") == commissions_main_email, col("commissions_7_commission_value"))
    .when(col("commissions_8_user_email") == commissions_main_email, col("commissions_8_commission_value"))
    .when(col("commissions_9_user_email") == commissions_main_email, col("commissions_9_commission_value"))
    .when(col("commissions_10_user_email") == commissions_main_email, col("commissions_10_commission_value"))
    .otherwise(0)
)

dataframe_relevant_data = dataframe_relevant_data.withColumn(
    "purchase_offer_payment_mode",
    when(col("purchase_offer_payment_mode") == "SUBSCRIPTION", "Assinatura")
    .when(col("purchase_payment_installments_number") == 1, "Apenas à vista")
    .when(col("purchase_offer_payment_mode") == "SMART_INSTALLMENT", "Parcelamento Inteligente")
    .when(col("purchase_offer_payment_mode") == "BILLET_INSTALLMENT", "Boleto Bancário")
    .when((col("purchase_offer_payment_mode") == "INVOICE") | (col("purchase_offer_payment_mode") == "NOT_DEFINED"), "Outros")
    .otherwise("Parcelado (até 12x)")
)

dataframe_relevant_data = dataframe_relevant_data.withColumn(
    'purchase_offer_currency_code',
    when(col("commissions_1_user_email") == commissions_main_email, col("commissions_1_commission_currency_code"))
    .when(col("commissions_2_user_email") == commissions_main_email, col("commissions_2_commission_currency_code"))
    .when(col("commissions_3_user_email") == commissions_main_email, col("commissions_3_commission_currency_code"))
    .when(col("commissions_4_user_email") == commissions_main_email, col("commissions_4_commission_currency_code"))
    .when(col("commissions_5_user_email") == commissions_main_email, col("commissions_5_commission_currency_code"))
    .when(col("commissions_6_user_email") == commissions_main_email, col("commissions_6_commission_currency_code"))
    .when(col("commissions_7_user_email") == commissions_main_email, col("commissions_7_commission_currency_code"))
    .when(col("commissions_8_user_email") == commissions_main_email, col("commissions_8_commission_currency_code"))
    .when(col("commissions_9_user_email") == commissions_main_email, col("commissions_9_commission_currency_code"))
    .when(col("commissions_10_user_email") == commissions_main_email, col("commissions_10_commission_currency_code"))
    .otherwise(0)
)

dataframe_relevant_data = dataframe_relevant_data.withColumn(
    "purchase_payment_type",
    when(col("purchase_payment_type") == "CREDIT_CARD", "Cartão de Crédito")
    .when(col("purchase_payment_type") == "APPLE_PAY", "Apple Pay")
    .when(col("purchase_payment_type") == "BILLET", "Boleto")
    .when(col("purchase_payment_type") == "CASH_PAYMENT", "Em Dinheiro")
    .when(col("purchase_payment_type") == "DIRECT_DEBIT", "Cartão de Débito")
    .when(col("purchase_payment_type") == "FINANCED_BILLET", "Boleto Financiado")
    .when(col("purchase_payment_type") == "GOOGLE_PAY", "Google Pay")
    .when(col("purchase_payment_type") == "HYBRID", "Híbrido")
    .when(col("purchase_payment_type") == "PAYPAL", "PayPal")
    .when(col("purchase_payment_type") == "PIX", "Pix")
    .when(col("purchase_payment_type") == "WALLET", "Wallet")
    .otherwise("Outro")
)

commission_sum = (
    when(col("commissions_1_commission_value").isNotNull(), col("commissions_1_commission_value")).otherwise(0) +
    when(col("commissions_2_commission_value").isNotNull(), col("commissions_2_commission_value")).otherwise(0) +
    when(col("commissions_3_commission_value").isNotNull(), col("commissions_3_commission_value")).otherwise(0) +
    when(col("commissions_4_commission_value").isNotNull(), col("commissions_4_commission_value")).otherwise(0) +
    when(col("commissions_5_commission_value").isNotNull(), col("commissions_5_commission_value")).otherwise(0) +
    when(col("commissions_6_commission_value").isNotNull(), col("commissions_6_commission_value")).otherwise(0) +
    when(col("commissions_7_commission_value").isNotNull(), col("commissions_7_commission_value")).otherwise(0) +
    when(col("commissions_8_commission_value").isNotNull(), col("commissions_8_commission_value")).otherwise(0) +
    when(col("commissions_9_commission_value").isNotNull(), col("commissions_9_commission_value")).otherwise(0) +
    when(col("commissions_10_commission_value").isNotNull(), col("commissions_10_commission_value")).otherwise(0)
)

addon_commissions = (
    when(col("commissions_1_source") == "ADDON", col("commissions_1_commission_value"))
    .when(col("commissions_2_source") == "ADDON", col("commissions_2_commission_value"))
    .when(col("commissions_3_source") == "ADDON", col("commissions_3_commission_value"))
    .when(col("commissions_4_source") == "ADDON", col("commissions_4_commission_value"))
    .when(col("commissions_5_source") == "ADDON", col("commissions_5_commission_value"))
    .when(col("commissions_6_source") == "ADDON", col("commissions_6_commission_value"))
    .when(col("commissions_7_source") == "ADDON", col("commissions_7_commission_value"))
    .when(col("commissions_8_source") == "ADDON", col("commissions_8_commission_value"))
    .when(col("commissions_9_source") == "ADDON", col("commissions_9_commission_value"))
    .when(col("commissions_10_source") == "ADDON", col("commissions_10_commission_value"))
    .otherwise(0)
)
producer_commissions = (
    when((col("commissions_1_source") == "PRODUCER") & (col("commissions_1_user_email") != commissions_main_email), col("commissions_1_commission_value"))
    .when((col("commissions_2_source") == "PRODUCER") & (col("commissions_2_user_email") != commissions_main_email), col("commissions_2_commission_value"))
    .when((col("commissions_3_source") == "PRODUCER") & (col("commissions_3_user_email") != commissions_main_email), col("commissions_3_commission_value"))
    .when((col("commissions_4_source") == "PRODUCER") & (col("commissions_4_user_email") != commissions_main_email), col("commissions_4_commission_value"))
    .when((col("commissions_5_source") == "PRODUCER") & (col("commissions_5_user_email") != commissions_main_email), col("commissions_5_commission_value"))
    .when((col("commissions_6_source") == "PRODUCER") & (col("commissions_6_user_email") != commissions_main_email), col("commissions_6_commission_value"))
    .when((col("commissions_7_source") == "PRODUCER") & (col("commissions_7_user_email") != commissions_main_email), col("commissions_7_commission_value"))
    .when((col("commissions_8_source") == "PRODUCER") & (col("commissions_8_user_email") != commissions_main_email), col("commissions_8_commission_value"))
    .when((col("commissions_9_source") == "PRODUCER") & (col("commissions_9_user_email") != commissions_main_email), col("commissions_9_commission_value"))
    .when((col("commissions_10_source") == "PRODUCER") & (col("commissions_10_user_email") != commissions_main_email), col("commissions_10_commission_value"))
    .otherwise(0)
)

main_email_commissions = (
    when(col("commissions_1_user_email") == commissions_main_email, col("commissions_1_commission_value"))
    .when(col("commissions_2_user_email") == commissions_main_email, col("commissions_2_commission_value"))
    .when(col("commissions_3_user_email") == commissions_main_email, col("commissions_3_commission_value"))
    .when(col("commissions_4_user_email") == commissions_main_email, col("commissions_4_commission_value"))
    .when(col("commissions_5_user_email") == commissions_main_email, col("commissions_5_commission_value"))
    .when(col("commissions_6_user_email") == commissions_main_email, col("commissions_6_commission_value"))
    .when(col("commissions_7_user_email") == commissions_main_email, col("commissions_7_commission_value"))
    .when(col("commissions_8_user_email") == commissions_main_email, col("commissions_8_commission_value"))
    .when(col("commissions_9_user_email") == commissions_main_email, col("commissions_9_commission_value"))
    .when(col("commissions_10_user_email") == commissions_main_email, col("commissions_10_commission_value"))
    .otherwise(0)
)

dataframe_relevant_data = dataframe_relevant_data.withColumn(
    "purchase_offer_price_coproducers_affiliates",
    commission_sum - (addon_commissions + producer_commissions + main_email_commissions)
)

dataframe_relevant_data = dataframe_relevant_data.withColumn(
    "purchase_addon_tax",
    addon_commissions
)

dataframe_relevant_data = dataframe_relevant_data.withColumn(
    "purchase_offer_price_producer",
    producer_commissions
)

dataframe_relevant_data = dataframe_relevant_data.withColumn(
    "country_code",
    when(col("users_2_user_address_country") == "Afghanistan", "AF")
    .when(col("users_2_user_address_country") == "South Africa", "ZA")
    .when(col("users_2_user_address_country") == "Albania", "AL")
    .when(col("users_2_user_address_country") == "Germany", "DE")
    .when(col("users_2_user_address_country") == "Andorra", "AD")
    .when(col("users_2_user_address_country") == "Angola", "AO")
    .when(col("users_2_user_address_country") == "Anguilla", "AI")
    .when(col("users_2_user_address_country") == "Antarctica", "AQ")
    .when(col("users_2_user_address_country") == "Antigua and Barbuda", "AG")
    .when(col("users_2_user_address_country") == "Netherlands Antilles", "AN")
    .when(col("users_2_user_address_country") == "Saudi Arabia", "SA")
    .when(col("users_2_user_address_country") == "Algeria", "DZ")
    .when(col("users_2_user_address_country") == "Argentina", "AR")
    .when(col("users_2_user_address_country") == "Armenia", "AM")
    .when(col("users_2_user_address_country") == "Aruba", "AW")
    .when(col("users_2_user_address_country") == "Australia", "AU")
    .when(col("users_2_user_address_country") == "Austria", "AT")
    .when(col("users_2_user_address_country") == "Azerbaijan", "AZ")
    .when(col("users_2_user_address_country") == "Bahamas", "BS")
    .when(col("users_2_user_address_country") == "Bahrain", "BH")
    .when(col("users_2_user_address_country") == "Bangladesh", "BD")
    .when(col("users_2_user_address_country") == "Barbados", "BB")
    .when(col("users_2_user_address_country") == "Belarus", "BY")
    .when(col("users_2_user_address_country") == "Belgium", "BE")
    .when(col("users_2_user_address_country") == "Belize", "BZ")
    .when(col("users_2_user_address_country") == "Benin", "BJ")
    .when(col("users_2_user_address_country") == "Bermuda", "BM")
    .when(col("users_2_user_address_country") == "Bolivia", "BO")
    .when(col("users_2_user_address_country") == "Bosnia and Herzegovina", "BA")
    .when(col("users_2_user_address_country") == "Botswana", "BW")
    .when(col("users_2_user_address_country") == "Brasil", "BR")
    .when(col("users_2_user_address_country") == "Brunei", "BN")
    .when(col("users_2_user_address_country") == "Bulgaria", "BG")
    .when(col("users_2_user_address_country") == "Burkina Faso", "BF")
    .when(col("users_2_user_address_country") == "Burundi", "BI")
    .when(col("users_2_user_address_country") == "Bhutan", "BT")
    .when(col("users_2_user_address_country") == "Cape Verde", "CV")
    .when(col("users_2_user_address_country") == "Cameroon", "CM")
    .when(col("users_2_user_address_country") == "Cambodia", "KH")
    .when(col("users_2_user_address_country") == "Canada", "CA")
    .when(col("users_2_user_address_country") == "Kazakhstan", "KZ")
    .when(col("users_2_user_address_country") == "Chad", "TD")
    .when(col("users_2_user_address_country") == "Chile", "CL")
    .when(col("users_2_user_address_country") == "China", "CN")
    .when(col("users_2_user_address_country") == "Cyprus", "CY")
    .when(col("users_2_user_address_country") == "Singapore", "SG")
    .when(col("users_2_user_address_country") == "Colombia", "CO")
    .when(col("users_2_user_address_country") == "Congo", "CG")
    .when(col("users_2_user_address_country") == "North Korea", "KP")
    .when(col("users_2_user_address_country") == "South Korea", "KR")
    .when(col("users_2_user_address_country") == "Ivory Coast", "CI")
    .when(col("users_2_user_address_country") == "Costa Rica", "CR")
    .when(col("users_2_user_address_country") == "Croatia (Hrvatska)", "HR")
    .when(col("users_2_user_address_country") == "Cuba", "CU")
    .when(col("users_2_user_address_country") == "Denmark", "DK")
    .when(col("users_2_user_address_country") == "Djibouti", "DJ")
    .when(col("users_2_user_address_country") == "Dominica", "DM")
    .when(col("users_2_user_address_country") == "Dominican Republic", "DO")
    .when(col("users_2_user_address_country") == "Egypt", "EG")
    .when(col("users_2_user_address_country") == "El Salvador", "SV")
    .when(col("users_2_user_address_country") == "United Arab Emirates", "AE")
    .when(col("users_2_user_address_country") == "Ecuador", "EC")
    .when(col("users_2_user_address_country") == "Eritrea", "ER")
    .when(col("users_2_user_address_country") == "Slovakia", "SK")
    .when(col("users_2_user_address_country") == "Slovenia", "SI")
    .when(col("users_2_user_address_country") == "Spain", "ES")
    .when(col("users_2_user_address_country") == "United States", "US")
    .when(col("users_2_user_address_country") == "Estonia", "EE")
    .when(col("users_2_user_address_country") == "Ethiopia", "ET")
    .when(col("users_2_user_address_country") == "Fiji", "FJ")
    .when(col("users_2_user_address_country") == "Philippines", "PH")
    .when(col("users_2_user_address_country") == "Finland", "FI")
    .when(col("users_2_user_address_country") == "France", "FR")
    .when(col("users_2_user_address_country") == "Gabon", "GA")
    .when(col("users_2_user_address_country") == "Gambia", "GM")
    .when(col("users_2_user_address_country") == "Ghana", "GH")
    .when(col("users_2_user_address_country") == "Georgia", "GE")
    .when(col("users_2_user_address_country") == "Gibraltar", "GI")
    .when(col("users_2_user_address_country") == "Great Britain", "GB")
    .when(col("users_2_user_address_country") == "Greece", "GR")
    .when(col("users_2_user_address_country") == "Grenada", "GD")
    .when(col("users_2_user_address_country") == "Greenland", "GL")
    .when(col("users_2_user_address_country") == "Guadeloupe", "GP")
    .when(col("users_2_user_address_country") == "Guam", "GU")
    .when(col("users_2_user_address_country") == "Guatemala", "GT")
    .when(col("users_2_user_address_country") == "French Guiana", "GF")
    .when(col("users_2_user_address_country") == "Guernsey", "GG")
    .when(col("users_2_user_address_country") == "Guinea", "GN")
    .when(col("users_2_user_address_country") == "Equatorial Guinea", "GQ")
    .when(col("users_2_user_address_country") == "Guinea-Bissau", "GW")
    .when(col("users_2_user_address_country") == "Guyana", "GY")
    .when(col("users_2_user_address_country") == "Haiti", "HT")
    .when(col("users_2_user_address_country") == "Honduras", "HN")
    .when(col("users_2_user_address_country") == "Hong Kong", "HK")
    .when(col("users_2_user_address_country") == "Hungary", "HU")
    .when(col("users_2_user_address_country") == "India", "IN")
    .when(col("users_2_user_address_country") == "Indonesia", "ID")
    .when(col("users_2_user_address_country") == "Iraq", "IQ")
    .when(col("users_2_user_address_country") == "Iran", "IR")
    .when(col("users_2_user_address_country") == "Ireland", "IE")
    .when(col("users_2_user_address_country") == "Iceland", "IS")
    .when(col("users_2_user_address_country") == "Israel", "IL")
    .when(col("users_2_user_address_country") == "Italy", "IT")
    .when(col("users_2_user_address_country") == "Jamaica", "JM")
    .when(col("users_2_user_address_country") == "Japan", "JP")
    .when(col("users_2_user_address_country") == "Jersey", "JE")
    .when(col("users_2_user_address_country") == "Jordan", "JO")
    .when(col("users_2_user_address_country") == "Yugoslavia", "YU")
    .when(col("users_2_user_address_country") == "Kenya", "KE")
    .when(col("users_2_user_address_country") == "Kyrgyzstan", "KG")
    .when(col("users_2_user_address_country") == "Kiribati", "KI")
    .when(col("users_2_user_address_country") == "Kuwait", "KW")
    .when(col("users_2_user_address_country") == "Laos", "LA")
    .when(col("users_2_user_address_country") == "Lesotho", "LS")
    .when(col("users_2_user_address_country") == "Latvia", "LV")
    .when(col("users_2_user_address_country") == "Lebanon", "LB")
    .when(col("users_2_user_address_country") == "Liberia", "LR")
    .when(col("users_2_user_address_country") == "Libya", "LY")
    .when(col("users_2_user_address_country") == "Liechtenstein", "LI")
    .when(col("users_2_user_address_country") == "Lithuania", "LT")
    .when(col("users_2_user_address_country") == "Luxembourg", "LU")
    .when(col("users_2_user_address_country") == "Macao", "MO")
    .when(col("users_2_user_address_country") == "Madagascar", "MG")
    .when(col("users_2_user_address_country") == "Malaysia", "MY")
    .when(col("users_2_user_address_country") == "Malawi", "MW")
    .when(col("users_2_user_address_country") == "Maldives", "MV")
    .when(col("users_2_user_address_country") == "Mali", "ML")
    .when(col("users_2_user_address_country") == "Malta", "MT")
    .when(col("users_2_user_address_country") == "Morocco", "MA")
    .when(col("users_2_user_address_country") == "Martinique", "MQ")
    .when(col("users_2_user_address_country") == "Mauritania", "MR")
    .when(col("users_2_user_address_country") == "Mauritius", "MU")
    .when(col("users_2_user_address_country") == "Mexico", "MX")
    .when(col("users_2_user_address_country") == "Monaco", "MC")
    .when(col("users_2_user_address_country") == "Mongolia", "MN")
    .when(col("users_2_user_address_country") == "Montserrat", "MS")
    .when(col("users_2_user_address_country") == "Mozambique", "MZ")
    .when(col("users_2_user_address_country") == "Moldova", "MD")
    .when(col("users_2_user_address_country") == "Myanmar", "MM")
    .when(col("users_2_user_address_country") == "Namibia", "NA")
    .when(col("users_2_user_address_country") == "Nauru", "NR")
    .when(col("users_2_user_address_country") == "Nepal", "NP")
    .when(col("users_2_user_address_country") == "Nicaragua", "NI")
    .when(col("users_2_user_address_country") == "Niger", "NE")
    .when(col("users_2_user_address_country") == "Nigeria", "NG")
    .when(col("users_2_user_address_country") == "Niue", "NU")
    .when(col("users_2_user_address_country") == "Norway", "NO")
    .when(col("users_2_user_address_country") == "New Zealand", "NZ")
    .when(col("users_2_user_address_country") == "Oman", "OM")
    .when(col("users_2_user_address_country") == "Uganda", "UG")
    .when(col("users_2_user_address_country") == "Uzbekistan", "UZ")
    .when(col("users_2_user_address_country") == "Pakistan", "PK")
    .when(col("users_2_user_address_country") == "Palau", "PW")
    .when(col("users_2_user_address_country") == "Panama", "PA")
    .when(col("users_2_user_address_country") == "Papua New Guinea", "PG")
    .when(col("users_2_user_address_country") == "Paraguay", "PY")
    .when(col("users_2_user_address_country") == "Peru", "PE")
    .when(col("users_2_user_address_country") == "Pitcairn", "PN")
    .when(col("users_2_user_address_country") == "Poland", "PL")
    .when(col("users_2_user_address_country") == "Portugal", "PT")
    .when(col("users_2_user_address_country") == "Puerto Rico", "PR")
    .when(col("users_2_user_address_country") == "Qatar", "QA")
    .when(col("users_2_user_address_country") == "Central African Republic", "CF")
    .when(col("users_2_user_address_country") == "Democratic Republic of the Congo", "CD")
    .when(col("users_2_user_address_country") == "Czech Republic", "CZ")
    .when(col("users_2_user_address_country") == "Romania", "RO")
    .when(col("users_2_user_address_country") == "United Kingdom", "GB")
    .when(col("users_2_user_address_country") == "Russia", "RU")
    .when(col("users_2_user_address_country") == "Rwanda", "RW")
    .when(col("users_2_user_address_country") == "Western Sahara", "EH")
    .when(col("users_2_user_address_country") == "Saint Kitts and Nevis", "KN")
    .when(col("users_2_user_address_country") == "San Marino", "SM")
    .when(col("users_2_user_address_country") == "Saint Pierre and Miquelon", "PM")
    .when(col("users_2_user_address_country") == "Saint Vincent and the Grenadines", "VC")
    .when(col("users_2_user_address_country") == "Saint Lucia", "LC")
    .when(col("users_2_user_address_country") == "Samoa", "WS")
    .when(col("users_2_user_address_country") == "American Samoa", "AS")
    .when(col("users_2_user_address_country") == "Saint Helena", "SH")
    .when(col("users_2_user_address_country") == "Sao Tome and Principe", "ST")
    .when(col("users_2_user_address_country") == "Senegal", "SN")
    .when(col("users_2_user_address_country") == "Seychelles", "SC")
    .when(col("users_2_user_address_country") == "Sierra Leone", "SL")
    .when(col("users_2_user_address_country") == "Zimbabwe", "ZW")
    .when(col("users_2_user_address_country") == "Serbia", "RS")
    .when(col("users_2_user_address_country") == "Singapore", "SG")
    .when(col("users_2_user_address_country") == "Syria", "SY")
    .when(col("users_2_user_address_country") == "Somalia", "SO")
    .when(col("users_2_user_address_country") == "Sri Lanka", "LK")
    .when(col("users_2_user_address_country") == "Swaziland", "SZ")
    .when(col("users_2_user_address_country") == "South Africa", "ZA")
    .when(col("users_2_user_address_country") == "Sudan", "SD")
    .when(col("users_2_user_address_country") == "South Sudan", "SS")
    .when(col("users_2_user_address_country") == "Sweden", "SE")
    .when(col("users_2_user_address_country") == "Switzerland", "CH")
    .when(col("users_2_user_address_country") == "Suriname", "SR")
    .when(col("users_2_user_address_country") == "Slovakia", "SK")
    .when(col("users_2_user_address_country") == "Slovenia", "SI")
    .when(col("users_2_user_address_country") == "Tajikistan", "TJ")
    .when(col("users_2_user_address_country") == "Thailand", "TH")
    .when(col("users_2_user_address_country") == "Tanzania", "TZ")
    .when(col("users_2_user_address_country") == "Togo", "TG")
    .when(col("users_2_user_address_country") == "Tokelau", "TK")
    .when(col("users_2_user_address_country") == "Tonga", "TO")
    .when(col("users_2_user_address_country") == "Trinidad and Tobago", "TT")
    .when(col("users_2_user_address_country") == "Tunisia", "TN")
    .when(col("users_2_user_address_country") == "Turkey", "TR")
    .when(col("users_2_user_address_country") == "Turkmenistan", "TM")
    .when(col("users_2_user_address_country") == "Tuvalu", "TV")
    .when(col("users_2_user_address_country") == "Ukraine", "UA")
    .when(col("users_2_user_address_country") == "Uruguay", "UY")
    .when(col("users_2_user_address_country") == "Vanuatu", "VU")
    .when(col("users_2_user_address_country") == "Venezuela", "VE")
    .when(col("users_2_user_address_country") == "Vietnam", "VN")
    .when(col("users_2_user_address_country") == "Wallis and Futuna", "WF")
    .when(col("users_2_user_address_country") == "Yemen", "YE")
    .when(col("users_2_user_address_country") == "Zambia", "ZM")
    .otherwise("00")
    )

#CODE FOR RELEVANT DATA TABLE:
#Script generated for node All Data PostgreSQL
#dataframe_relevant_data = TratarRelevantData_node.toDF()
batch_data_relevant_data = dataframe_relevant_data.collect()

for row in batch_data_relevant_data:
    transaction = row['transaction']
    cursor.execute("""SELECT * FROM public.tabela_hotmart_relevant_data
        WHERE transaction = %s""", (transaction,))
    existing_record = cursor.fetchone()

    if existing_record:
        cursor.execute("""
            UPDATE public.tabela_hotmart_relevant_data
            SET
                purchase_payment_type = %s,
                purchase_payment_installments_number = %s,
                purchase_hotmart_fee_total = %s,
                purchase_hotmart_fee_percentage = %s,
                purchase_warranty_expire_date = %s,
                purchase_tracking_source = %s,
                purchase_recurrency_number = %s,
                purchase_offer_payment_mode = %s,
                purchase_order_date = %s,
                purchase_status = %s,
                buyer_email = %s,
                product_name = %s,
                users_1_user_name = %s,
                users_2_user_name = %s,
                users_2_user_address_city = %s,
                users_2_user_address_state = %s,
                users_2_user_address_country = %s,
                base_currency_code = %s,
                base_value_converted = %s,
                total_value_converted = %s,
                purchase_offer_price_main_user = %s,
                purchase_offer_currency_code = %s,
                purchase_offer_price_coproducers_affiliates = %s,
                purchase_addon_tax = %s,
                purchase_offer_price_producer = %s,
                country_code = %s
            WHERE transaction = %s
            """,
            (
                row['purchase_payment_type'],
                row['purchase_payment_installments_number'],
                row['purchase_hotmart_fee_total'],
                row['purchase_hotmart_fee_percentage'],
                row['purchase_warranty_expire_date'],
                row['purchase_tracking_source'],
                row['purchase_recurrency_number'],
                row['purchase_offer_payment_mode'],
                row['purchase_order_date'],
                row['purchase_status'],
                row['buyer_email'],
                row['product_name'],
                row['users_1_user_name'],
                row['users_2_user_name'],
                row['users_2_user_address_city'],
                row['users_2_user_address_state'],
                row['users_2_user_address_country'],
                row['base_currency_code'],
                row['base_value_converted'],
                row['total_value_converted'],
                row['purchase_offer_price_main_user'],
                row['purchase_offer_currency_code'],
                row['purchase_offer_price_coproducers_affiliates'],
                row['purchase_addon_tax'],
                row['purchase_offer_price_producer'],
                row['country_code'],
                transaction
            )
        )
    else:
        cursor.execute("""
            INSERT INTO public.tabela_hotmart_relevant_data (
                transaction,
                purchase_payment_type,
                purchase_payment_installments_number,
                purchase_hotmart_fee_total,
                purchase_hotmart_fee_percentage,
                purchase_warranty_expire_date,
                purchase_tracking_source,
                purchase_recurrency_number,
                purchase_offer_payment_mode,
                purchase_order_date,
                purchase_status,
                buyer_email,
                product_name,
                users_1_user_name,
                users_2_user_name,
                users_2_user_address_city,
                users_2_user_address_state,
                users_2_user_address_country,
                base_currency_code,
                base_value_converted,
                total_value_converted,
                purchase_offer_price_main_user,
                purchase_offer_currency_code,
                purchase_offer_price_coproducers_affiliates,
                purchase_addon_tax,
                purchase_offer_price_producer,
                country_code
            ) VALUES (

%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            """,
            (
                transaction,
                row['purchase_payment_type'],
                row['purchase_payment_installments_number'],
                row['purchase_hotmart_fee_total'],
                row['purchase_hotmart_fee_percentage'],
                row['purchase_warranty_expire_date'],
                row['purchase_tracking_source'],
                row['purchase_recurrency_number'],
                row['purchase_offer_payment_mode'],
                row['purchase_order_date'],
                row['purchase_status'],
                row['buyer_email'],
                row['product_name'],
                row['users_1_user_name'],
                row['users_2_user_name'],
                row['users_2_user_address_city'],
                row['users_2_user_address_state'],
                row['users_2_user_address_country'],
                row['base_currency_code'],
                row['base_value_converted'],
                row['total_value_converted'],
                row['purchase_offer_price_main_user'],
                row['purchase_offer_currency_code'],
                row['purchase_offer_price_coproducers_affiliates'],
                row['purchase_addon_tax'],
                row['purchase_offer_price_producer'],
                row['country_code']
            )
        )
        
conn.commit()
cursor.close()
conn.close()
job.commit()
