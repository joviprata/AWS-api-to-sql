![API to Postgres Diagram](https://github.com/user-attachments/assets/e3afb286-a19a-4852-9f33-210784c13c91)
# How to store External API Data in Postgres with AWS

Data engineering step-by-step process on how to use AWS tools to get data from an API, send them to an S3 Bucket using a Lambda function, do ETL processes with a Glue Job and send to a PostgreSQL database with an Upsert approach. This repository is under development.

First and foremost, I recommend using Ohio's region (us-east-2) for all of the AWS services in your project, since it's one of the cheapest regions. I made the mistake of using another region that's closer to where I live because I thought it would be more expensive to run services in another country, but that wasn't the case. Switching regions cut costs drastically for me, and it didn't seem to affect the performance of the services.<br/>
Also, I used to run an AWS Redshift database, but switched to RDS Postgres, since it's cheaper and it serves well for my needs. Redshift implementation on AWS Glue is easier to do than RDS Postgres when you need to do an Upsert approach (overwriting lines with same ID value as a new one and inserting other lines, a method that avoids duplicate lines). AWS Glue Visual Job doesn't have an out-of-the-box function for Postgres like Redshift does, so in order to connect the Glue Job to RDS after you set all the nodes in the visual editor, you need to convert it to a script-only Job and use external Python libraries to do the Upsert. It's a harder process but it's worth the cost reduction.
## S3 Bucket
### Create bucket
Here are my configurations:
- ACLs disabled
- Block all public access
- Enable Bucket Versioning (good to see the progress of your tables as time goes on)
- No tags are needed
- Encryption type: Server-side encryption with Amazon S3 managed keys (SSE-S3)
- Enable Bucket Key
- Disable Object Lock

## IAM Roles and policies
### Create user
- Add permissions - Attach the following policies directly to the user:
    - AdministratorAccess
    - AmazonAthenaFullAccess
    - AmazonS3FullAccess
    - AWSGlueConsoleFullAccess
    - AWSGlueServiceRole
    - AWSLakeFormationCrossAccountManager
    - AWSLakeFormationDataAdmin
    - CloudWatchLogsReadOnlyAccess
- No inline policies are needed

### Create role
ETLlambdaAccessRole:
- Trusted entity type - AWS account - this account
- MFA and external ID not required
- Attach these policies:
  - AdministratorAccess
  - AmazonS3FullAccess
  - AWSGlueServiceRole
  - AWSLambdaExecute
  - CloudWatchFullAccess

In the trust relationships tab, edit the trust policy and paste this code:

		{
		    "Version": "2012-10-17",
		    "Statement": [
		        {
		            "Sid": "Statement1",
		            "Effect": "Allow",
		            "Principal": {
		                "Service": [
		                    "lakeformation.amazonaws.com",
		                    "events.amazonaws.com",
		                    "apigateway.amazonaws.com",
		                    "osis-pipelines.amazonaws.com",
		                    "redshift.amazonaws.com",
		                    "ecs.amazonaws.com",
		                    "rds.amazonaws.com",
		                    "transfer.amazonaws.com",
		                    "glue.amazonaws.com",
		                    "cloudformation.amazonaws.com",
		                    "lambda.amazonaws.com",
		                    "dms.amazonaws.com",
		                    "datapipeline.amazonaws.com",
		                    "textract.amazonaws.com",
		                    "delivery.logs.amazonaws.com",
		                    "ecs-tasks.amazonaws.com",
		                    "logs.amazonaws.com",
		                    "cloudsearch.amazonaws.com"
		                ],
		                "AWS": "arn:aws:iam::<YOUR_ACCOUNT_ID>:root"
		            },
		            "Action": "sts:AssumeRole"
		        }
		    ]
		}



	
## Gathering data from API and uploading to S3 Bucket
This process consists of writing code, testing it on an IDE like VSCode, sending that code into AWS Lambda with some minor adjustments, and automating it EventBridge.
I used VSCode to write my code. Basic requirements for this are:
- Refer to your API documentation on how to fetch data. For this case, I used Hotmart's API, a sales website
  - Hotmart's API provides response.json() data in the form similar to a heterogenic Python dictionary: a dictionary with other sub-dictionaries, integers, floats or strings. I implemented a DFS (Depth-First-Search) function to reorganize this structure into a dictionary where the keys are paths to the value it receives. Example: {"dictionary_1_sub-dictionary_1": "Value 1", "dictionary_1_sub-dictionary_2": "Value 2"}
- Interpret the API data and do minor transformations if necessary. However, keep in mind that these changes should be minimal: it is easier and more organized to do ETL changes later with AWS Glue
- Create a CSV file to store the resulting data
- Download VSCode's AWS extension to login with your AWS credentials and be able to test the code outside of Lambda
  - It is also common practice to create a .env file to store sensitive information outside of your code, such as username, password, among other things
- Upload the CSV into S3 

## Automating with AWS Lambda and CloudWatch
After the code is done, create a Lambda function. These are my configurations:
- Use a blueprint
- Hello world function (python3.10)
- Name it.
- Use an exising role - ETLlambdaAccessRole (created previously)
- Create function.

- Test the blueprint code. Give the event test a name (like "test"), keep it private, use the hello-world template and delete all Event JSON keys, leaving only "{}". Delete the sample code, paste this, deploy and test:

		import json
		print('Loading function')
		
		def lambda_handler(event, context):
		    #print("Received event: " + json.dumps(event, indent=2))
		    print("Wassup world")
		    return "TEST"
		    #raise Exception('Something went wrong')

It should work fine. Now,
- Delete __pycache__
- Rename lambda_function.py to send_<CUSTOM_NAME>_s3.py. Scroll down to Runtime settings, edit, and rename handler to send_<CUSTOM_NAME>_s3.lambda_handler
- There's no need for a requirements.txt file.
- Scroll down to Layers - Add a layer.
- If you used Pandas in your code, add AWS layer AWSSDKPandas-Python310. The version that I picked was the most recent and only available one, version 15. For other libraries used in your code, refer to this [tutorial](https://youtu.be/ddSoj_ihLfo?si=huewQzZas46Av-7p). Do the same process as the tutorial, just changing "requests" to the library you want to install. (Obs: if you want to make sql queries in your function, use pg8000 instead of psycopg2).
- Paste your code on the editor. The main part of your code must be inside the function lambda_handler(event, context).

If you want to automate the function, in the function overview click "Add trigger". 
- Select EventBridge (CloudWatch Events) as source.
- Create a new rule. Give it a name.
- Usually I make it run at a rate of 7 minutes, but it's up to you how much time you need.

## Creating a PostgreSQL-based RDS database
Go to AWS RDS. In the Databases page, click "create database". These are my configurations:
- Use Standard Create
- Choose PostgreSQL for engine type
- Choose engine version 13.14-R2 (this was the latest version I found that works with AWS Glue)
- RDS Extended Support disabled
- Templates - Free tier
- Give your DB instance a name (DB instance identifier, or DB instance ID).
- Credentials management - Self managed - write an username and master password for your database
- Use db.t3.micro for instance configuration
- Enable public access
- 7 days (free tier) Performance Insights
- Give initial database name.
- Backup retention period - 7 days
- Leave everything else as its default settings, and create database.
- Wait for RDS database to be completely created, then open it, and copy its endpoint.
- Open Postgres
- On Servers, click "Add New Server"
- Give it a name.
- On the Connection page, paste the endpoint you copied into the Host name/address field
- Write your username and password, then save.

Now on AWS Services, go to VPC:
- Click on Security groups
- Click on the security group used by your RDS (usually the default one)
- Edit inbound rules
- Add these rules:
  - Type: All traffic; Source: Anywhere-IPv4
  - Type: HTTP; Source: Anywhere-IPv4
  - Type: All TCP; Source: Anywhere-IPv4
 
With all that done, you should be able to connect to server normally.

## Creating AWS Glue Job for ETL process
Use the Visual Job editor first, do any ETL processes you need to do to the CSV files on your bucket, and name each node accordingly (this will be important later). Do the whole process as if you were normally on an ETL Visual Job, but don't use RDS target nodes yet (those will be created in the Script Editor). Instead of sending to RDS, I recommend sending to another S3 Bucket for now. Run the job. Running the Job will automatically add any necessary Python Libraries for the common Glue tools used in your Job, and it's easier to detect and fix errors than on the Script Editor. After you verify that your Job is running well, your Target Bucket is getting properly formatted data, and you're not facing any errors other than related to Target Nodes (considering those will be corrected later), clone your Job and convert the cloned Job into Script Editor only. Cloning the job allows you to see all the nodes that are leading to your target in the Visual Job when editing the cloned Script-only job.

To Upsert files into Postgres, you need to download [pg8000](https://pypi.org/project/pg8000/), [asn1crypto](https://pypi.org/project/asn1crypto/) and [scramp](https://pypi.org/project/scramp/) .whl files. Upload them to a S3 Bucket. I recommend creating one just for Python Libraries, and naming it Python Libraries or something like that. In your Glue Job, go to Job details and scroll down to Libraries. If you used any tools in your Job (such as Format Timestamp, Derived Column, etc), you should see them listed in the Python library path. Add the URI path to your files there. The paths must be separated by a single comma (,) and nothing more (example: s3://<YOUR_BUCKET>/pg8000-1.24.2-py3-none-any.whl,s3://<YOUR_BUCKET>/python-libs/scramp-1.4.5-py3-none-any.whl). Although you only import pg8000 in your code, asn1crypto and scramp files may be required to run pg8000 properly.

Go to Job details - Advanced properties - Job parameters, and add any parameters that contain sensitive information, adding "--" before naming them. For this case, I used the following parameters: --HOST, --DATABASE, --USER and --PASSWORD. In the args variable in the code, add the parameters you'll use after 'JOB_NAME':

	args = getResolvedOptions(sys.argv, ['JOB_NAME',
	'HOST', 'DATABASE', 'USER', 'PASSWORD'])
 
Establish a connection with Postgres using pg8000. Use the parameters like so:
 
	conn = pg8000.connect(
 		host = args['HOST'],
   		database = args['DATABASE'],
     	user = args['USER'],
       	password = args['PASSWORD']
	)
  	cursor = conn.cursor()

Next, scroll down to the end of the code. This is where naming the nodes in the Visual Job Editor come in handy: from the generated script you need to identify the last node that would be connected to RDS. Copy its variable name and write the following code:
 	
	dataframe_YOUR_TABLE = LAST_NODE_VARIABLE.toDF()
	batch_data_YOUR_TABLE = dataframe_YOUR_TABLE.collect()
	
	for row in batch_data_YOUR_TABLE
		key = row['key']
	    	cursor.execute("""SELECT * FROM public.YOUR_POSTGRES_TABLE
	        	WHERE key = %s""", (key,))
	    	existing_record = cursor.fetchone()

	if existing_record:
 		cursor.execute("""
   			UPDATE public.YOUR_POSTGRES_TABLE
      		SET
	 			COLUMN_1 = %s,
     			COLUMN_2 = %s,
	 			...
     		WHERE key = %s
		""",
   			(
      		row['COLUMN_1'],
	  		row['COLUMN_2'],
      		...,
	  		key
      		)
	 	)
   	else:
    	cursor.execute("""
      		INSERT INTO public.YOUR_POSTGRES_TABLE (
	 		COLUMN_1,
     		COLUMN_2,
	 		...
     		) VALUES (
				%s, %s, ...
   				)
      			""",
	 		(
    		row['COLUMN_1'],
			row['COLUMN_2'],
    		...
			)
   		)

Any type of column value (string, float, integer...) works when using %s. The database still gets the values in the right data type. The process of writing each column is tedious, and so far I haven't found out a way to make it simpler. However, it is possible to automate it by making a "code generator" that given the inputs of the variables (such as key, list of columns, etc) prints the resulting code for you to copy and paste on the script.
 	
At the very end of the code, commit the connection, close the cursor and close the connection before committing the job. Like so:

	conn.commit()
	cursor.close()
	conn.close()
	job.commit()

### Files description:

...

This repository was possible with the help of many coders and their tutorials on YouTube, Linkedin, and Stack Overflow: [Anything2Cloud](https://youtu.be/xqxFWB5BD0o?si=FewMLDPzv-6jkfOe), [Yogesh Vats](https://www.linkedin.com/pulse/using-external-python-library-aws-glue-etl-job-postgresql-yogesh-vats/), among others.
