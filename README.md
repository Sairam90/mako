# mako
mako files

- Download all files and cd into location of docker-compose file
- start the container using docker-compose up -d
- Execute the pyspark script
- Result_final.csv is the result I have produced by running on Databricks

Notes:
- I have assumed the file storage for the json files in a production setting would be in AWS S3
- Read and create temp views from the files using Pyspark
- Use SPARK SQL to transform and produce final result
- If the two input files can be placed in a S3 bucket the file paths would have to be changed in the pyspark.py file and can be run via databricks too
