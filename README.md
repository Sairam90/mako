# mako
mako files

- Download all files and cd into location of docker-compose file
- start the container using docker-compose up -d
- Execute the pyspark script
- Result_final.csv is the result I have produced by running on Databricks

Notes:
- I have assumed the file storage for the json files in a production setting would be in AWS S3
- Read and create temp views from the files using Pyspark
- Use SPARL SQL to transform and produce final result
