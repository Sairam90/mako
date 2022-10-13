# mako
mako files

1.Download all files and cd into location of docker-compose file
2.start the container using docker-compose up -d
3.Execute the pyspark script
4.Result_final.csv is the result I have produced by running on Databricks

Notes:
1.I have assumed the file storage for the json files in a production setting would be in AWS S3
2.Read and create temp views from the files using Pyspark
3.Use SPARL SQL to transform and produce final result
