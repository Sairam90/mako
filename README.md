# mako


- Download and extract from the **zip file** and cd into location of docker-compose file
- start the container using docker-compose up -d
- the /test-files/ path in the docker container has the files 
- enter the docker container by using  - docker container exec -it <master-spark-container-name> /bin/bash
- Execute the pyspark script using - bin/spark-submit /test-files/pyspark.py
- Result_final.csv is the result I have produced by running on Databricks
- **SQL Logic** file added

Notes:
- I have assumed the file storage for the json files in a production setting would be in AWS S3
- Read and create temp views from the files using Pyspark
- Use SPARK SQL to transform and produce final result
- If the two input files can be placed in a S3 bucket the file paths would have to be changed in the pyspark.py file and can be run via databricks too
