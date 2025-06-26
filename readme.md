### Google Cloud Composer 3 (Apache Airflow) 

Manage dataproc serverless jobs.

We use this [spark](https://github.com/cloudymoma/dataproc-scala) job in this
sample. You could build your own jars.

Before you start, update the `makefile` and `deploy.sh` with your own
environment settings accordingly.

`make composer` - Create a Composer 3 in your project

`make deploy` - Deploy the DAG to composer

