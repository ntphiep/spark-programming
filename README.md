# Testing Spark
This repo is to improve my knowledge about Apache Spark

## Notes

There are many ways to setup a Spark cluster at local using Docker compose. In this repo, I will try some simple ways.

### Josephmachado's way
This way using docker-compose file with 1 master and 2 worker, 1 upstream database (Postgres) and a File stogare (MinIO)


You need to add this environment variable to the `.env` file

```bash
SPARK_NO_DAEMONIZE=true
```

After that, build the image for *spark-master* first

```bash
docker compose build spark-master
```

Then start the containers
```bash
docker compose up --build -d --scale spark-worker=2
```