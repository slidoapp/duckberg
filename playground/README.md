## DuckBerg playground

You can easily spin up the playground environment by docker compose. The environment consist of
- [Spark Iceberg](https://github.com/tabular-io/docker-spark-iceberg) for Iceberg data initialisation through Spark 
- [Rest Iceberg Catalog](https://github.com/tabular-io/iceberg-rest-image) for storing Iceberg metadata 
- [Minio](https://min.io/) as an object storage that is S3 compatible. 
- Jupyter notebook with preinstalled Duckberg to run examples and experiments

Start the environment by

```shell
docker-compose up -d
```

The initial run could take additional time for jupyter docker image build. Then you can access

### Iceberg data init
Once all the containers have been initiated run the [Spark Iceberg Jupyter notebook](http://localhost:8889/notebooks/000%20Init%20Iceberg%20data.ipynb) that will
init the Iceberg data and catalog.

### Duckberg playground

Navigate to [localhost:8888](http://localhost:8888). Then select example Jupyter notebook you want to run and enjoy Duckberg!

