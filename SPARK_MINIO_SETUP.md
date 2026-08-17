# Spark + MinIO + Airflow Setup Guide

## Overview
This setup provides a complete data processing pipeline with:
- **Apache Airflow** for orchestration (running via Astronomer)
- **Apache Spark** for distributed data processing (separate Docker containers)
- **MinIO** for S3-compatible object storage (separate Docker container)

## Architecture

```
┌─────────────────────┐         ┌──────────────────────┐
│   Airflow           │         │  MinIO & Spark       │
│   (Astro Dev)       │ ──────> │  (Separate Docker)   │
│   Port: 8080        │         │  Ports: 9000-9001,   │
└─────────────────────┘         │  7077, 8088-8089     │
                                └──────────────────────┘
```

## Quick Start

### 1. Start MinIO and Spark services first
```bash
# Start MinIO and Spark containers
docker-compose -f docker-compose.minio-spark.yml up -d

# Wait for services to initialize
sleep 10

# Verify services are running
docker-compose -f docker-compose.minio-spark.yml ps
```

### 2. Start Airflow
```bash
astro dev start
```

### Alternative: Use the convenience script
```bash
# Start everything with one command
./start-services.sh

# Stop everything
./stop-services.sh
```

## Service URLs

Once all services are running, access them at:

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| MinIO API | http://localhost:9000 | minioadmin / minioadmin |
| Spark Master UI | http://localhost:8088 | - |
| Spark Worker UI | http://localhost:8089 | - |

## Running the Data Pipeline

### 1. Verify services are running
Check that all services are accessible via their web interfaces.

### 2. Trigger the DAG
1. Go to Airflow UI: http://localhost:8080
2. Find the DAG: `spark_minio_processing`
3. Toggle it ON
4. Click the "Play" button to trigger manually

### 3. Monitor the pipeline
The DAG will:
1. **Check MinIO Connection** - Verify MinIO is accessible and buckets exist
2. **Upload Sample Data** - Create and upload test CSV data to input-bucket
3. **Submit Spark Job** - Process data using Spark
4. **Verify Output** - Check processed data in output-bucket

### 4. View results
- Check MinIO Console to see input and output files
- View Spark job execution in Spark Master UI
- Monitor task logs in Airflow UI

## File Structure

```
.
├── docker-compose.minio-spark.yml  # MinIO & Spark services
├── spark/
│   └── Dockerfile                   # Spark container with dependencies
├── spark-jobs/
│   └── process_data.py             # PySpark processing script
├── dags/
│   └── spark_minio_processing_dag.py  # Airflow DAG
├── include/
│   └── upload_test_data.py         # Helper to create test data
├── start-services.sh                # Start all services
└── stop-services.sh                 # Stop all services
```

## Data Processing Details

The PySpark job (`spark-jobs/process_data.py`):
- Reads CSV/JSON data from MinIO input-bucket
- Adds metadata columns (timestamp, job name)
- Transforms string columns to uppercase
- Calculates derived fields
- Writes processed data to output-bucket

## Customization

### Modify processing logic
Edit `spark-jobs/process_data.py` to change how data is processed.

### Add new Spark jobs
1. Create new `.py` files in `spark-jobs/`
2. Update the DAG to call your new script

### Change bucket names
Update bucket names in:
- `docker-compose.minio-spark.yml` (minio-init service)
- `dags/spark_minio_processing_dag.py`

### Adjust Spark resources
Edit `docker-compose.minio-spark.yml`:
```yaml
SPARK_WORKER_CORES: 2      # CPU cores
SPARK_WORKER_MEMORY: 2g    # Memory allocation
```

## Uploading Your Own Data

### Via MinIO Console
1. Go to http://localhost:9001
2. Login with minioadmin/minioadmin
3. Navigate to `input-bucket`
4. Upload your CSV or JSON files

### Via Python script
```bash
# From Airflow container
astro dev bash
python /usr/local/airflow/include/upload_test_data.py
```

### Via AWS CLI
```bash
# Configure AWS CLI for MinIO
aws configure set aws_access_key_id minioadmin
aws configure set aws_secret_access_key minioadmin

# Upload file
aws --endpoint-url http://localhost:9000 \
    s3 cp yourfile.csv s3://input-bucket/
```

## Troubleshooting

### MinIO Connection Issues
```bash
# Check MinIO is running
docker ps | grep minio

# View MinIO logs
docker logs minio

# Test connection
curl http://localhost:9000/minio/health/live
```

### Spark Issues
```bash
# Check Spark containers
docker ps | grep spark

# View Spark Master logs
docker logs spark-master

# View Spark Worker logs
docker logs spark-worker

# Check if worker is registered
curl http://localhost:8088
```

### Airflow Task Failures
1. Click on the failed task in Airflow UI
2. Select "Log" to view detailed error messages
3. Check if external services are accessible

### Network Issues
If Airflow can't reach MinIO/Spark:
```bash
# Check Docker networks
docker network ls

# Inspect network
docker network inspect spark-minio-network

# Restart services
./stop-services.sh
./start-services.sh
```

## Cleanup

### Stop all services
```bash
./stop-services.sh
```

### Remove all data and volumes
```bash
# Stop services first
./stop-services.sh

# Remove MinIO and Spark volumes
docker-compose -f docker-compose.minio-spark.yml down -v

# Clean Airflow
astro dev kill
```

## Performance Tuning

### Spark Configuration
Edit spark-submit parameters in the DAG:
- `spark.executor.memory`: Memory per executor
- `spark.executor.cores`: Cores per executor
- `spark.sql.shuffle.partitions`: Number of partitions for shuffles

### MinIO Performance
- Use multiple MinIO nodes for production
- Configure appropriate storage backend
- Tune connection pool sizes in Spark

## Next Steps

1. **Add more workers**: Scale Spark by adding more worker containers
2. **Implement partitioning**: Partition data by date/category for better performance
3. **Add data validation**: Implement schema validation in Spark jobs
4. **Create more DAGs**: Build additional pipelines for different data sources
5. **Set up monitoring**: Add Prometheus/Grafana for metrics

## Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [MinIO Documentation](https://docs.min.io/)
- [Astronomer Documentation](https://docs.astronomer.io/)
- [Airflow Documentation](https://airflow.apache.org/docs/)