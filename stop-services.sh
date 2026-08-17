#!/bin/bash

# Script to stop all services

echo "🛑 Stopping Airflow..."
astro dev stop

echo ""
echo "🛑 Stopping MinIO and Spark services..."
docker-compose -f docker-compose.minio-spark.yml down

echo ""
echo "✅ All services stopped successfully!"