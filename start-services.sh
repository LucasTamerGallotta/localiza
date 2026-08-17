#!/bin/bash

# Script to start all services

echo "🚀 Starting MinIO and Spark services..."
docker-compose -f docker-compose.minio-spark.yml up -d

echo ""
echo "⏳ Waiting for services to be ready..."
sleep 10

echo ""
echo "🔍 Checking service status..."
docker-compose -f docker-compose.minio-spark.yml ps

echo ""
echo "✅ Services started successfully!"
echo ""
echo "📌 Service URLs:"
echo "   MinIO Console:     http://localhost:9001 (minioadmin/minioadmin)"
echo "   MinIO API:         http://localhost:9000"
echo "   Spark Master UI:   http://localhost:8088"
echo "   Spark Worker UI:   http://localhost:8089"
echo ""

echo "🚀 Starting Airflow..."
astro dev start

echo ""
echo "✨ All services are running!"
echo ""
echo "📌 Complete service URLs:"
echo "   Airflow UI:        http://localhost:8080 (admin/admin)"
echo "   MinIO Console:     http://localhost:9001 (minioadmin/minioadmin)"
echo "   Spark Master UI:   http://localhost:8088"
echo "   Spark Worker UI:   http://localhost:8089"
echo ""
echo "To trigger the DAG:"
echo "  1. Go to http://localhost:8080"
echo "  2. Find 'spark_minio_processing' DAG"
echo "  3. Toggle it ON and click the Play button"