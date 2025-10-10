#!/bin/bash
docker run --rm \
  -e POSTGRES_DB=main \
  -e POSTGRES_USER=user \
  -e POSTGRES_PASSWORD=abc123 \
  -p 5432:5432 \
  -v benchmark_pg_data:/var/lib/postgresql/data \
  postgres
