FROM python:3.12-slim

RUN pip install --no-cache-dir boto3==1.40.15 duckdb==1.5.5 && \
    python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL httpfs'); c.execute('INSTALL quack')"
WORKDIR /bench
COPY benchmarks/duckdb-object-store/benchmark.py /bench/benchmark.py
