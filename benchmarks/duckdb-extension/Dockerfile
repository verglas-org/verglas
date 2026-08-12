FROM python:3.12-slim
RUN pip install --no-cache-dir duckdb==1.5.5 pyarrow==19.0.1
WORKDIR /bench
COPY benchmark.py /bench/benchmark.py
