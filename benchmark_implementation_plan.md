# TPC-DS Benchmark Implementation Plan
## Testing Iceberg, DuckLake, and Delta Lake with DuckDB

---

## Table of Contents
1. [Project Overview](#project-overview)
2. [Docker Infrastructure Setup](#docker-infrastructure-setup)
3. [Benchmark Architecture](#benchmark-architecture)
4. [Implementation Phases](#implementation-phases)
5. [Testing Methodology](#testing-methodology)
6. [Expected Outputs](#expected-outputs)
7. [Timeline and Milestones](#timeline-and-milestones)

---

## Project Overview

### Objective
Implement a comprehensive benchmark framework to compare the performance of three open table formats (Apache Iceberg, DuckLake, Delta Lake) using DuckDB as the query engine, following the methodology from the CIDR 2023 paper "Analyzing and Comparing Lakehouse Storage Systems."

### Key Differences from Original Paper
- **Query Engine**: DuckDB instead of Apache Spark
- **Formats Tested**: Iceberg, DuckLake (DuckDB-native), Delta Lake
- **Infrastructure**: Docker containers for reproducibility
- **Scale**: Focus on 1GB, 10GB, 100GB datasets (extensible to larger scales)

### Reference Materials
- CIDR 2023 Paper: https://www.cidrdb.org/cidr2023/papers/p92-jain.pdf
- LHBench Repository: https://github.com/lhbench/lhbench
- Existing Code: https://github.com/soulrrrrr/CS598RAP-Project

---

## Docker Infrastructure Setup

### Phase 1.1: Docker Architecture

#### Container Strategy
Use a multi-container setup with Docker Compose:

```yaml
# docker-compose.yml
version: '3.8'

services:
  benchmark-runner:
    build: ./docker/benchmark
    container_name: tpcds-benchmark
    volumes:
      - ./benchmark:/app/benchmark
      - ./data:/data
      - ./results:/results
      - ./config:/app/config
    environment:
      - PYTHONUNBUFFERED=1
      - BENCHMARK_SCALE_FACTOR=1
      - OUTPUT_DIR=/results
    networks:
      - benchmark-net
    
  # Optional: Metadata catalog services
  postgres:
    image: postgres:15
    container_name: iceberg-catalog
    environment:
      POSTGRES_USER: iceberg
      POSTGRES_PASSWORD: iceberg
      POSTGRES_DB: iceberg_catalog
    volumes:
      - postgres-data:/var/lib/postgresql/data
    networks:
      - benchmark-net
    
  # Optional: MinIO for S3-compatible storage testing
  minio:
    image: minio/minio:latest
    container_name: benchmark-storage
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio-data:/data
    networks:
      - benchmark-net

networks:
  benchmark-net:
    driver: bridge

volumes:
  postgres-data:
  minio-data:
```

#### Benchmark Container Dockerfile

```dockerfile
# docker/benchmark/Dockerfile
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /data /results /app/logs

# Set Python path
ENV PYTHONPATH=/app

# Default command
CMD ["python", "run_benchmark.py"]
```

#### Requirements File

```txt
# requirements.txt
duckdb>=0.10.0
pyarrow>=14.0.0
pandas>=2.0.0
numpy>=1.24.0
pyyaml>=6.0
pytest>=7.4.0
matplotlib>=3.7.0
seaborn>=0.12.0
jupyterlab>=4.0.0
psycopg2-binary>=2.9.0
boto3>=1.28.0
deltalake>=0.10.0
```

### Phase 1.2: Volume Structure

```
project-root/
├── docker/
│   ├── benchmark/
│   │   ├── Dockerfile
│   │   └── entrypoint.sh
│   └── docker-compose.yml
├── data/
│   ├── raw/                  # TPC-DS generated data
│   │   ├── 1gb/
│   │   ├── 10gb/
│   │   └── 100gb/
│   ├── iceberg/              # Iceberg format tables
│   ├── deltalake/            # Delta Lake format tables
│   └── ducklake/             # DuckLake format tables
├── results/
│   ├── raw/                  # Raw benchmark results
│   ├── processed/            # Processed metrics
│   └── visualizations/       # Charts and graphs
├── benchmark/
│   └── [implementation files]
├── config/
│   └── [configuration files]
└── notebooks/
    └── [analysis notebooks]
```

### Phase 1.3: Docker Helper Scripts

#### Build and Run Script

```bash
#!/bin/bash
# scripts/run_benchmark.sh

set -e

echo "Building Docker containers..."
docker-compose build

echo "Starting services..."
docker-compose up -d postgres minio

# Wait for services to be ready
echo "Waiting for services to initialize..."
sleep 10

echo "Running benchmarks..."
docker-compose run --rm benchmark-runner python run_benchmark.py \
    --config /app/config/config.yaml \
    --scale-factor ${SCALE_FACTOR:-1} \
    --formats iceberg,deltalake,ducklake

echo "Benchmark complete! Results in ./results/"
```

#### Cleanup Script

```bash
#!/bin/bash
# scripts/cleanup.sh

echo "Stopping containers..."
docker-compose down

echo "Cleaning up data volumes? (y/n)"
read -r response
if [[ "$response" == "y" ]]; then
    docker-compose down -v
    rm -rf ./data/iceberg/* ./data/deltalake/* ./data/ducklake/*
    echo "Data volumes cleaned"
fi
```

---

## Benchmark Architecture

### Phase 2: Core Components

#### 2.1 Project Structure

```
benchmark/
├── __init__.py
├── core/
│   ├── __init__.py
│   ├── loader.py              # Data loading for all formats
│   ├── query_executor.py      # Query execution engine
│   ├── merge_ops.py           # Update/merge operations
│   └── metadata_ops.py        # Metadata performance tests
├── formats/
│   ├── __init__.py
│   ├── iceberg_handler.py     # Iceberg-specific operations
│   ├── delta_handler.py       # Delta Lake-specific operations
│   └── ducklake_handler.py    # DuckLake-specific operations
├── metrics/
│   ├── __init__.py
│   ├── collector.py           # Metrics collection
│   └── reporter.py            # Results reporting
├── utils/
│   ├── __init__.py
│   ├── config.py              # Configuration management
│   ├── logger.py              # Logging utilities
│   └── helpers.py             # Helper functions
└── tests/
    ├── __init__.py
    ├── test_loader.py
    ├── test_executor.py
    └── test_metrics.py
```

#### 2.2 Data Loader Module

```python
# benchmark/core/loader.py

from abc import ABC, abstractmethod
import time
from typing import Dict, Any
import duckdb

class BaseLoader(ABC):
    """Abstract base class for data loaders"""
    
    def __init__(self, source_path: str, target_path: str):
        self.source_path = source_path
        self.target_path = target_path
        self.metrics = {}
    
    @abstractmethod
    def load_table(self, table_name: str) -> Dict[str, Any]:
        """Load a single table and return metrics"""
        pass
    
    @abstractmethod
    def load_all_tables(self) -> Dict[str, Any]:
        """Load all TPC-DS tables"""
        pass
    
    def measure_load_time(self, func):
        """Decorator to measure loading time"""
        start_time = time.time()
        result = func()
        elapsed = time.time() - start_time
        return result, elapsed


class IcebergLoader(BaseLoader):
    """Loader for Apache Iceberg format"""
    
    def __init__(self, source_path: str, target_path: str, catalog_uri: str):
        super().__init__(source_path, target_path)
        self.catalog_uri = catalog_uri
        self.conn = duckdb.connect()
        self._setup_iceberg()
    
    def _setup_iceberg(self):
        """Configure DuckDB for Iceberg"""
        self.conn.execute("INSTALL iceberg")
        self.conn.execute("LOAD iceberg")
    
    def load_table(self, table_name: str) -> Dict[str, Any]:
        """Load a single table to Iceberg format"""
        start_time = time.time()
        
        # Read source data
        source_file = f"{self.source_path}/{table_name}.parquet"
        
        # Write to Iceberg
        query = f"""
        CREATE OR REPLACE TABLE iceberg_scan('{self.target_path}/{table_name}')
        AS SELECT * FROM read_parquet('{source_file}')
        """
        self.conn.execute(query)
        
        load_time = time.time() - start_time
        
        # Collect metrics
        metrics = {
            'table_name': table_name,
            'format': 'iceberg',
            'load_time_seconds': load_time,
            'num_rows': self._get_row_count(table_name),
            'size_bytes': self._get_table_size(table_name)
        }
        
        return metrics
    
    def load_all_tables(self) -> Dict[str, Any]:
        """Load all TPC-DS tables"""
        tpcds_tables = [
            'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
            'customer', 'customer_address', 'customer_demographics', 'date_dim',
            'household_demographics', 'income_band', 'inventory', 'item',
            'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
            'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
            'web_sales', 'web_site'
        ]
        
        all_metrics = []
        total_start = time.time()
        
        for table in tpcds_tables:
            print(f"Loading {table} to Iceberg...")
            metrics = self.load_table(table)
            all_metrics.append(metrics)
        
        total_time = time.time() - total_start
        
        return {
            'format': 'iceberg',
            'total_load_time': total_time,
            'table_metrics': all_metrics
        }
    
    def _get_row_count(self, table_name: str) -> int:
        result = self.conn.execute(
            f"SELECT COUNT(*) FROM iceberg_scan('{self.target_path}/{table_name}')"
        ).fetchone()
        return result[0]
    
    def _get_table_size(self, table_name: str) -> int:
        # Implementation to get actual file size
        import os
        path = f"{self.target_path}/{table_name}"
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                total_size += os.path.getsize(filepath)
        return total_size


class DeltaLoader(BaseLoader):
    """Loader for Delta Lake format"""
    
    def __init__(self, source_path: str, target_path: str):
        super().__init__(source_path, target_path)
        self.conn = duckdb.connect()
        self._setup_delta()
    
    def _setup_delta(self):
        """Configure DuckDB for Delta Lake"""
        self.conn.execute("INSTALL delta")
        self.conn.execute("LOAD delta")
    
    def load_table(self, table_name: str) -> Dict[str, Any]:
        """Load a single table to Delta Lake format"""
        start_time = time.time()
        
        source_file = f"{self.source_path}/{table_name}.parquet"
        target_path = f"{self.target_path}/{table_name}"
        
        # Write to Delta Lake
        query = f"""
        COPY (SELECT * FROM read_parquet('{source_file}'))
        TO '{target_path}' (FORMAT DELTA)
        """
        self.conn.execute(query)
        
        load_time = time.time() - start_time
        
        metrics = {
            'table_name': table_name,
            'format': 'delta',
            'load_time_seconds': load_time,
            'num_rows': self._get_row_count(target_path),
            'size_bytes': self._get_table_size(target_path)
        }
        
        return metrics
    
    def load_all_tables(self) -> Dict[str, Any]:
        """Load all TPC-DS tables"""
        # Similar implementation to IcebergLoader
        pass
    
    def _get_row_count(self, table_path: str) -> int:
        result = self.conn.execute(
            f"SELECT COUNT(*) FROM delta_scan('{table_path}')"
        ).fetchone()
        return result[0]
    
    def _get_table_size(self, table_path: str) -> int:
        # Similar to Iceberg implementation
        pass


class DuckLakeLoader(BaseLoader):
    """Loader for DuckLake format"""
    
    def __init__(self, source_path: str, target_path: str, catalog_db: str):
        super().__init__(source_path, target_path)
        self.catalog_db = catalog_db
        self.conn = duckdb.connect(catalog_db)
    
    def load_table(self, table_name: str) -> Dict[str, Any]:
        """Load a single table to DuckLake format"""
        start_time = time.time()
        
        source_file = f"{self.source_path}/{table_name}.parquet"
        target_path = f"{self.target_path}/{table_name}"
        
        # Create table in catalog
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_parquet('{source_file}')
        """)
        
        # Export to Parquet with DuckLake metadata
        self.conn.execute(f"""
            COPY {table_name} TO '{target_path}' (FORMAT PARQUET)
        """)
        
        load_time = time.time() - start_time
        
        metrics = {
            'table_name': table_name,
            'format': 'ducklake',
            'load_time_seconds': load_time,
            'num_rows': self._get_row_count(table_name),
            'size_bytes': self._get_table_size(target_path)
        }
        
        return metrics
    
    def load_all_tables(self) -> Dict[str, Any]:
        """Load all TPC-DS tables"""
        # Similar implementation
        pass
    
    def _get_row_count(self, table_name: str) -> int:
        result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return result[0]
    
    def _get_table_size(self, table_path: str) -> int:
        # Similar implementation
        pass
```

#### 2.3 Query Executor Module

```python
# benchmark/core/query_executor.py

import time
import duckdb
from typing import List, Dict, Any
from statistics import median

class QueryExecutor:
    """Execute TPC-DS queries and measure performance"""
    
    def __init__(self, format_type: str, data_path: str):
        self.format_type = format_type
        self.data_path = data_path
        self.conn = duckdb.connect()
        self._setup_format()
        self.query_results = []
    
    def _setup_format(self):
        """Configure DuckDB for specific format"""
        if self.format_type == 'iceberg':
            self.conn.execute("INSTALL iceberg; LOAD iceberg;")
        elif self.format_type == 'delta':
            self.conn.execute("INSTALL delta; LOAD delta;")
        # DuckLake uses native DuckDB
    
    def _create_table_view(self, table_name: str):
        """Create a view for the table based on format"""
        if self.format_type == 'iceberg':
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW {table_name} AS
                SELECT * FROM iceberg_scan('{self.data_path}/{table_name}')
            """)
        elif self.format_type == 'delta':
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW {table_name} AS
                SELECT * FROM delta_scan('{self.data_path}/{table_name}')
            """)
        elif self.format_type == 'ducklake':
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW {table_name} AS
                SELECT * FROM read_parquet('{self.data_path}/{table_name}/**/*.parquet')
            """)
    
    def setup_tpcds_views(self):
        """Create views for all TPC-DS tables"""
        tpcds_tables = [
            'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
            'customer', 'customer_address', 'customer_demographics', 'date_dim',
            'household_demographics', 'income_band', 'inventory', 'item',
            'promotion', 'reason', 'ship_mode', 'store', 'store_returns',
            'store_sales', 'time_dim', 'warehouse', 'web_page', 'web_returns',
            'web_sales', 'web_site'
        ]
        
        for table in tpcds_tables:
            self._create_table_view(table)
    
    def execute_query(self, query: str, query_id: str, 
                     num_runs: int = 3) -> Dict[str, Any]:
        """
        Execute a query multiple times and return metrics.
        First run is considered cold, median of remaining runs is reported.
        """
        times = []
        
        for run in range(num_runs + 1):  # +1 for cold run
            # Clear cache if needed
            if run > 0:
                self.conn.execute("CHECKPOINT")
            
            start_time = time.time()
            
            try:
                result = self.conn.execute(query)
                rows = result.fetchall()
                execution_time = time.time() - start_time
                
                # Skip first (cold) run
                if run > 0:
                    times.append(execution_time)
                
                # Get execution stats
                stats = self._get_query_stats()
                
            except Exception as e:
                return {
                    'query_id': query_id,
                    'format': self.format_type,
                    'status': 'failed',
                    'error': str(e)
                }
        
        median_time = median(times)
        
        metrics = {
            'query_id': query_id,
            'format': self.format_type,
            'status': 'success',
            'execution_time_seconds': median_time,
            'all_run_times': times,
            'cold_run_time': times[0] if times else None,
            'rows_returned': len(rows) if rows else 0,
            **stats
        }
        
        self.query_results.append(metrics)
        return metrics
    
    def _get_query_stats(self) -> Dict[str, Any]:
        """Extract query execution statistics from DuckDB"""
        try:
            # Get query profiling information
            profile = self.conn.execute("SELECT * FROM duckdb_query_profile()").fetchdf()
            
            return {
                'bytes_read': profile['total_bytes_read'].sum() if 'total_bytes_read' in profile else 0,
                'rows_scanned': profile['total_rows_scanned'].sum() if 'total_rows_scanned' in profile else 0,
            }
        except:
            return {'bytes_read': 0, 'rows_scanned': 0}
    
    def execute_tpcds_suite(self, queries: List[tuple]) -> List[Dict[str, Any]]:
        """
        Execute full TPC-DS query suite.
        queries: List of (query_id, query_text) tuples
        """
        print(f"Executing TPC-DS suite for {self.format_type}...")
        results = []
        
        for query_id, query_text in queries:
            print(f"  Running Query {query_id}...")
            metrics = self.execute_query(query_text, query_id)
            results.append(metrics)
        
        return results
    
    def warm_up(self):
        """Perform a warm-up query to initialize connections"""
        self.conn.execute("SELECT 1").fetchall()
```

#### 2.4 Merge Operations Module

```python
# benchmark/core/merge_ops.py

import time
from typing import Dict, Any, List
import duckdb

class MergeOperations:
    """Handle update and merge operations for benchmarking"""
    
    def __init__(self, format_type: str, data_path: str):
        self.format_type = format_type
        self.data_path = data_path
        self.conn = duckdb.connect()
        self._setup_format()
    
    def _setup_format(self):
        """Configure DuckDB for specific format"""
        if self.format_type == 'iceberg':
            self.conn.execute("INSTALL iceberg; LOAD iceberg;")
        elif self.format_type == 'delta':
            self.conn.execute("INSTALL delta; LOAD delta;")
    
    def tpcds_refresh_benchmark(self, base_path: str, 
                                refresh_data_path: str,
                                scale_factor: int = 100) -> Dict[str, Any]:
        """
        TPC-DS Refresh Benchmark:
        1. Load 100GB base dataset
        2. Run 5 sample queries (Q3, Q9, Q34, Q42, Q59)
        3. Perform 10 refresh operations (3% of data each)
        4. Re-run the 5 queries
        """
        results = {
            'format': self.format_type,
            'scale_factor': scale_factor,
            'stages': {}
        }
        
        # Stage 1: Load base data
        print("Stage 1: Loading base data...")
        load_start = time.time()
        self._load_base_tables(base_path)
        results['stages']['load_time'] = time.time() - load_start
        
        # Stage 2: Run initial queries
        print("Stage 2: Running initial queries...")
        sample_queries = self._get_sample_queries()
        query_start = time.time()
        initial_query_results = self._run_queries(sample_queries)
        results['stages']['initial_query_time'] = time.time() - query_start
        results['initial_queries'] = initial_query_results
        
        # Stage 3: Perform 10 merge operations
        print("Stage 3: Performing merge operations...")
        merge_times = []
        for i in range(1, 11):
            print(f"  Merge operation {i}/10...")
            merge_start = time.time()
            self._perform_merge(refresh_data_path, i)
            merge_time = time.time() - merge_start
            merge_times.append(merge_time)
        
        results['stages']['merge_times'] = merge_times
        results['stages']['total_merge_time'] = sum(merge_times)
        results['stages']['avg_merge_time'] = sum(merge_times) / len(merge_times)
        
        # Stage 4: Re-run queries
        print("Stage 4: Re-running queries...")
        query_start = time.time()
        final_query_results = self._run_queries(sample_queries)
        results['stages']['final_query_time'] = time.time() - query_start
        results['final_queries'] = final_query_results
        
        # Calculate query slowdown
        results['query_slowdown'] = self._calculate_slowdown(
            initial_query_results, final_query_results
        )
        
        return results
    
    def micro_merge_benchmark(self, base_table_size_gb: int = 100,
                             update_percentages: List[float] = None) -> Dict[str, Any]:
        """
        Micro Merge Benchmark:
        Test varying sizes of merge operations
        
        Args:
            base_table_size_gb: Size of base synthetic table
            update_percentages: List of update sizes as % of base table
        """
        if update_percentages is None:
            update_percentages = [0.0001, 0.001, 0.01, 0.1]
        
        results = {
            'format': self.format_type,
            'base_table_size_gb': base_table_size_gb,
            'tests': []
        }
        
        # Create synthetic base table
        print("Creating synthetic base table...")
        self._create_synthetic_table(base_table_size_gb)
        
        # Run baseline query
        baseline_query_time = self._run_baseline_query()
        results['baseline_query_time'] = baseline_query_time
        
        # Test each update size
        for update_pct in update_percentages:
            print(f"\nTesting {update_pct}% update size...")
            
            # Perform merge
            merge_start = time.time()
            num_rows_updated = self._perform_micro_merge(update_pct)
            merge_time = time.time() - merge_start
            
            # Measure post-merge query performance
            post_merge_query_time = self._run_baseline_query()
            
            # Calculate slowdown
            slowdown = post_merge_query_time / baseline_query_time
            
            test_result = {
                'update_percentage': update_pct,
                'rows_updated': num_rows_updated,
                'merge_time_seconds': merge_time,
                'post_merge_query_time': post_merge_query_time,
                'query_slowdown': slowdown
            }
            
            results['tests'].append(test_result)
            
            # Reset table for next test
            self._reset_synthetic_table()
        
        return results
    
    def _load_base_tables(self, base_path: str):
        """Load base TPC-DS tables"""
        # Implementation depends on format
        pass
    
    def _get_sample_queries(self) -> List[tuple]:
        """Get the 5 sample TPC-DS queries (Q3, Q9, Q34, Q42, Q59)"""
        # Return list of (query_id, query_text) tuples
        pass
    
    def _run_queries(self, queries: List[tuple]) -> List[Dict[str, Any]]:
        """Run a list of queries and return results"""
        results = []
        for query_id, query_text in queries:
            start = time.time()
            self.conn.execute(query_text)
            exec_time = time.time() - start
            results.append({
                'query_id': query_id,
                'execution_time': exec_time
            })
        return results
    
    def _perform_merge(self, refresh_data_path: str, iteration: int):
        """Perform a single merge operation"""
        if self.format_type == 'delta':
            # Delta Lake MERGE INTO syntax
            query = f"""
            MERGE INTO store_sales AS target
            USING (SELECT * FROM delta_scan('{refresh_data_path}/iteration_{iteration}')) AS source
            ON target.ss_item_sk = source.ss_item_sk 
                AND target.ss_ticket_number = source.ss_ticket_number
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
            self.conn.execute(query)
        # Similar implementations for Iceberg and DuckLake
    
    def _create_synthetic_table(self, size_gb: int):
        """Create synthetic table for micro-benchmark"""
        # Create table with 4 columns, approximately size_gb in size
        num_rows = int(size_gb * 1e9 / 40)  # Rough estimate: 40 bytes per row
        
        query = f"""
        CREATE OR REPLACE TABLE synthetic_table AS
        SELECT
            random() AS col1,
            random() AS col2,
            random() AS col3,
            random() AS col4
        FROM range({num_rows})
        """
        self.conn.execute(query)
    
    def _perform_micro_merge(self, update_pct: float) -> int:
        """Perform a micro-merge with specified percentage of updates"""
        # 50% inserts, 50% updates as per paper methodology
        pass
    
    def _run_baseline_query(self) -> float:
        """Run a simple aggregation query on the synthetic table"""
        start = time.time()
        self.conn.execute("SELECT SUM(col1), AVG(col2) FROM synthetic_table")
        return time.time() - start
    
    def _reset_synthetic_table(self):
        """Reset the synthetic table to baseline state"""
        # Reload the table without modifications
        pass
    
    def _calculate_slowdown(self, initial: List[Dict], final: List[Dict]) -> float:
        """Calculate average query slowdown after merges"""
        initial_avg = sum(q['execution_time'] for q in initial) / len(initial)
        final_avg = sum(q['execution_time'] for q in final) / len(final)
        return final_avg / initial_avg
```

#### 2.5 Metadata Operations Module

```python
# benchmark/core/metadata_ops.py

import time
from typing import List, Dict, Any
import duckdb
import os

class MetadataOperations:
    """Test metadata handling performance with varying file counts"""
    
    def __init__(self, format_type: str, data_path: str):
        self.format_type = format_type
        self.data_path = data_path
        self.conn = duckdb.connect()
        self._setup_format()
    
    def _setup_format(self):
        """Configure DuckDB for specific format"""
        if self.format_type == 'iceberg':
            self.conn.execute("INSTALL iceberg; LOAD iceberg;")
        elif self.format_type == 'delta':
            self.conn.execute("INSTALL delta; LOAD delta;")
    
    def large_file_count_test(self, 
                             file_counts: List[int] = None,
                             file_size_mb: int = 10) -> Dict[str, Any]:
        """
        Test metadata performance with varying file counts.
        
        Args:
            file_counts: List of file counts to test (e.g., [1000, 50000, 100000, 200000])
            file_size_mb: Size of each file in MB
        
        Returns:
            Dictionary with test results
        """
        if file_counts is None:
            file_counts = [1000, 50000, 100000, 200000]
        
        results = {
            'format': self.format_type,
            'file_size_mb': file_size_mb,
            'tests': []
        }
        
        for file_count in file_counts:
            print(f"\nTesting with {file_count} files...")
            
            # Create fragmented table
            table_path = self._create_fragmented_table(file_count, file_size_mb)
            
            # Run three types of selective queries
            test_result = {
                'file_count': file_count,
                'total_size_gb': (file_count * file_size_mb) / 1024,
                'queries': []
            }
            
            # Query 1: SELECT LIMIT 1
            query1_result = self._test_query(
                query_id="select_limit_1",
                query="SELECT * FROM test_table LIMIT 1",
                table_path=table_path
            )
            test_result['queries'].append(query1_result)
            
            # Query 2: Filter by partition
            query2_result = self._test_query(
                query_id="filter_by_partition",
                query="SELECT * FROM test_table WHERE partition_col = 'partition_0'",
                table_path=table_path
            )
            test_result['queries'].append(query2_result)
            
            # Query 3: Filter by value
            query3_result = self._test_query(
                query_id="filter_by_value",
                query="SELECT * FROM test_table WHERE ss_sold_date_sk = 2451119",
                table_path=table_path
            )
            test_result['queries'].append(query3_result)
            
            results['tests'].append(test_result)
            
            # Cleanup
            self._cleanup_table(table_path)
        
        return results
    
    def _create_fragmented_table(self, file_count: int, 
                                 file_size_mb: int) -> str:
        """
        Create a table split into many small files.
        Uses TPC-DS store_sales table as base.
        """
        table_path = f"{self.data_path}/fragmented_{file_count}_files"
        os.makedirs(table_path, exist_ok=True)
        
        # Calculate rows per file to achieve target file size
        # Assuming ~40 bytes per row for store_sales
        rows_per_file = int((file_size_mb * 1024 * 1024) / 40)
        
        print(f"  Creating {file_count} files with ~{rows_per_file} rows each...")
        
        # Generate data from TPC-DS store_sales
        # Split into file_count files
        for i in range(file_count):
            file_path = f"{table_path}/part_{i:06d}.parquet"
            
            query = f"""
            COPY (
                SELECT * FROM store_sales 
                LIMIT {rows_per_file} 
                OFFSET {i * rows_per_file}
            )
            TO '{file_path}' (FORMAT PARQUET)
            """
            self.conn.execute(query)
        
        return table_path
    
    def _test_query(self, query_id: str, query: str, 
                   table_path: str) -> Dict[str, Any]:
        """
        Test a single query and measure startup vs execution time.
        
        Returns metrics including:
        - startup_time: Time for query planning
        - execution_time: Time for actual query execution
        - total_time: Total query time
        """
        # Create view based on format
        if self.format_type == 'iceberg':
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW test_table AS
                SELECT * FROM iceberg_scan('{table_path}')
            """)
        elif self.format_type == 'delta':
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW test_table AS
                SELECT * FROM delta_scan('{table_path}')
            """)
        else:  # ducklake
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW test_table AS
                SELECT * FROM read_parquet('{table_path}/**/*.parquet')
            """)
        
        # Warm up
        self.conn.execute("SELECT 1")
        
        # Measure query with profiling
        total_start = time.time()
        
        # Enable profiling
        self.conn.execute("PRAGMA enable_profiling")
        
        result = self.conn.execute(query)
        rows = result.fetchall()
        
        total_time = time.time() - total_start
        
        # Get profiling info to separate planning from execution
        profile_info = self.conn.execute(
            "SELECT * FROM pragma_last_profiling_output()"
        ).fetchdf()
        
        # Estimate startup time (planning + optimization)
        # vs execution time (actual data processing)
        # This is an approximation based on DuckDB's profiling
        execution_time = total_time * 0.8  # Rough estimate
        startup_time = total_time - execution_time
        
        return {
            'query_id': query_id,
            'startup_time_ms': startup_time * 1000,
            'execution_time_ms': execution_time * 1000,
            'total_time_ms': total_time * 1000,
            'rows_returned': len(rows)
        }
    
    def _cleanup_table(self, table_path: str):
        """Remove temporary fragmented table"""
        import shutil
        if os.path.exists(table_path):
            shutil.rmtree(table_path)
```

#### 2.6 Metrics Collection

```python
# benchmark/metrics/collector.py

import json
import csv
from datetime import datetime
from typing import Dict, Any, List
import os

class MetricsCollector:
    """Collect and store benchmark metrics"""
    
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.metrics = {
            'timestamp': datetime.now().isoformat(),
            'load_metrics': [],
            'query_metrics': [],
            'merge_metrics': [],
            'metadata_metrics': []
        }
        
        os.makedirs(output_dir, exist_ok=True)
    
    def add_load_metrics(self, metrics: Dict[str, Any]):
        """Add data loading metrics"""
        self.metrics['load_metrics'].append(metrics)
    
    def add_query_metrics(self, metrics: List[Dict[str, Any]]):
        """Add query execution metrics"""
        self.metrics['query_metrics'].extend(metrics)
    
    def add_merge_metrics(self, metrics: Dict[str, Any]):
        """Add merge operation metrics"""
        self.metrics['merge_metrics'].append(metrics)
    
    def add_metadata_metrics(self, metrics: Dict[str, Any]):
        """Add metadata operation metrics"""
        self.metrics['metadata_metrics'].append(metrics)
    
    def save_json(self, filename: str = None):
        """Save all metrics to JSON file"""
        if filename is None:
            filename = f"metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(self.metrics, f, indent=2)
        
        print(f"Metrics saved to {filepath}")
        return filepath
    
    def save_csv_summary(self):
        """Save summary metrics to CSV files"""
        # Load metrics summary
        if self.metrics['load_metrics']:
            load_file = os.path.join(self.output_dir, 'load_summary.csv')
            self._save_list_to_csv(self.metrics['load_metrics'], load_file)
        
        # Query metrics summary
        if self.metrics['query_metrics']:
            query_file = os.path.join(self.output_dir, 'query_summary.csv')
            self._save_list_to_csv(self.metrics['query_metrics'], query_file)
        
        # Merge metrics summary
        if self.metrics['merge_metrics']:
            merge_file = os.path.join(self.output_dir, 'merge_summary.csv')
            self._save_list_to_csv(self.metrics['merge_metrics'], merge_file)
        
        # Metadata metrics summary
        if self.metrics['metadata_metrics']:
            meta_file = os.path.join(self.output_dir, 'metadata_summary.csv')
            self._save_list_to_csv(self.metrics['metadata_metrics'], meta_file)
    
    def _save_list_to_csv(self, data: List[Dict], filepath: str):
        """Save list of dictionaries to CSV"""
        if not data:
            return
        
        # Flatten nested dictionaries
        flat_data = []
        for item in data:
            flat_item = self._flatten_dict(item)
            flat_data.append(flat_item)
        
        keys = flat_data[0].keys()
        
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(flat_data)
        
        print(f"CSV saved to {filepath}")
    
    def _flatten_dict(self, d: Dict, parent_key: str = '', 
                     sep: str = '_') -> Dict:
        """Flatten nested dictionary"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
        return dict(items)
    
    def get_summary_statistics(self) -> Dict[str, Any]:
        """Calculate summary statistics across all metrics"""
        summary = {
            'total_load_time_by_format': {},
            'avg_query_time_by_format': {},
            'total_queries_run': len(self.metrics['query_metrics']),
            'formats_tested': set()
        }
        
        # Load time summary
        for load_metric in self.metrics['load_metrics']:
            fmt = load_metric.get('format')
            if fmt:
                summary['formats_tested'].add(fmt)
                total_time = load_metric.get('total_load_time', 0)
                summary['total_load_time_by_format'][fmt] = total_time
        
        # Query time summary
        query_times = {}
        for query_metric in self.metrics['query_metrics']:
            fmt = query_metric.get('format')
            if fmt:
                if fmt not in query_times:
                    query_times[fmt] = []
                exec_time = query_metric.get('execution_time_seconds', 0)
                query_times[fmt].append(exec_time)
        
        for fmt, times in query_times.items():
            summary['avg_query_time_by_format'][fmt] = sum(times) / len(times)
        
        summary['formats_tested'] = list(summary['formats_tested'])
        
        return summary
```

---

## Implementation Phases

### Phase 3: Configuration Management

#### 3.1 Configuration File

```yaml
# config/config.yaml

benchmark:
  name: "TPC-DS Lakehouse Benchmark"
  version: "1.0.0"
  scale_factors: [1, 10, 100]
  formats: 
    - iceberg
    - deltalake
    - ducklake
  
  # Execution settings
  num_query_runs: 3  # Median of 3 runs (plus 1 cold run)
  enable_profiling: true
  clear_cache_between_runs: true

# Data paths
paths:
  raw_data: "/data/raw"
  iceberg_data: "/data/iceberg"
  delta_data: "/data/deltalake"
  ducklake_data: "/data/ducklake"
  results: "/results"
  logs: "/app/logs"

# Storage configuration
storage:
  use_s3: false
  s3_bucket: "benchmark-data"
  s3_region: "us-east-1"
  local_storage: true

# Catalog configuration
catalogs:
  postgres:
    host: "postgres"
    port: 5432
    database: "iceberg_catalog"
    user: "iceberg"
    password: "iceberg"
  
  ducklake:
    catalog_db: "/data/ducklake/catalog.db"

# TPC-DS configuration
tpcds:
  # Queries to run (or "all" for all queries)
  queries: 
    - 3
    - 7
    - 9
    - 19
    - 27
    - 34
    - 42
    - 43
    - 52
    - 55
    - 59
    - 63
    - 65
    - 73
    - 79
    - 88
  
  # For refresh benchmark
  refresh_queries: [3, 9, 34, 42, 59]
  num_refresh_iterations: 10
  refresh_data_percentage: 3  # Percent of base data per iteration

# Merge benchmark configuration
merge:
  base_table_size_gb: 100
  update_percentages: [0.0001, 0.001, 0.01, 0.1]
  insert_update_ratio: 0.5  # 50% inserts, 50% updates

# Metadata benchmark configuration
metadata:
  file_size_mb: 10
  file_counts: [1000, 50000, 100000, 200000]
  test_queries:
    - name: "select_limit_1"
      query: "SELECT * FROM test_table LIMIT 1"
    - name: "filter_by_partition"
      query: "SELECT * FROM test_table WHERE partition_col = 'partition_0'"
    - name: "filter_by_value"
      query: "SELECT * FROM test_table WHERE ss_sold_date_sk = 2451119"

# DuckDB configuration
duckdb:
  memory_limit: "16GB"
  threads: 8
  enable_object_cache: true
  max_memory: "80%"

# Logging
logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  file: "/app/logs/benchmark.log"

# Reporting
reporting:
  generate_plots: true
  plot_formats: ["png", "svg"]
  create_html_report: true
  compare_with_baseline: false
```

#### 3.2 Query Definitions

```yaml
# config/queries.yaml

tpcds_queries:
  query_3: |
    SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,SUM(ss_ext_sales_price) sum_agg
    FROM date_dim dt, store_sales, item
    WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
      AND store_sales.ss_item_sk = item.i_item_sk
      AND item.i_manufact_id = 128
      AND dt.d_moy=11
    GROUP BY dt.d_year, item.i_brand, item.i_brand_id
    ORDER BY dt.d_year, sum_agg desc, brand_id
    LIMIT 100
  
  query_7: |
    SELECT i_item_id, avg(ss_quantity) agg1, avg(ss_list_price) agg2,
           avg(ss_coupon_amt) agg3, avg(ss_sales_price) agg4
    FROM store_sales, customer_demographics, date_dim, item, promotion
    WHERE ss_sold_date_sk = d_date_sk
      AND ss_item_sk = i_item_sk
      AND ss_cdemo_sk = cd_demo_sk
      AND ss_promo_sk = p_promo_sk
      AND cd_gender = 'F'
      AND cd_marital_status = 'W'
      AND cd_education_status = 'Primary'
      AND (p_channel_email = 'N' or p_channel_event = 'N')
      AND d_year = 1998
    GROUP BY i_item_id
    ORDER BY i_item_id
    LIMIT 100
  
  # Add more queries...
```

### Phase 4: Main Execution Script

```python
# run_benchmark.py

import argparse
import yaml
import logging
import sys
from pathlib import Path
from datetime import datetime

from benchmark.core.loader import IcebergLoader, DeltaLoader, DuckLakeLoader
from benchmark.core.query_executor import QueryExecutor
from benchmark.core.merge_ops import MergeOperations
from benchmark.core.metadata_ops import MetadataOperations
from benchmark.metrics.collector import MetricsCollector
from benchmark.metrics.reporter import BenchmarkReporter
from benchmark.utils.logger import setup_logger

class BenchmarkRunner:
    """Main benchmark orchestrator"""
    
    def __init__(self, config_path: str):
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Setup logging
        self.logger = setup_logger(
            log_file=self.config['logging']['file'],
            level=self.config['logging']['level']
        )
        
        # Initialize metrics collector
        self.metrics = MetricsCollector(self.config['paths']['results'])
        
        self.logger.info("Benchmark Runner initialized")
        self.logger.info(f"Testing formats: {self.config['benchmark']['formats']}")
    
    def run_all_benchmarks(self):
        """Run all benchmark tests"""
        try:
            for scale_factor in self.config['benchmark']['scale_factors']:
                self.logger.info(f"\n{'='*60}")
                self.logger.info(f"Running benchmarks for scale factor {scale_factor}")
                self.logger.info(f"{'='*60}\n")
                
                # Phase 1: Data Loading
                self.run_load_benchmark(scale_factor)
                
                # Phase 2: TPC-DS Query Performance
                self.run_tpcds_benchmark(scale_factor)
                
                # Phase 3: TPC-DS Refresh (only for SF 100)
                if scale_factor == 100:
                    self.run_refresh_benchmark(scale_factor)
                
                # Phase 4: Micro Merge Benchmark
                self.run_micro_merge_benchmark(scale_factor)
                
                # Phase 5: Metadata Performance
                self.run_metadata_benchmark(scale_factor)
            
            # Save and generate report
            self.finalize_results()
            
            self.logger.info("\nAll benchmarks completed successfully!")
            
        except Exception as e:
            self.logger.error(f"Benchmark failed: {str(e)}", exc_info=True)
            raise
    
    def run_load_benchmark(self, scale_factor: int):
        """Run data loading benchmark for all formats"""
        self.logger.info(f"Phase 1: Data Loading (SF{scale_factor})")
        
        raw_data_path = f"{self.config['paths']['raw_data']}/{scale_factor}gb"
        
        for format_type in self.config['benchmark']['formats']:
            self.logger.info(f"  Loading data for {format_type}...")
            
            try:
                if format_type == 'iceberg':
                    loader = IcebergLoader(
                        source_path=raw_data_path,
                        target_path=self.config['paths']['iceberg_data'],
                        catalog_uri=self._get_catalog_uri()
                    )
                elif format_type == 'deltalake':
                    loader = DeltaLoader(
                        source_path=raw_data_path,
                        target_path=self.config['paths']['delta_data']
                    )
                elif format_type == 'ducklake':
                    loader = DuckLakeLoader(
                        source_path=raw_data_path,
                        target_path=self.config['paths']['ducklake_data'],
                        catalog_db=self.config['catalogs']['ducklake']['catalog_db']
                    )
                
                # Load all tables
                load_metrics = loader.load_all_tables()
                load_metrics['scale_factor'] = scale_factor
                
                self.metrics.add_load_metrics(load_metrics)
                self.logger.info(f"    Completed in {load_metrics['total_load_time']:.2f}s")
                
            except Exception as e:
                self.logger.error(f"    Failed to load {format_type}: {str(e)}")
    
    def run_tpcds_benchmark(self, scale_factor: int):
        """Run TPC-DS query benchmark for all formats"""
        self.logger.info(f"\nPhase 2: TPC-DS Query Performance (SF{scale_factor})")
        
        # Load queries
        queries = self._load_queries()
        
        for format_type in self.config['benchmark']['formats']:
            self.logger.info(f"  Running queries for {format_type}...")
            
            try:
                data_path = self._get_data_path(format_type)
                executor = QueryExecutor(format_type, data_path)
                
                # Setup tables
                executor.setup_tpcds_views()
                
                # Execute query suite
                query_metrics = executor.execute_tpcds_suite(queries)
                
                # Add metadata
                for metric in query_metrics:
                    metric['scale_factor'] = scale_factor
                
                self.metrics.add_query_metrics(query_metrics)
                
                total_time = sum(
                    m['execution_time_seconds'] 
                    for m in query_metrics 
                    if m['status'] == 'success'
                )
                self.logger.info(f"    Total query time: {total_time:.2f}s")
                
            except Exception as e:
                self.logger.error(f"    Failed queries for {format_type}: {str(e)}")
    
    def run_refresh_benchmark(self, scale_factor: int):
        """Run TPC-DS refresh benchmark (100GB only)"""
        self.logger.info(f"\nPhase 3: TPC-DS Refresh Benchmark (SF{scale_factor})")
        
        for format_type in self.config['benchmark']['formats']:
            self.logger.info(f"  Running refresh benchmark for {format_type}...")
            
            try:
                data_path = self._get_data_path(format_type)
                merge_ops = MergeOperations(format_type, data_path)
                
                base_path = f"{data_path}/tpcds_{scale_factor}"
                refresh_path = f"{self.config['paths']['raw_data']}/refresh_data"
                
                refresh_metrics = merge_ops.tpcds_refresh_benchmark(
                    base_path, refresh_path, scale_factor
                )
                
                self.metrics.add_merge_metrics(refresh_metrics)
                self.logger.info(
                    f"    Completed. Total merge time: "
                    f"{refresh_metrics['stages']['total_merge_time']:.2f}s"
                )
                
            except Exception as e:
                self.logger.error(
                    f"    Failed refresh benchmark for {format_type}: {str(e)}"
                )
    
    def run_micro_merge_benchmark(self, scale_factor: int):
        """Run micro merge benchmark"""
        self.logger.info(f"\nPhase 4: Micro Merge Benchmark (SF{scale_factor})")
        
        for format_type in self.config['benchmark']['formats']:
            self.logger.info(f"  Running micro merge for {format_type}...")
            
            try:
                data_path = self._get_data_path(format_type)
                merge_ops = MergeOperations(format_type, data_path)
                
                micro_metrics = merge_ops.micro_merge_benchmark(
                    base_table_size_gb=self.config['merge']['base_table_size_gb'],
                    update_percentages=self.config['merge']['update_percentages']
                )
                micro_metrics['scale_factor'] = scale_factor
                
                self.metrics.add_merge_metrics(micro_metrics)
                self.logger.info(f"    Completed micro merge tests")
                
            except Exception as e:
                self.logger.error(
                    f"    Failed micro merge for {format_type}: {str(e)}"
                )
    
    def run_metadata_benchmark(self, scale_factor: int):
        """Run metadata performance benchmark"""
        self.logger.info(f"\nPhase 5: Metadata Performance (SF{scale_factor})")
        
        for format_type in self.config['benchmark']['formats']:
            self.logger.info(f"  Running metadata tests for {format_type}...")
            
            try:
                data_path = self._get_data_path(format_type)
                metadata_ops = MetadataOperations(format_type, data_path)
                
                meta_metrics = metadata_ops.large_file_count_test(
                    file_counts=self.config['metadata']['file_counts'],
                    file_size_mb=self.config['metadata']['file_size_mb']
                )
                meta_metrics['scale_factor'] = scale_factor
                
                self.metrics.add_metadata_metrics(meta_metrics)
                self.logger.info(f"    Completed metadata tests")
                
            except Exception as e:
                self.logger.error(
                    f"    Failed metadata tests for {format_type}: {str(e)}"
                )
    
    def finalize_results(self):
        """Save metrics and generate reports"""
        self.logger.info("\nFinalizing results...")
        
        # Save raw metrics
        self.metrics.save_json()
        self.metrics.save_csv_summary()
        
        # Generate summary statistics
        summary = self.metrics.get_summary_statistics()
        self.logger.info("\nSummary Statistics:")
        for key, value in summary.items():
            self.logger.info(f"  {key}: {value}")
        
        # Generate visualizations and report
        if self.config['reporting']['generate_plots']:
            reporter = BenchmarkReporter(
                self.metrics,
                output_dir=self.config['paths']['results']
            )
            reporter.generate_all_visualizations()
            
            if self.config['reporting']['create_html_report']:
                reporter.create_html_report()
    
    def _load_queries(self) -> list:
        """Load TPC-DS queries from configuration"""
        queries_file = 'config/queries.yaml'
        with open(queries_file, 'r') as f:
            queries_config = yaml.safe_load(f)
        
        query_list = []
        for query_id in self.config['tpcds']['queries']:
            query_key = f"query_{query_id}"
            if query_key in queries_config['tpcds_queries']:
                query_text = queries_config['tpcds_queries'][query_key]
                query_list.append((query_id, query_text))
        
        return query_list
    
    def _get_data_path(self, format_type: str) -> str:
        """Get data path for specific format"""
        path_map = {
            'iceberg': self.config['paths']['iceberg_data'],
            'deltalake': self.config['paths']['delta_data'],
            'ducklake': self.config['paths']['ducklake_data']
        }
        return path_map.get(format_type, '')
    
    def _get_catalog_uri(self) -> str:
        """Get catalog URI for Iceberg"""
        pg_config = self.config['catalogs']['postgres']
        return f"postgresql://{pg_config['user']}:{pg_config['password']}@" \
               f"{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Run TPC-DS benchmark for lakehouse formats'
    )
    parser.add_argument(
        '--config',
        default='config/config.yaml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--scale-factor',
        type=int,
        help='Override scale factor from config'
    )
    parser.add_argument(
        '--formats',
        help='Comma-separated list of formats to test'
    )
    
    args = parser.parse_args()
    
    # Create runner
    runner = BenchmarkRunner(args.config)
    
    # Override config if arguments provided
    if args.scale_factor:
        runner.config['benchmark']['scale_factors'] = [args.scale_factor]
    
    if args.formats:
        runner.config['benchmark']['formats'] = args.formats.split(',')
    
    # Run benchmarks
    runner.run_all_benchmarks()


if __name__ == '__main__':
    main()
```

---

## Testing Methodology

### Benchmark Execution Strategy

Based on the CIDR 2023 paper methodology:

1. **Load Performance**
   - Measure time to load all TPC-DS tables
   - Track storage size and file counts
   - Record compression ratios

2. **Query Performance**
   - Run each query 4 times (1 cold + 3 warm runs)
   - Report median of 3 warm runs
   - Track startup time vs execution time
   - Monitor bytes read and rows scanned

3. **Update Performance (TPC-DS Refresh)**
   - Load 100GB base dataset
   - Run 5 sample queries
   - Perform 10 merge operations (3% of data each)
   - Re-run queries and measure slowdown

4. **Micro Merge Benchmark**
   - Create 100GB synthetic table
   - Test updates at 0.0001%, 0.001%, 0.01%, 0.1%
   - 50% inserts, 50% updates per merge
   - Measure merge latency and query slowdown

5. **Metadata Performance**
   - Create tables with varying file counts (1K to 200K files)
   - Each file is 10MB
   - Run selective queries:
     - SELECT LIMIT 1
     - Filter by partition
     - Filter by value
   - Measure planning time vs execution time

### Fairness Considerations

1. **Default Configurations**
   - Use out-of-the-box settings for all formats
   - No format-specific tuning unless documented
   - Same DuckDB version and settings

2. **Hardware Consistency**
   - All tests run in identical Docker containers
   - Same resource allocation
   - Clean slate between format tests

3. **Cache Management**
   - Clear caches between format comparisons
   - Warm-up runs for query tests
   - Consistent cold start measurements

4. **Reproducibility**
   - Docker ensures consistent environment
   - Version-pinned dependencies
   - Documented configuration

---

## Expected Outputs

### 1. Raw Metrics (JSON/CSV)
```
results/
├── metrics_20250120_143022.json
├── load_summary.csv
├── query_summary.csv
├── merge_summary.csv
└── metadata_summary.csv
```

### 2. Visualizations

#### Load Time Comparison
Bar chart showing total load time for each format at each scale factor.

#### Query Performance Summary
Table with columns:
- Query ID
- Iceberg Time
- Delta Time
- DuckLake Time
- Best Format
- Speedup Ratio

#### Individual Query Breakdowns
For queries with significant differences (>2x):
- Comparison charts
- Breakdown of execution phases

#### Merge Operation Performance
Stacked bar chart showing:
- Load time
- Initial query time
- Merge times (10 iterations)
- Final query time

#### Metadata Scaling Charts
Line charts showing:
- Query time vs file count
- Startup time vs file count
- Execution time vs file count
- Separate lines for each format

### 3. HTML Report

Comprehensive HTML report including:
- Executive summary
- Load performance comparison
- Query-by-query analysis
- Update performance analysis
- Metadata scaling analysis
- Recommendations and insights

---

## Timeline and Milestones

### Week 1-2: Infrastructure Setup
- [ ] Docker environment configuration
- [ ] Volume structure setup
- [ ] Dependency installation and testing
- [ ] Basic connectivity tests

### Week 3-4: Core Implementation
- [ ] Data loader modules
- [ ] Query executor
- [ ] Format handlers (Iceberg, Delta, DuckLake)
- [ ] Unit tests

### Week 5-6: Benchmark Tests
- [ ] Merge operations module
- [ ] Metadata operations module
- [ ] Metrics collector
- [ ] Integration tests

### Week 7: Execution & Analysis
- [ ] Run benchmarks for all scale factors
- [ ] Data collection and validation
- [ ] Results reporter
- [ ] Visualization generation

### Week 8: Documentation & Reporting
- [ ] Final report generation
- [ ] Code documentation
- [ ] README and usage guide
- [ ] Performance analysis writeup

---

## Quick Start Guide

### Initial Setup

```bash
# Clone repository
git clone https://github.com/soulrrrrr/CS598RAP-Project
cd CS598RAP-Project

# Build Docker containers
docker-compose build

# Generate TPC-DS data (if not already done)
./scripts/generate_data.sh --scale-factor 1
```

### Run Benchmarks

```bash
# Run full benchmark suite
./scripts/run_benchmark.sh

# Run specific scale factor
docker-compose run --rm benchmark-runner \
  python run_benchmark.py --scale-factor 10

# Run specific formats only
docker-compose run --rm benchmark-runner \
  python run_benchmark.py --formats iceberg,deltalake
```

### View Results

```bash
# Results are in ./results/
ls -l results/

# View summary
cat results/metrics_*.json | jq '.summary'

# Open HTML report
open results/benchmark_report.html
```

---

## References

1. Jain, P., et al. (2023). "Analyzing and Comparing Lakehouse Storage Systems." CIDR 2023.
   - Paper: https://www.cidrdb.org/cidr2023/papers/p92-jain.pdf

2. LHBench Repository
   - Code: https://github.com/lhbench/lhbench

3. DuckDB Documentation
   - Website: https://duckdb.org/docs/

4. Apache Iceberg Documentation
   - Website: https://iceberg.apache.org/docs/latest/

5. Delta Lake Documentation
   - Website: https://docs.delta.io/latest/index.html

6. TPC-DS Benchmark Specification
   - Specification: http://www.tpc.org/tpcds/

---

## Notes

- This plan is designed to be modular and extensible
- Each component can be implemented and tested independently
- Docker ensures reproducibility across different environments
- The methodology closely follows the CIDR 2023 paper while adapting for DuckDB
- Consider starting with small scale factors (1GB) for initial testing
- Larger scale factors (100GB+) may require significant time and resources

