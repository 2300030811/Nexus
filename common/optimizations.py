"""
Performance optimization utilities for Nexus platform.

Provides caching, connection pooling, query optimization, and performance monitoring.
"""

import asyncio
import functools
import hashlib
import json
import time
from typing import Any, Callable, Dict, Optional, Union, List
from cachetools import TTLCache, LRUCache
import psycopg2
from psycopg2.extras import execute_batch
from concurrent.futures import ThreadPoolExecutor
import threading

from .logging_utils import get_logger

logger = get_logger("optimizations")

# Performance caches
query_cache = TTLCache(maxsize=1000, ttl=300)  # 5-minute TTL for queries
config_cache = LRUCache(maxsize=500)  # LRU for configuration
metrics_cache = TTLCache(maxsize=100, ttl=60)  # 1-minute TTL for metrics

class PerformanceMonitor:
    """Monitor and track performance metrics."""
    
    def __init__(self):
        self.metrics = {}
        self.lock = threading.Lock()
    
    def record_timing(self, operation: str, duration: float):
        """Record timing for an operation."""
        with self.lock:
            if operation not in self.metrics:
                self.metrics[operation] = []
            self.metrics[operation].append(duration)
            
            # Keep only last 100 measurements
            if len(self.metrics[operation]) > 100:
                self.metrics[operation] = self.metrics[operation][-100:]
    
    def get_stats(self, operation: str) -> Dict[str, float]:
        """Get statistics for an operation."""
        with self.lock:
            if operation not in self.metrics or not self.metrics[operation]:
                return {}
            
            timings = self.metrics[operation]
            return {
                'count': len(timings),
                'avg': sum(timings) / len(timings),
                'min': min(timings),
                'max': max(timings),
                'p95': sorted(timings)[int(len(timings) * 0.95)],
                'p99': sorted(timings)[int(len(timings) * 0.99)]
            }

# Global performance monitor
perf_monitor = PerformanceMonitor()

def timed(operation_name: str = None):
    """Decorator to time function execution."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                op_name = operation_name or f"{func.__module__}.{func.__name__}"
                perf_monitor.record_timing(op_name, duration)
                
                # Log slow operations
                if duration > 1.0:  # Log operations taking > 1 second
                    logger.warning("Slow operation detected", extra={
                        "operation": op_name,
                        "duration": duration,
                        "threshold": 1.0
                    })
        return wrapper
    return decorator

def async_timed(operation_name: str = None):
    """Decorator to time async function execution."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                op_name = operation_name or f"{func.__module__}.{func.__name__}"
                perf_monitor.record_timing(op_name, duration)
                
                if duration > 1.0:
                    logger.warning("Slow async operation detected", extra={
                        "operation": op_name,
                        "duration": duration
                    })
        return wrapper
    return decorator

def smart_cache(ttl: int = 300, maxsize: int = 1000, key_func: Callable = None):
    """Advanced caching with TTL and custom key generation."""
    def decorator(func: Callable) -> Callable:
        cache = TTLCache(maxsize=maxsize, ttl=ttl)
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                # Default key generation
                key_parts = [str(arg) for arg in args]
                key_parts.extend([f"{k}={v}" for k, v in sorted(kwargs.items())])
                cache_key = hashlib.md5("|".join(key_parts).encode()).hexdigest()
            
            # Check cache
            if cache_key in cache:
                logger.debug("Cache hit", extra={"function": func.__name__, "cache_key": cache_key})
                return cache[cache_key]
            
            # Execute and cache
            result = func(*args, **kwargs)
            cache[cache_key] = result
            
            logger.debug("Cache miss - cached result", extra={
                "function": func.__name__, 
                "cache_key": cache_key,
                "ttl": ttl
            })
            
            return result
        
        # Add cache management methods
        wrapper.cache_clear = cache.clear
        wrapper.cache_info = lambda: {
            'maxsize': cache.maxsize,
            'currsize': len(cache),
            'ttl': ttl
        }
        
        return wrapper
    return decorator

class OptimizedDatabasePool:
    """Enhanced database connection pool with optimization features."""
    
    def __init__(self, minconn: int = 2, maxconn: int = 20, **kwargs):
        self.minconn = minconn
        self.maxconn = maxconn
        self.kwargs = kwargs
        self._pool = None
        self._lock = threading.Lock()
        self._stats = {
            'created': 0,
            'reused': 0,
            'errors': 0
        }
    
    def get_pool(self):
        """Get or create connection pool with lazy initialization."""
        if self._pool is None:
            with self._lock:
                if self._pool is None:
                    self._pool = psycopg2.pool.ThreadedConnectionPool(
                        minconn=self.minconn,
                        maxconn=self.maxconn,
                        **self.kwargs
                    )
                    logger.info("Database pool created", extra={
                        'min_connections': self.minconn,
                        'max_connections': self.maxconn
                    })
        return self._pool
    
    @timed("db_get_connection")
    def get_connection(self):
        """Get optimized database connection."""
        pool = self.get_pool()
        try:
            conn = pool.getconn()
            self._stats['reused'] += 1
            
            # Optimize connection settings
            with conn.cursor() as cur:
                cur.execute("SET application_name = 'nexus_optimized'")
                cur.execute("SET statement_timeout = '30s'")
                cur.execute("SET idle_in_transaction_session_timeout = '5min'")
            
            return conn
        except psycopg2.pool.PoolError as e:
            self._stats['errors'] += 1
            logger.error("Database pool exhausted", extra={'error': str(e)})
            raise
        except Exception as e:
            self._stats['errors'] += 1
            logger.error("Database connection error", extra={'error': str(e)})
            raise
    
    def return_connection(self, conn):
        """Return connection to pool with cleanup."""
        try:
            # Rollback any open transaction before resetting state
            if conn.status != 0:  # 0 = STATUS_READY (idle)
                conn.rollback()
            # Reset connection state using autocommit to avoid transaction block
            old_autocommit = conn.autocommit
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    cur.execute("DISCARD ALL")
            finally:
                conn.autocommit = old_autocommit
        except Exception as e:
            logger.warning("Error during connection cleanup", extra={'error': str(e)})
        finally:
            # Always return connection to pool even if cleanup fails
            try:
                self.get_pool().putconn(conn)
            except Exception as e:
                logger.error("Failed to return connection to pool", extra={'error': str(e)})
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
        pool = self.get_pool()
        return {
            'pool_stats': {
                'minconn': pool.minconn,
                'maxconn': pool.maxconn,
                'used': pool._used,
                'idle': len(pool._pool)
            },
            'operation_stats': self._stats.copy()
        }

class QueryOptimizer:
    """Database query optimization utilities."""
    
    @staticmethod
    @smart_cache(ttl=600, maxsize=100)
    def get_analyzed_query(query: str) -> str:
        """Get optimized query with EXPLAIN ANALYZE."""
        return f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"
    
    @staticmethod
    def optimize_batch_insert(conn, table: str, data: List[Dict], batch_size: int = 1000):
        """Optimized batch insert using execute_batch."""
        if not data:
            return
        
        # Prepare columns and values
        columns = list(data[0].keys())
        placeholders = ', '.join([f"%({col})s" for col in columns])
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Batch execute
        with conn.cursor() as cur:
            execute_batch(cur, query, data, page_size=batch_size)
        
        logger.info("Batch insert completed", extra={
            'table': table,
            'rows': len(data),
            'batch_size': batch_size
        })
    
    @staticmethod
    @timed("db_query_optimized")
    def execute_optimized_query(conn, query: str, params: tuple = None, fetch_size: int = 1000):
        """Execute query with cursor optimization for large result sets."""
        with conn.cursor(name='optimized_cursor') as cur:
            cur.itersize = fetch_size
            cur.execute(query, params)
            
            # For large result sets, use server-side cursor
            if fetch_size > 0:
                results = cur.fetchmany(fetch_size)
                while results:
                    yield results
                    results = cur.fetchmany(fetch_size)
            else:
                yield cur.fetchall()

class AsyncBatchProcessor:
    """Async batch processor for high-throughput operations."""
    
    def __init__(self, max_workers: int = 4, batch_size: int = 100):
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
    
    async def process_batches(self, items: List[Any], processor: Callable) -> List[Any]:
        """Process items in batches concurrently."""
        if not items:
            return []
        
        # Create batches
        batches = [items[i:i + self.batch_size] for i in range(0, len(items), self.batch_size)]
        
        # Process batches concurrently
        loop = asyncio.get_event_loop()
        tasks = []
        
        for batch in batches:
            task = loop.run_in_executor(self.executor, processor, batch)
            tasks.append(task)
        
        # Wait for all batches to complete
        results = await asyncio.gather(*tasks)
        
        # Flatten results
        return [item for batch_result in results for item in batch_result]
    
    def shutdown(self):
        """Shutdown the executor."""
        self.executor.shutdown(wait=True)

class MemoryOptimizer:
    """Memory optimization utilities."""
    
    @staticmethod
    def optimize_dataframe_memory(df):
        """Optimize pandas DataFrame memory usage."""
        import pandas as pd
        import numpy as np
        
        # Convert object columns to categorical where appropriate
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].nunique() / len(df) < 0.5:  # Less than 50% unique values
                df[col] = df[col].astype('category')
        
        # Downcast numeric columns
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='integer')
        
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')
        
        return df
    
    @staticmethod
    @smart_cache(ttl=1800, maxsize=50)  # 30-minute cache
    def get_cached_aggregation(data_hash: str, agg_func: Callable, *args, **kwargs):
        """Cache expensive aggregations."""
        return agg_func(*args, **kwargs)

# Performance monitoring middleware
class PerformanceMiddleware:
    """FastAPI middleware for performance monitoring."""
    
    def __init__(self, app):
        self.app = app
    
    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            start_time = time.time()
            
            # Process request
            await self.app(scope, receive, send)
            
            # Record timing
            duration = time.time() - start_time
            path = scope.get("path", "unknown")
            method = scope.get("method", "unknown")
            
            perf_monitor.record_timing(f"http.{method}.{path}", duration)
            
            # Log slow requests
            if duration > 2.0:
                logger.warning("Slow HTTP request", extra={
                    "method": method,
                    "path": path,
                    "duration": duration
                })
        else:
            await self.app(scope, receive, send)

# Utility functions
def get_performance_report() -> Dict[str, Any]:
    """Get comprehensive performance report."""
    return {
        'operation_stats': {op: perf_monitor.get_stats(op) for op in perf_monitor.metrics.keys()},
        'cache_stats': {
            'query_cache': {'size': len(query_cache), 'maxsize': query_cache.maxsize},
            'config_cache': {'size': len(config_cache), 'maxsize': config_cache.maxsize},
            'metrics_cache': {'size': len(metrics_cache), 'maxsize': metrics_cache.maxsize}
        }
    }

def clear_all_caches():
    """Clear all performance caches."""
    query_cache.clear()
    config_cache.clear()
    metrics_cache.clear()
    logger.info("All performance caches cleared")
