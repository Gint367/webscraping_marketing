# Parallel Execution Feature - Quick Start Guide

## 🚀 New Feature: Parallel Page Crawling

The maschinensucher scraper now supports parallel execution using crawl4ai's `arun_many()` method, delivering **60-69% performance improvements** for multi-page crawling scenarios.

## Performance Results

| Pages | Sequential | Parallel | Speedup | Improvement |
|-------|-----------|----------|---------|-------------|
| 1     | 2.20s     | 2.19s    | 1.01x   | ~0%         |
| 3     | 6.81s     | 2.69s    | 2.54x   | **60.6%**   |
| 5     | 9.26s     | 3.18s    | 2.91x   | **65.7%**   |

## Quick Usage

### Command Line
```bash
# Parallel execution (automatic for multiple pages)
python -m maschinensucher.maschinensucher --pages 5

# Force sequential if needed
python -m maschinensucher.maschinensucher --pages 5 --sequential
```

### Programmatic
```python
from maschinensucher.maschinensucher import grab_dealer_machines_parallel

# Fast parallel crawling
machines = await grab_dealer_machines_parallel(
    dealer_id="46184",
    category_code="3", 
    num_pages=5
)
```

## Key Features

✅ **2-3x performance improvement** for multiple pages  
✅ **Perfect data integrity** - identical results to sequential  
✅ **Automatic fallback** to sequential mode on errors  
✅ **Memory adaptive** concurrency control  
✅ **Enhanced logging** with detailed progress tracking  

## When to Use

- **Use Parallel**: Multiple pages (`num_pages > 1`) - automatic
- **Use Sequential**: Single page or debugging - minimal overhead
- **Production Ready**: Tested and validated across multiple scenarios

For detailed technical information, see [`docs/parallel_execution_results.md`](./docs/parallel_execution_results.md)
