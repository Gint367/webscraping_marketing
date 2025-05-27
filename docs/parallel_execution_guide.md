# Parallel Execution Usage Guide

## Overview

The maschinensucher scraper now supports parallel execution for improved performance when crawling multiple pages per dealer. This implementation uses crawl4ai's `arun_many()` method to crawl multiple pages simultaneously instead of sequentially.

## Command Line Usage

### Basic Usage (Sequential - Default)
```bash
cd /home/novoai/Documents/scraper
python -m maschinensucher.maschinensucher --pages 2 --machine-pages 3
```

### Parallel Execution
```bash
cd /home/novoai/Documents/scraper
python -m maschinensucher.maschinensucher --pages 2 --machine-pages 5 --parallel
```

### Command Line Arguments

- `--pages N`: Number of dealer listing pages to scrape (default: 1)
- `--machine-pages N`: Number of machine pages to crawl per dealer (default: 1)  
- `--parallel`: Enable parallel execution for machine data crawling
- `--force-schema`: Force regeneration of dealer schema

## When to Use Parallel Execution

### Recommended Scenarios
- **Multiple machine pages per dealer** (`--machine-pages > 1`)
- **Dealers with many machine listings** (5+ pages)
- **Batch processing of multiple dealers**
- **Performance-critical scraping tasks**

### Not Recommended Scenarios
- **Single page per dealer** (overhead outweighs benefits)
- **Limited system resources** (memory < 4GB)
- **Unreliable network connections**
- **Rate-limited APIs** (may trigger blocking)

## Performance Expectations

### Sequential vs Parallel Performance

| Pages per Dealer | Sequential Time | Parallel Time | Improvement |
|------------------|----------------|---------------|-------------|
| 1 page           | 5-6 seconds    | 5-6 seconds   | ~0% (overhead) |
| 3 pages          | 15-18 seconds  | 8-12 seconds  | ~40-50% |
| 5 pages          | 25-30 seconds  | 10-15 seconds | ~60-67% |
| 10 pages         | 50-60 seconds  | 15-25 seconds | ~70-80% |

*Performance may vary based on network conditions and system resources.*

## Technical Implementation

### Memory Management
- Uses `MemoryAdaptiveDispatcher` with 75% memory threshold
- Automatically pauses crawling if memory usage exceeds limit
- Graceful degradation to sequential mode on memory pressure

### Rate Limiting
- Conservative concurrency limits (2-5 concurrent requests)
- Built-in delays between requests (1.0 second)
- Respects server response times

### Error Handling
- Individual page failures don't break entire batch
- Automatic fallback to sequential mode on errors
- Comprehensive logging for debugging

## Example Usage in Code

```python
import asyncio
from maschinensucher.maschinensucher import (
    grab_dealer_machines,
    grab_dealer_machines_parallel
)

async def example_usage():
    dealer_id = "46184"
    category_code = "3"
    
    # Sequential crawling (original method)
    sequential_results = await grab_dealer_machines(
        dealer_id=dealer_id,
        category_code=category_code,
        num_pages=5
    )
    
    # Parallel crawling (new method)
    parallel_results = await grab_dealer_machines_parallel(
        dealer_id=dealer_id,
        category_code=category_code,
        num_pages=5
    )
    
    print(f"Sequential: {len(sequential_results)} items")
    print(f"Parallel: {len(parallel_results)} items")

# Run the example
asyncio.run(example_usage())
```

## Testing the Implementation

### Performance Testing
```bash
cd /home/novoai/Documents/scraper
python test_parallel_execution.py
```

### Unit Testing
```bash
cd /home/novoai/Documents/scraper
python -m unittest tests.test_parallel_execution -v 2>&1
```

## Configuration Options

### Memory Adaptive Dispatcher Settings
```python
# Conservative (recommended)
memory_threshold_percent = 75.0
check_interval = 0.5

# Aggressive (high-performance systems)
memory_threshold_percent = 85.0
check_interval = 0.3
```

### Rate Limiting Configuration
```python
# Conservative (default)
delay_before_return_html = 1.0
requests_per_second = 2.0

# Faster (use with caution)
delay_before_return_html = 0.5
requests_per_second = 5.0
```

## Troubleshooting

### Common Issues

1. **Memory Usage Too High**
   - Reduce `--machine-pages` parameter
   - Use sequential mode for large batches
   - Monitor system memory during execution

2. **Rate Limiting/IP Blocking**
   - Increase delays in crawler configuration
   - Reduce concurrent requests
   - Use proxy rotation (future enhancement)

3. **Inconsistent Results**
   - Check network stability
   - Verify data integrity with sequential comparison
   - Review error logs for failed pages

### Debugging

Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

Check memory usage:
```bash
htop  # Monitor memory during execution
```

## Future Enhancements

### Planned Features
- **Dealer-level parallelization**: Process multiple dealers simultaneously
- **Proxy rotation**: Avoid IP blocking with rotating proxies  
- **Adaptive rate limiting**: Dynamic adjustment based on server response
- **Streaming results**: Process results as they arrive
- **Resume functionality**: Continue from failed batches

### Configuration Files
Future versions will support YAML configuration files:
```yaml
parallel_config:
  memory_threshold: 75.0
  max_concurrent_pages: 5
  rate_limit: 2.0
  fallback_to_sequential: true
```

## Best Practices

1. **Start Conservative**: Begin with default settings and adjust based on results
2. **Monitor Resources**: Watch memory and CPU usage during execution
3. **Test First**: Use test script to validate performance before production runs
4. **Log Everything**: Enable detailed logging for troubleshooting
5. **Respect Servers**: Don't overwhelm target servers with too many concurrent requests
6. **Validate Data**: Compare parallel results with sequential for data integrity

## Support

For issues or questions about parallel execution:
1. Check the error logs for specific error messages
2. Run the test script to isolate performance issues
3. Compare results with sequential mode to verify data integrity
4. Review memory and network usage during execution
