#!/usr/bin/env python3
"""
Test script for the parallel execution implementation in maschinensucher scraper.

This script demonstrates the performance difference between sequential and parallel
crawling by testing with a known dealer and multiple pages.
"""

import asyncio
import logging
import time
from maschinensucher.maschinensucher import (
    grab_dealer_machines,
    grab_dealer_machines_parallel,
    extract_dealer_id_from_link
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def test_parallel_vs_sequential():
    """
    Test and compare parallel vs sequential execution.
    """
    # Test parameters - you can adjust these
    test_dealer_id = "46184"  # Example dealer ID
    test_category_code = "3"  # Example category
    test_pages = 3  # Number of pages to test with
    
    print(f"\n=== Testing Parallel vs Sequential Execution ===")
    print(f"Dealer ID: {test_dealer_id}")
    print(f"Category: {test_category_code}")
    print(f"Pages: {test_pages}")
    print("=" * 60)
    
    # Test sequential execution
    print(f"\n📋 Testing SEQUENTIAL execution for {test_pages} pages...")
    start_time = time.time()
    
    try:
        sequential_results = await grab_dealer_machines(
            dealer_id=test_dealer_id,
            category_code=test_category_code,
            num_pages=test_pages
        )
        sequential_duration = time.time() - start_time
        sequential_count = len(sequential_results)
        
        print(f"✅ Sequential: {sequential_count} machines in {sequential_duration:.2f}s")
        
    except Exception as e:
        print(f"❌ Sequential failed: {str(e)}")
        sequential_duration = float('inf')
        sequential_count = 0
        sequential_results = []
    
    # Wait a bit between tests
    await asyncio.sleep(2)
    
    # Test parallel execution
    print(f"\n🚀 Testing PARALLEL execution for {test_pages} pages...")
    start_time = time.time()
    
    try:
        parallel_results = await grab_dealer_machines_parallel(
            dealer_id=test_dealer_id,
            category_code=test_category_code,
            num_pages=test_pages
        )
        parallel_duration = time.time() - start_time
        parallel_count = len(parallel_results)
        
        print(f"✅ Parallel: {parallel_count} machines in {parallel_duration:.2f}s")
        
    except Exception as e:
        print(f"❌ Parallel failed: {str(e)}")
        parallel_duration = float('inf')
        parallel_count = 0
        parallel_results = []
    
    # Performance comparison
    print(f"\n📊 PERFORMANCE COMPARISON")
    print("=" * 40)
    print(f"Sequential: {sequential_duration:.2f}s ({sequential_count} items)")
    print(f"Parallel:   {parallel_duration:.2f}s ({parallel_count} items)")
    
    if sequential_duration > 0 and parallel_duration > 0:
        speedup = sequential_duration / parallel_duration
        improvement = ((sequential_duration - parallel_duration) / sequential_duration) * 100
        print(f"Speedup:    {speedup:.2f}x faster")
        print(f"Improvement: {improvement:.1f}% time reduction")
    
    # Data integrity check
    print(f"\n🔍 DATA INTEGRITY CHECK")
    print("=" * 30)
    
    if sequential_count == parallel_count:
        print(f"✅ Both methods extracted {sequential_count} items")
    else:
        print(f"⚠️  Different counts: Sequential={sequential_count}, Parallel={parallel_count}")
    
    # Show sample data structure (first item from each)
    if sequential_results and parallel_results:
        print(f"\n📋 Sample data structure comparison:")
        seq_keys = set(sequential_results[0].keys()) if sequential_results else set()
        par_keys = set(parallel_results[0].keys()) if parallel_results else set()
        
        if seq_keys == par_keys:
            print("✅ Data structures match")
        else:
            print("⚠️  Data structures differ")
            print(f"Sequential keys: {sorted(seq_keys)}")
            print(f"Parallel keys: {sorted(par_keys)}")

async def test_single_vs_multiple_pages():
    """
    Test performance with different page counts.
    """
    test_dealer_id = "46184"
    test_category_code = "3"
    
    print(f"\n=== Page Count Performance Test ===")
    
    for num_pages in [1, 2, 3, 5]:
        print(f"\n🔬 Testing with {num_pages} page(s):")
        
        # Sequential test
        start_time = time.time()
        try:
            seq_results = await grab_dealer_machines(
                dealer_id=test_dealer_id,
                category_code=test_category_code,
                num_pages=num_pages
            )
            seq_duration = time.time() - start_time
            seq_count = len(seq_results)
            print(f"  Sequential: {seq_duration:.2f}s ({seq_count} items)")
        except Exception as e:
            print(f"  Sequential failed: {str(e)}")
            continue
        
        await asyncio.sleep(1)
        
        # Parallel test
        start_time = time.time()
        try:
            par_results = await grab_dealer_machines_parallel(
                dealer_id=test_dealer_id,
                category_code=test_category_code,
                num_pages=num_pages
            )
            par_duration = time.time() - start_time
            par_count = len(par_results)
            print(f"  Parallel:   {par_duration:.2f}s ({par_count} items)")
            
            if seq_duration > 0 and par_duration > 0:
                speedup = seq_duration / par_duration
                print(f"  Speedup:    {speedup:.2f}x")
        except Exception as e:
            print(f"  Parallel failed: {str(e)}")

async def main():
    """
    Main test function.
    """
    print("🧪 Parallel Execution Test Suite")
    print("=" * 50)
    
    try:
        # Basic comparison test
        await test_parallel_vs_sequential()
        
        # Page count performance test
        await test_single_vs_multiple_pages()
        
        print(f"\n✅ All tests completed successfully!")
        
    except Exception as e:
        logging.error(f"Test suite failed: {str(e)}")
        print(f"\n❌ Test suite failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
