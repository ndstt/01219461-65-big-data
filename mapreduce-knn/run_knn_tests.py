#!/usr/bin/env python3
"""
KNN MapReduce Performance Testing Script
Tests various configurations of mappers, reducers, and block sizes.
"""

import subprocess
import time
import re
import os
import json
import csv
from datetime import datetime

# Configuration
KNN_JAR = '/home/hadoop/homework/mapreduce-knn/knn.jar'
RESULTS_DIR = '/home/hadoop/homework/mapreduce-knn/results'
HDFS_BASE = '/knn/performance_test'
PARAMS_PATH = f'{HDFS_BASE}/knnParams.txt'

# Test parameters
DATA_FILES = ['knn_6_1000.csv', 'knn_6_10000.csv', 'knn_6_50000.csv', 'knn_6_100000.csv']
BLOCK_SIZES = ['64MB', '128MB', '256MB']
REDUCERS = [2, 4, 8]

# File sizes in bytes
FILE_SIZES = {
    'knn_6_1000.csv': 119432,
    'knn_6_10000.csv': 1193107,
    'knn_6_50000.csv': 1193107,
    'knn_6_100000.csv': 11915930
}

def run_command(cmd):
    """Run command and return stdout, stderr, returncode"""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout, result.stderr, result.returncode

def parse_job_stats(stderr):
    """Parse MapReduce job statistics from stderr"""
    stats = {
        'map_tasks': 0,
        'reduce_tasks': 0,
        'bytes_read': 0,
        'bytes_written': 0,
    }
    
    map_match = re.search(r'Launched map tasks=(\d+)', stderr)
    reduce_match = re.search(r'Launched reduce tasks=(\d+)', stderr)
    bytes_read_match = re.search(r'Bytes Read=(\d+)', stderr)
    bytes_written_match = re.search(r'Bytes Written=(\d+)', stderr)
    
    if map_match:
        stats['map_tasks'] = int(map_match.group(1))
    if reduce_match:
        stats['reduce_tasks'] = int(reduce_match.group(1))
    if bytes_read_match:
        stats['bytes_read'] = int(bytes_read_match.group(1))
    if bytes_written_match:
        stats['bytes_written'] = int(bytes_written_match.group(1))
    
    return stats

def run_knn_job(input_path, output_path, num_reducers, num_mappers_target):
    """Run KNN MapReduce job and return results"""
    # Remove output directory
    run_command(f'hdfs dfs -rm -r -f {output_path} 2>/dev/null')
    
    # Build command with split size to control mappers
    # We use split.maxsize to control number of mappers
    split_configs = ''
    if num_mappers_target > 1:
        # For small files, set a small split size to get more mappers
        max_split = 1024 * 1024  # 1MB to force more splits
        split_configs = f'-D mapreduce.input.fileinputformat.split.maxsize={max_split}'
    
    cmd = f'hadoop jar {split_configs} {KNN_JAR} KnnPattern {input_path} {output_path} {PARAMS_PATH} {num_reducers}'
    
    print(f"  Running: hadoop jar ... {input_path.split('/')[-1]} reducers={num_reducers}")
    
    start_time = time.time()
    stdout, stderr, rc = run_command(cmd)
    exec_time = time.time() - start_time
    
    success = rc == 0
    
    # Get result
    result = None
    if success:
        out, _, _ = run_command(f'hdfs dfs -cat {output_path}/part-r-00000 2>/dev/null')
        result = out.strip()
    
    stats = parse_job_stats(stderr)
    
    return exec_time, success, result, stats

def format_time(seconds):
    if seconds < 60:
        return f"{seconds:.2f}s"
    else:
        mins = int(seconds // 60)
        secs = seconds % 60
        return f"{mins}m {secs:.2f}s"

def format_size(bytes_size):
    if bytes_size < 1024:
        return f"{bytes_size}B"
    elif bytes_size < 1024**2:
        return f"{bytes_size/1024:.2f}KB"
    else:
        return f"{bytes_size/(1024**2):.2f}MB"

def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    
    all_results = []
    test_counter = 0
    total_tests = len(DATA_FILES) * len(BLOCK_SIZES) * len(REDUCERS)
    
    print("="*80)
    print("KNN MapReduce Performance Testing")
    print("="*80)
    print(f"Total tests to run: {total_tests}")
    print(f"Data files: {DATA_FILES}")
    print(f"Block sizes: {BLOCK_SIZES}")
    print(f"Reducers: {REDUCERS}")
    print("="*80)
    
    for data_file in DATA_FILES:
        record_count = int(data_file.split('_')[2].replace('.csv', ''))
        file_size = FILE_SIZES.get(data_file, 0)
        
        for block_size in BLOCK_SIZES:
            input_path = f'{HDFS_BASE}/data_{block_size}/{data_file}'
            
            for num_reducers in REDUCERS:
                test_counter += 1
                output_path = f'{HDFS_BASE}/output/{data_file}_{block_size}_r{num_reducers}'
                
                print(f"\n[{test_counter}/{total_tests}] Data: {data_file}, Block: {block_size}, Reducers: {num_reducers}")
                
                # Target mappers based on block size index
                mapper_target = BLOCK_SIZES.index(block_size) + 2
                
                exec_time, success, result, stats = run_knn_job(
                    input_path, output_path, num_reducers, mapper_target
                )
                
                throughput = record_count / exec_time if exec_time > 0 else 0
                mb_per_sec = (file_size / (1024**2)) / exec_time if exec_time > 0 else 0
                
                test_result = {
                    'data_file': data_file,
                    'records': record_count,
                    'file_size': file_size,
                    'file_size_str': format_size(file_size),
                    'block_size': block_size,
                    'num_reducers': num_reducers,
                    'num_mappers': stats['map_tasks'],
                    'execution_time': round(exec_time, 2),
                    'execution_time_str': format_time(exec_time),
                    'success': success,
                    'result': result,
                    'bytes_read': stats['bytes_read'],
                    'bytes_written': stats['bytes_written'],
                    'throughput_records_per_sec': round(throughput, 2),
                    'throughput_mb_per_sec': round(mb_per_sec, 4)
                }
                all_results.append(test_result)
                
                status = "✓" if success else "✗"
                print(f"  {status} Time: {format_time(exec_time)}, Mappers: {stats['map_tasks']}, Result: {result}")
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save as JSON
    json_file = f'{RESULTS_DIR}/knn_results_{timestamp}.json'
    with open(json_file, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"\n✓ Results saved to: {json_file}")
    
    # Save as CSV
    csv_file = f'{RESULTS_DIR}/knn_results_{timestamp}.csv'
    if all_results:
        keys = all_results[0].keys()
        with open(csv_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(all_results)
    print(f"✓ Results saved to: {csv_file}")
    
    # Save latest copy
    with open(f'{RESULTS_DIR}/knn_results_latest.json', 'w') as f:
        json.dump(all_results, f, indent=2)
    with open(f'{RESULTS_DIR}/knn_results_latest.csv', 'w', newline='') as f:
        if all_results:
            writer = csv.DictWriter(f, fieldnames=all_results[0].keys())
            writer.writeheader()
            writer.writerows(all_results)
    
    print("\n" + "="*80)
    print("Testing complete!")
    print("="*80)
    
    return all_results

if __name__ == '__main__':
    main()
