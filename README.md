# SRA Distributed Processing Workflow

A Python-based distributed computing workflow for efficiently downloading and processing large-scale Sequence Read Archive (SRA) data. This project optimizes resource allocation across compute nodes using HTCondor workload management system.

## Features

- **Smart Query Generation**: Search SRA database with custom date ranges and keywords
- **Automated Load Balancing**: Distributes SRA downloads across compute nodes based on file sizes
- **Resource Optimization**: Dynamically adjusts CPU and disk allocation based on workload
- **HTCondor Integration**: Generates optimized submit files for distributed processing

## Prerequisites

- Python 3.9
- pysradb
- pandas
- loguru

## Installation

```bash
git clone [your-repository-url]
cd [repository-name]

pip install -r requirements.txt
```

## Configuration

Create a `config.json` file in the `config/` directory with the following structure:

```json
{
  "dates": {
    "start": "dd-mm-yyyy",
    "end": "dd-mm-yyyy"
  },
  "query": {
    "keyword1": "your-keyword",
    "keyword2": "your-keyword"
  },
  "process_configs": {
    "on_chtc": true,
    "cpu_per_node": 8,
    "max_cpu_request": 32,
    "minimum_submissions_for_balancing": 10
  },
  "directory": {
    "output_results": "path/to/output"
  },
  "files": {
    "sra_list_folder": "sras_to_process",
    "sra_query_file": "sra_queue.txt"
  }
}
```

## Usage

1. Configure your search parameters and resource requirements in `config.json`
2. Run the workflow:

```bash
python3 -m sra_dispatch
```

The workflow will:
1. Query the SRA database based on your parameters
2. Balance the workload across available compute nodes
3. Generate HTCondor submit files
4. Create node-specific SRA lists in the `sras_to_process/` directory

## How It Works

### 1. Query Generation
The `query_SRA_for_size_df` function queries the SRA database using specified date ranges and keywords, returning a DataFrame of accessions and file sizes.

### 2. Load Balancing
The `balance_nodes` function:
- Calculates optimal disk space allocation per node
- Distributes SRA downloads based on file sizes
- Adjusts CPU allocation based on workload distribution
- Generates node-specific SRA lists

### 3. Submit File Generation
The workflow generates HTCondor submit files with computed resource requirements and configuration parameters.

## Error Handling

- Validates date formats and ranges
- Checks minimum submission thresholds
- Ensures balanced node allocation
- Prevents empty partitions through dynamic adjustment

## Notes

- Set `on_chtc: false` in config for local, non-HTCondor execution
- File sizes include a 20x multiplier for fasterq-dump processing
- The system automatically adjusts CPU allocation when nodes are underutilized
