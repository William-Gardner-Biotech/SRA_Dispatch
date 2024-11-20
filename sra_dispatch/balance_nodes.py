from __future__ import annotations

import json
import os
import shutil

import pandas as pd
from loguru import logger


def balance_nodes(df: pd.DataFrame, configs: dict):
    """Balances the SRA downloads across available compute nodes based on file sizes.

    This function takes a DataFrame containing SRA run accessions and their file sizes,
    along with configuration parameters, to optimally distribute the download and
    processing workload across compute nodes. The balancing process aims to:

    1. Allocate downloads evenly based on total disk space requirements
    2. Ensure no node gets overwhelmed by large files
    3. Optimize CPU utilization by adjusting nodes based on workload
    4. Generate balanced SRR lists for each compute node

    Args:
        df (pd.DataFrame): DataFrame containing run accessions and file sizes
        configs (dict): Configuration dictionary with processing parameters

    Returns:
        dict: Updated configs with balanced node parameters

    The function creates files containing:
    - Individual SRA lists per node in the sras_to_process/ directory
    - A queue file listing all SRA list files
    - Updated configuration parameters in submit_configs.json
    """

    # First condition we seek to achieve is requesting X nodes that are simply the max_cpu_request / cpu_per_node

    # If we do that we would have X nodes but if we have only 2X submissions of varying sizes it is possible that there
    # is a maximum we cannot fit in this schema, so we now must decrease num of nodes to allocate more size partitioning
    # for the nodes

    nodes = (
        configs["process_configs"]["max_cpu_request"]
        // configs["process_configs"]["cpu_per_node"]
    )

    logger.info(f"Nodes= {nodes}")

    # Convert run_1_size to numeric if it's stored as string
    # and handle any units (GB, MB, etc.) if present
    def convert_size(size_str: str) -> float | int:
        if pd.isna(size_str):
            return 0
        size_str = str(size_str).strip()
        try:
            # If it's already a number, just convert to float
            return float(size_str)
        except ValueError:
            # Handle strings with units
            if "GB" in size_str:
                return float(size_str.replace("GB", "")) * 1024  # Convert to MB
            if "MB" in size_str:
                return float(size_str.replace("MB", ""))
            if "KB" in size_str:
                return float(size_str.replace("KB", "")) / 1024
            logger.warning(f"Unexpected size format: {size_str}")
            return 0

    # Convert sizes
    df["run_1_size"] = df["run_1_size"].apply(convert_size)

    # Calculate disk requirements (20x multiplier is necessary for fasterq-dump size
    df["disk_req"] = df["run_1_size"] * 20

    # Divide the total disk request for all and divide by nodes to find disk request per node
    avg_disk_per_node = df["disk_req"].sum() / nodes
    max_size_SRA = df["disk_req"].max()
    logger.info("\nDisk Requirements Summary:")
    logger.info(f"Max disk requirement: {max_size_SRA} B")
    logger.info(f"Min disk requirement: {df['disk_req'].min()} B")
    logger.info(f"Average disk per node: {avg_disk_per_node} B")

    # Check that we don't have a larger size of SRA submission than we have partitioned
    # Disk for as it will make an empty node
    if max_size_SRA > avg_disk_per_node:
        while max_size_SRA > avg_disk_per_node:
            logger.warning(
                "Error, empty partition will exist, incrementing avg disk request by 1Gb",
            )
            avg_disk_per_node += 1000000000
    else:
        logger.success(
            f"Proceeding successfully with Avg disk request of {avg_disk_per_node}",
        )

    logger.info(f"Average disk request {avg_disk_per_node}")

    df_sorted = df.sort_values(by="disk_req", ascending=False)

    # init X node bins

    group = 0
    curr_group = []
    all_groups = []
    node_size = 0

    # Check if the folder exists and remove it along with its contents
    if os.path.exists(configs["files"]["sra_list_folder"]):
        shutil.rmtree(configs["files"]["sra_list_folder"])

    # Check if the file exists and remove it
    if os.path.exists(configs["files"]["sra_query_file"]):
        os.remove(configs["files"]["sra_query_file"])

    # Init the new sra_file_folder
    os.mkdir(configs["files"]["sra_list_folder"])

    while len(df_sorted) > 0:
        if len(df_sorted) == 1:
            logger.info(f'Group {group}: {df_sorted["disk_req"].iloc[0]}')
            df_sorted.drop(df_sorted.index[0], axis=0, inplace=True)
            break

        if node_size == 0:
            node_size += df_sorted["disk_req"].iloc[0]
            logger.info(f'Group {group}: {df_sorted["disk_req"].iloc[0]}')
            curr_group.append(df_sorted["run_1_accession"].iloc[0])
            df_sorted.drop(df_sorted.index[0], axis=0, inplace=True)
            continue
        if node_size + df_sorted["disk_req"].iloc[-1] < avg_disk_per_node:
            logger.info(f'Group {group}: {df_sorted["disk_req"].iloc[-1]}')
            curr_group.append(df_sorted["run_1_accession"].iloc[-1])
            df_sorted.drop(df_sorted.index[-1], axis=0, inplace=True)
            node_size += df_sorted["disk_req"].iloc[-1]
        else:
            group += 1
            node_size = 0

            # Write the SRR group to a txt formatted \n delimited file
            with open(f"sras_to_process/SRA_set_{group}", "a") as outfile:
                outfile.write("\n".join(curr_group))
            all_groups.append(f"sras_to_process/SRA_set_{group}")
            curr_group = []
            logger.info("Group size exceeded incrementing to new group")

    with open("sra_queue.txt", "a") as batch:
        batch.write("\n".join(all_groups))

    # Situation in which we have fewer partitions than we expected with our nodes
    # Reallocate our cpu request for more cpus

    # condition in which we have more than half unused nodes so we can double cpu
    if nodes // len(all_groups) >= 2:
        cpu_per_node = 2 * configs["process_configs"]["cpu_per_node"]
    else:
        cpu_per_node = configs["process_configs"]["cpu_per_node"]

    # Write out the process configs into another json for downstream processes to access.
    configs["process_configs"]["disk_request"] = avg_disk_per_node
    configs["process_configs"]["cpu_per_node"] = cpu_per_node

    # Write out a new json config file
    with open("configs/submit_configs.json", "w") as submit_json_file:
        json.dump(configs, submit_json_file, indent=1)

    return configs
