import json
import os
import shutil
import textwrap
import time
from datetime import date, datetime, timedelta

import pandas as pd
from pysradb.search import SraSearch


def generate_SRR_size_df(configs:dict) -> pd.DataFrame:
    """
    Build a query into SRA using config file specified dates. The dates are called start and end and are from a chronological
    perspective. Start means the day further back in time and end is more recent in time. All of the values handled by
    this function are the query and dates and a check of minimum size of submissions to enable balancing. End date set
    to today will have one day subtracted to prevent downloading midday and missing later submissions.

    Output of this function will be a pandas dataframe with two columns. The SRR/ERR run name along with
    the size of the sequencing files that we will download later. This dataframe will then be used in
    the next function called balance_nodes will dynamically balance the distributed computing requests.
    """
    # Handle "today" and valid date strings for the start date
    if configs["dates"]["start"] == "today":
        start_date = date.today().strftime("%d-%m-%Y")
    else:
        # Try parsing the start date
        try:
            start_date = datetime.strptime(configs["dates"]["start"], "%d-%m-%Y").strftime("%d-%m-%Y")
        except ValueError as e:
            raise ValueError(f"Error parsing start date: {e}. Ensure it follows the format 'dd-mm-yyyy'.")

    # Handle "today" for the end date
    if configs["dates"]["end"] == "today":
        end_date = (date.today() - timedelta(days=1)).strftime("%d-%m-%Y")
    else:
        try:
            end_date = datetime.strptime(configs["dates"]["end"], "%d-%m-%Y").strftime("%d-%m-%Y")
        except ValueError as e:
            raise ValueError(f"Error parsing end date: {e}. Ensure it follows the format 'dd-mm-yyyy'.")

    # Assert that both dates are valid and make logical sense (start date <= end date)
    assert datetime.strptime(start_date, "%d-%m-%Y") <= datetime.strptime(end_date, "%d-%m-%Y"), \
        f"Start date {start_date} cannot be after end date {end_date}."

    print(f"Start date: {start_date}, End date: {end_date}")

    # Generate the query using corrected f-string syntax (single quotes inside f-string)
    # FIXME rename this
    bioproject_query = SraSearch(
        query=f'{configs["query"]["keyword1"]} {configs["query"]["keyword2"]}',
        publication_date=f"{start_date}:{end_date}",
        verbosity=2,
        return_max=100000,
    )

    # Execute the query
    bioproject_query.search()

    # We can see the SRX numbers of newly published bioprojects AKA last entries and only run those!!!
    bioproject_query.get_df().to_csv("ww_sars-cov-2_meta.csv", sep="\t")

    SRR_with_sizes = bioproject_query.get_df()[["run_1_accession", "run_1_size"]]

    # Check step to ensure that load balancing makes sense, exits early if condition from config.json
    # file is met
    if len(SRR_with_sizes) < configs["process_configs"]["minimum_submissions_for_balancing"]:
        with open(f"{start_date}_{end_date}", "w") as f:
            # File is built that contains a sequential list of SRR's that can be used
            f.write("\n".join(set(SRR_with_sizes["run_1_accession"].tolist())))
        exit(f"Too few SRR submissions to balance, consider running locally \
             \n File: {start_date}_{end_date} was built and contains all run accessions\n\
            for use with modules/sra_cryptic_loop.sh -s {start_date}_{end_date}")

    return SRR_with_sizes

def balance_nodes(df:pd.DataFrame, configs:dict):
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

    nodes = configs["process_configs"]["max_cpu_request"]//configs["process_configs"]["cpu_per_node"]

    print(f"Nodes= {nodes}")

    # Convert run_1_size to numeric if it's stored as string
    # and handle any units (GB, MB, etc.) if present
    def convert_size(size_str):
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
            print(f"Unexpected size format: {size_str}")
            return 0

    # Convert sizes
    df["run_1_size"] = df["run_1_size"].apply(convert_size)

    # Calculate disk requirements (20x multiplier is necessary for fasterq-dump size
    df["disk_req"] = df["run_1_size"] * 20

    # Divide the total disk request for all and divide by nodes to find disk request per node
    avg_disk_per_node = (df["disk_req"].sum()/nodes)
    max_size_SRA = df["disk_req"].max()
    print("\nDisk Requirements Summary:")
    print(f"Max disk requirement: {max_size_SRA} B")
    print(f"Min disk requirement: {df['disk_req'].min()} B")
    print(f"Average disk per node: {avg_disk_per_node} B")

    # Check that we don't have a larger size of SRA submission than we have partitioned
    # Disk for as it will make an empty node
    if max_size_SRA > avg_disk_per_node:
        while max_size_SRA > avg_disk_per_node:
            print("Error, empty partition will exist, incrementing avg disk request by 1Gb")
            avg_disk_per_node += 1000000000
    else:
        print(f"Proceeding successfully with Avg disk request of {avg_disk_per_node}")

    print(f"Average disk request {avg_disk_per_node}")

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
            print(f'Group {group}: {df_sorted["disk_req"].iloc[0]}')
            df_sorted.drop(df_sorted.index[0], axis=0, inplace=True)
            break

        if node_size == 0:
            node_size += df_sorted["disk_req"].iloc[0]
            print(f'Group {group}: {df_sorted["disk_req"].iloc[0]}')
            curr_group.append(df_sorted["run_1_accession"].iloc[0])
            df_sorted.drop(df_sorted.index[0], axis=0, inplace=True)
            continue
        if node_size + df_sorted["disk_req"].iloc[-1] < avg_disk_per_node:
            print(f'Group {group}: {df_sorted["disk_req"].iloc[-1]}')
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
            print("Group size exceeded incrementing to new group")

    with open("sra_queue.txt", "a") as batch:
        batch.write("\n".join(all_groups))

    # Situation in which we have fewer partitions than we expected with our nodes
    # Reallocate our cpu request for more cpus

    # condition in which we have more than half unused nodes so we can double cpu
    if nodes//len(all_groups) >= 2:
        cpu_per_node = 2 * configs["process_configs"]["cpu_per_node"]
    else:
        cpu_per_node = configs["process_configs"]["cpu_per_node"]

    # Write out the process configs into another json for downstream processes to access.
    configs["process_configs"]["disk_request"] = avg_disk_per_node
    configs["process_configs"]["cpu_per_node"] = cpu_per_node

    # Write out a new json config file
    with open("submit_configs.json", "w") as submit_json_file:
        json.dump(configs, submit_json_file, indent=1)

    return configs


def populate_submit_file(configs:dict):
    """
    Creates an HTCondor submit file with parameters calculated from the configuration.

    This function generates a submit file for HTCondor that specifies:
    - Resource requirements (CPU, memory, disk)
    - File transfer configurations
    - Logging settings
    - Input file locations and arguments
    - Execution parameters

    The submit file enables distributed processing of SRA data by configuring:
    - Multi-node execution
    - Resource allocation and requirements
    - File staging and transfer
    - Job logging and monitoring

    Args:
        configs (dict): Configuration dictionary containing resource requirements,
                       file paths, and processing parameters

    Returns:
        None. Writes submit file to disk.
    """
    submit_file_content = textwrap.dedent(f"""
    executable = {configs['files']['sra_processing_program']}
    arguments = $(BATCH) {configs['directory']['output_results']}

    requirements = (OpSysMajorVer == 7) || (OpSysMajorVer == 8) || (OpSysMajorVer == 9) && (Target.HasCHTCStaging == true)
    _CONDOR_SCRATCH_DIR = {configs['directory']['fasterq-temp']}
    request_cpus = {configs['process_configs']['cpu_per_node']}
    request_memory = {configs['process_configs']['memory_request']}G
    request_disk = {configs['process_configs']['disk_request']//1000000000}G

    # file transfer options
    transfer_input_files = submit_configs.json, {configs['files']['static_files']}, {configs['files']['modules']}, {configs['files']['sra_processing_program']}, {configs['files']['sra_query_file']}, {configs['files']['sra_list_folder']}
    should_transfer_files = YES
    when_to_transfer_output = ON_EXIT

    # logging
    error = logs/$(Cluster).$(Process).err.txt
    output = logs/$(Cluster).$(Process).out.txt
    log = logs/$(Cluster).$(Process).log.txt

    queue BATCH from {configs['files']['sra_query_file']}
    """)

    # Write the .sub file to disk
    submit_file_path = "submit_file.sub"
    with open(submit_file_path, "w") as submit_file:
        submit_file.write(submit_file_content)

    print(f"Submit file written to: {submit_file_path}")



def read_config(json_file)-> dict:
    """
    Reads a JSON config file and returns a dictionary object.
    """

    with open(json_file) as file:
        config = json.load(file)
    return config

def main():
    """
    This program orchestrates a distributed computing workflow for downloading and processing SRA (Sequence Read Archive) data.
    It consists of several key components:

    1. Configuration Management (read_config):
       - Reads JSON configuration specifying dates, queries, and processing parameters

    2. SRA Query Generation (generate_SRR_size_df):
       - Builds query to search SRA database based on date ranges and keywords
       - Returns dataframe of SRR accessions and file sizes

    3. Node Balancing (balance_nodes):
       - Distributes SRA downloads across compute nodes based on file sizes
       - Optimizes CPU and disk allocation
       - Generates node-specific SRA lists

    4. HTCondor Submit Generation (populate_submit_file):
       - Creates HTCondor submit file with computed resource requirements
       - Configures file transfer and execution parameters

    The workflow enables efficient parallel processing of SRA data by:
    - Querying recent submissions within specified date ranges
    - Load balancing based on file sizes
    - Generating optimal HTCondor configurations
    - Managing resource allocation across compute nodes

    Required configs are specified in config.json including:
    - Date ranges for SRA queries
    - Search keywords
    - Process configurations (CPUs, memory, etc)
    - Directory paths
    - Minimum submission thresholds
    """
    program_start_time = time.time()

    config = read_config("config.json")

    # Make the output directory on chtc that will hold our processed files
    if config["process_configs"]["on_chtc"]:

        print("On chtc")

        # Will throw an error if dir already exists
        os.makedirs(config["directory"]["output_results"], exist_ok=False)

    else:
        print("Not on chtc")

    srr_with_size = generate_SRR_size_df(config)

    submit_configs = balance_nodes(srr_with_size, config)

    populate_submit_file(submit_configs)

    end_time = time.time()
    ex_time = end_time-program_start_time
    print(f"\n\nTotal Time of program: {ex_time}")

if __name__ == "__main__":
    main()
