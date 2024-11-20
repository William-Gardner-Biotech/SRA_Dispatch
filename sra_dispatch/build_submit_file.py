import os
import textwrap

from loguru import logger


def build_submit_file(configs: dict) -> None:
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
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    submit_file_content = textwrap.dedent(f"""
    executable = {os.path.join(base_dir, configs['files']['sra_processing_program'])}
    arguments = $(BATCH) {configs['directory']['output_results']}

    requirements = (OpSysMajorVer == 7) || (OpSysMajorVer == 8) || (OpSysMajorVer == 9) && (Target.HasCHTCStaging == true)
    _CONDOR_SCRATCH_DIR = {configs['directory']['fasterq-temp']}
    request_cpus = {configs['process_configs']['cpu_per_node']}
    request_memory = {configs['process_configs']['memory_request']}G
    request_disk = {configs['process_configs']['disk_request']//1000000000}G

    # file transfer options
    transfer_input_files = {os.path.join(base_dir, 'config/submit_configs.json')}, {os.path.join(base_dir, configs['files']['static_files'])}, {os.path.join(base_dir, configs['files']['modules'])}, {os.path.join(base_dir, configs['files']['sra_processing_program'])}, {configs['files']['sra_query_file']}, {configs['files']['sra_list_folder']}
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

    logger.info(f"Submit file written to: {submit_file_path}")
