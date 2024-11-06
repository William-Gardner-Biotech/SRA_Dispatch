#!/usr/bin/env python

from argparse import ArgumentParser, ArgumentTypeError
# import sys
import os
import json
from subprocess import PIPE, run as subprocess_run
import pandas as pd
from pathlib import Path

def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise ArgumentTypeError('Boolean value expected.')


def parse_process_arrays_args(parser: ArgumentParser):
    """Parses the python script arguments from bash and makes sure files/inputs are valid"""
    parser.add_argument('--submit_username',
                        type=str,
                        help='username for submit_server',
                        required=True)
    parser.add_argument('--submit_server',
                        type=str,
                        help='submit_server',
                        required=True)
    parser.add_argument('--outpath_username',
                        type=str,
                        help='outpath username for submit_server',
                        required=True)
    parser.add_argument('--outpath_server',
                        type=str,
                        help='outpath_server',
                        required=True)
    parser.add_argument('--ssh_connection_dir',
                        type=str,
                        help='/User/username/.ssh or /home/username/.ssh',
                        required=True)
    parser.add_argument('--chtc_completed',
                        type=str,
                        help='where_chtc_submits',
                        required=True)
    parser.add_argument('--chtc_module_out_dir',
                        type=str,
                        help='chtc_module_out_dir dir',
                        required=True)
    parser.add_argument('--local_module_out_dir',
                        type=str,
                        help='local_module_out_dir dir',
                        required=True)
    parser.add_argument('--chtc_sample_dir',
                        type=str,
                        help='chtc_sample_dir dir',
                        required=True),
    parser.add_argument('--local_sample_dir',
                        type=str,
                        help='local_sample_dir dir',
                        required=True)
    parser.add_argument('--local_stage_sra_dir',
                        type=str,
                        help='local_stage_sra_dir dir',
                        required=True
                        )
    parser.add_argument('--node_limit',
                        type=int,
                        help='Node limit for simultaneous launch',
                        default=20,
                        required=False)
    parser.add_argument('--files_per_node',
                        type=int,
                        help='Node limit for simultaneous launch',
                        default=30,
                        required=False)
    parser.add_argument('--size_limit_gb',
                        type=int,
                        help='Node limit for simultaneous launch',
                        default=5,
                        required=False)
    parser.add_argument('--ready_path',
                        type=str,
                        help='ready_path',
                        required=True)
    parser.add_argument('--completed_path',
                        type=str,
                        help='completed_path',
                        required=True)
    parser.add_argument('--status_dir',
                        type=str,
                        help='status_dir',
                        required=True)
    parser.add_argument('--sra_tracking_dir',
                        type=str,
                        help='status_dir',
                        required=True)




def get_process_arrays_args():
    """	Inputs arguments from bash
    Gets the arguments, checks requirements, returns a dictionary of arguments
    Return: args - Arguments as a dictionary
    """
    parser = ArgumentParser()
    parse_process_arrays_args(parser)
    args = parser.parse_args()
    return args


args = get_process_arrays_args()
chtc_completed = args.chtc_completed
local_stage_sra_dir = args.local_stage_sra_dir
all_files = os.path.join(local_stage_sra_dir, 'SRA_meta_run.tsv')
sra_tracking_dir = args.sra_tracking_dir
submit_username = args.submit_username
submit_server = args.submit_server
outpath_username = args.outpath_username
outpath_server = args.outpath_server
chtc_module_out_dir = args.chtc_module_out_dir
local_module_out_dir = args.local_module_out_dir
chtc_sample_dir = args.chtc_sample_dir
node_limit = args.node_limit
files_per_node = args.files_per_node
local_sample_dir = args.local_sample_dir
ssh_connection_dir = args.ssh_connection_dir
size_limit_gb = args.size_limit_gb
ready_path = args.ready_path
completed_path = args.completed_path
status_dir = args.status_dir

df_all = pd.read_csv(all_files, sep='\t', header=None)
df_all = df_all[~df_all[0].isnull()]
df_all = df_all[~df_all[6].isnull()]
df_all[6] = pd.to_numeric(df_all[6])
df_all = df_all[df_all[6] > 1]
remaining_list = set(df_all[0].unique())
print(len(remaining_list))
# print(remaining_list)
if len(remaining_list) < 1:
    exit(0)

os.system('rsync -e \'ssh -o ControlPath="{0}/%L-%r@%h:%p"\' -aP {1}@{2}:{3} {4}'.format(ssh_connection_dir,
                                                                                         submit_username,
                                                                                         submit_server,
                                                                                         chtc_completed,
                                                                                         local_stage_sra_dir))

chtc_submit_filename = os.path.basename(chtc_completed)
submit_local_path = os.path.join(local_stage_sra_dir, chtc_submit_filename)

if not os.path.exists(submit_local_path):
    print("Warning: condor_q_held.json Does not exist")
    chtc_json = []
else:
    try:
        with open(submit_local_path) as f_in:
            chtc_json = json.load(f_in)
    except ValueError:
        print("Warning: json is blank (none held) or bad format")
        chtc_json = []

full_sample_list = []
downloaded_list = []
full_downloaded_list = []
sample_list = []
downloaded_group_list = []
full_indv_sample_list = []
full_downloaded_filepath = os.path.join(sra_tracking_dir, 'downloaded_list.txt')
local_downloaded_filepath = os.path.join(local_stage_sra_dir, 'downloaded_list.txt')
local_downloaded_group_filepath = os.path.join(local_stage_sra_dir, 'downloaded_group_list.txt')
if os.path.exists(local_downloaded_group_filepath):
    with open(local_downloaded_group_filepath, 'r') as f:
        for line in f:
            line = line.strip()
            downloaded_group_list.append(line)

if os.path.exists(full_downloaded_filepath):
    with open(full_downloaded_filepath, 'r') as f:
        for line in f:
            line = line.strip()
            full_downloaded_list.append(line)

if os.path.exists(local_downloaded_filepath):
    with open(local_downloaded_filepath, 'r') as f:
        for line in f:
            line = line.strip()
            downloaded_list.append(line)

fullpath_dict = {}

if len(chtc_json) > 0:
    for chtc_json_i in chtc_json:
        iwd = chtc_json_i['iwd']
        full_sample_list = full_sample_list + chtc_json_i['sample_list']

        for sample_i in chtc_json_i['sample_list']:
            fullpath_dict[sample_i] = os.path.join(chtc_json_i['iwd_results_dir'],
                                                   '{0}_out.tar.gz'.format(sample_i[:-4]))
    ready_list = list(set(full_sample_list) - set(downloaded_group_list))
    print(ready_list)
    if len(ready_list) > 0:
        for ready_i in ready_list:
            path_i = fullpath_dict[ready_i]
            os.system('rsync -e \'ssh -o ControlPath="{0}/%L-%r@%h:%p"\' -aP {1}@{2}:{3} {4}'.format(ssh_connection_dir,
                                                                                                     outpath_username,
                                                                                                     outpath_server,
                                                                                                     path_i,
                                                                                                     local_module_out_dir))
            if os.path.exists(os.path.join(local_module_out_dir, os.path.basename(path_i))):
                downloaded_group_list.append(ready_i)
                with open(local_downloaded_group_filepath, 'a') as f:
                    f.write('{0}\n'.format(ready_i))
                with open(os.path.join(local_sample_dir, ready_i), 'r') as f:
                    with open(local_downloaded_filepath, 'a') as fw:
                        for line in f:
                            line = line.strip()
                            fw.write('{0}\n'.format(line))
                            downloaded_list.append(line)
                with open(os.path.join(local_sample_dir, ready_i), 'r') as f:
                    with open(full_downloaded_filepath, 'a') as fw:
                        for line in f:
                            line = line.strip()
                            fw.write('{0}\n'.format(line))

                os.system(
                    'ssh -o ControlPath="{0}/%L-%r@%h:%p" {1}@{2} "rm -f {3}"'.format(ssh_connection_dir,
                                                                                      outpath_username,
                                                                                      outpath_server,
                                                                                      path_i))
                os.system(
                    'ssh -o ControlPath="{0}/%L-%r@%h:%p" {1}@{2} "rm -f {3}"'.format(ssh_connection_dir,
                                                                                      outpath_username,
                                                                                      outpath_server,
                                                                                      os.path.join(
                                                                                          chtc_sample_dir,
                                                                                          ready_i)))
node_count_cmd = 'ssh -o ControlPath="{0}/%L-%r@%h:%p" {1}@{2} "ls {3} | wc -l"'.format(ssh_connection_dir,
                                                                                      outpath_username,
                                                                                      outpath_server,
                                                                                      chtc_sample_dir)

node_count_out = subprocess_run([node_count_cmd],
                                  shell=True,
                                  stdout=PIPE,
                                  stderr=PIPE)

check_submit_script_err = node_count_out.stderr.decode('utf-8').strip()
check_submit_script_out = node_count_out.stdout.decode('utf-8').strip()
if len(check_submit_script_err) > 0:
    print('cannot properly connect')
    exit(0)
node_count = node_limit

if len(check_submit_script_out) > 0:
    node_count = int(check_submit_script_out)
# downloaded_group_list = [x[:-4] for x in downloaded_group_list if x.endswith('.txt')]
# add samples
remaining_post_download = list(set(remaining_list) - set(downloaded_list))
# print(downloaded_list[0])
remaining_sample_list = []
pending_sample_list_cmd = 'ssh -o ControlPath="{0}/%L-%r@%h:%p" {1}@{2} "ls {3}"'.format(ssh_connection_dir,
                                                                                         outpath_username,
                                                                                         outpath_server,
                                                                                         chtc_sample_dir)
print(pending_sample_list_cmd)
sample_count_out = subprocess_run([pending_sample_list_cmd],
                                  shell=True,
                                  stdout=PIPE,
                                  stderr=PIPE)
check_submit_script_err = sample_count_out.stderr.decode('utf-8').strip()
check_submit_script_out = sample_count_out.stdout.decode('utf-8').strip()
if len(check_submit_script_err) > 0:
    print('cannot properly connect')
    exit(0)
# print(check_submit_script_out)
pending_sample_group_list = check_submit_script_out.split('\n')
# print(pending_sample_list)
pending_sample_list = []
print(pending_sample_list_cmd)
for sample_i in pending_sample_group_list:
    if len(sample_i.strip()) < 1:
        continue
    # print('sample name', sample_i)
    with open(os.path.join(local_sample_dir, sample_i), 'r') as f:
        for line in f:
            line = line.strip()
            pending_sample_list.append(line)
print(len(pending_sample_list))
remaining_post_download_pending = list(set(remaining_post_download) - set(pending_sample_list))
remaining_post_download_pending.sort(reverse=True)
df_all_remain = df_all[df_all[0].isin(remaining_post_download_pending)]
df_all_remain.sort_values(by=[6], ascending=True, inplace=True)
remaining_post_download_pending = list(df_all_remain[0])
# df_all_remain.to_csv('/Volumes/T8/remainin_sra.csv', index=False)
print(len(set(remaining_post_download)), len(set(pending_sample_list)))
print('--- New Node Count ---', node_limit - node_count)


def calc_files_per_node(df_rem, fpn_limit, size_limit_in_gb=5):
    fpn_count = 0
    file_size = 0
    size_limit = size_limit_in_gb * 1000000000
    for unused_, row in df_rem.iterrows():
        fpn_count += 1
        file_size += row[6]
        if file_size > size_limit:
            break
        if fpn_count >= fpn_limit:
            break
    return fpn_count

print(node_limit, node_count, len(remaining_post_download_pending) )

if (node_count < node_limit) and (len(remaining_post_download_pending) > 0):
    new_node_count = node_limit - node_count
    for i in range(0, new_node_count):
        fpn = calc_files_per_node(df_all_remain, files_per_node, size_limit_in_gb=size_limit_gb)
        print('Files per node {0}'.format(fpn))
        new_sample_list = remaining_post_download_pending[fpn * i:fpn * (i + 1)]
        new_sample_list_string = '\n'.join(new_sample_list)
        sample_path_i = os.path.join(local_sample_dir, '{0}.txt'.format(new_sample_list[0]))
        with open(sample_path_i, 'w') as f:
            f.write(new_sample_list_string)
        os.system('rsync -e \'ssh -o ControlPath="{0}/%L-%r@%h:%p"\' -aP {1} {2}@{3}:{4}'.format(ssh_connection_dir,
                                                                                                 sample_path_i,
                                                                                                 outpath_username,
                                                                                                 outpath_server,
                                                                                                 chtc_sample_dir))


if len(remaining_post_download) < 1:
    print('touching completed')
    Path(completed_path).touch()
    Path(ready_path).touch()
