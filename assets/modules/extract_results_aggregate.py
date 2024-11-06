from argparse import ArgumentParser, ArgumentTypeError
# import sys
import os
import json
from subprocess import PIPE, run as subprocess_run
import pandas as pd


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
    parser.add_argument('--input_dir',
                        type=str,
                        help='where your results for chtc reside',
                        required=True)
    parser.add_argument('--untar_dir',
                        type=str,
                        help='where the extracted files will be stored',
                        required=True)
    parser.add_argument('--fpd',
                        type=int,
                        help='file limit per directory',
                        default=500,
                        required=False)


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
input_dir = args.input_dir
untar_dir = args.untar_dir
finished_tars_path = os.path.join(untar_dir, 'finished_extractions.csv')
fpd = args.fpd
# input_dir = '/Volumes/T7/serpent/sra_cryptic_lineages/SRA_221213/out_files/sra_cryptic'
# untar_dir = '/Volumes/T7/serpent/sra_cryptic_lineages/untar'
# finished_tars_path = '/Volumes/T7/serpent/sra_cryptic_lineages/untar/finished_extractions.csv'
# file limit per directory
out_tar_list = []
# for input_dir in input_dir_list:
#
# get file list

finished_tar_list = []
if os.path.exists(finished_tars_path):
    try:
        df = pd.read_csv(finished_tars_path, header=None)
        finished_tar_list = list(df[0])
    except:
        finished_tar_list = []

os.makedirs(untar_dir, exist_ok=True)

def get_file_counts(untar_dir):
    untar_list = os.listdir(untar_dir)

    def isint(x):
        try:
            int(x)
            return True
        except ValueError:
            return False
    untar_list = [x for x in untar_list if isint(x) ]
    # print(untar_list)
    fc_dict = {}
    for untar_subdir in untar_list:
        untar_list = os.listdir(os.path.join(untar_dir, untar_subdir))
        fc_dict[untar_subdir] = len(untar_list)
    return fc_dict




out_tar_list_i = os.listdir(input_dir)
out_tar_list_i = [os.path.join(input_dir,
                               x) for x in out_tar_list_i if x.endswith('_out.tar.gz') and not x.startswith('._')]
out_tar_list = out_tar_list + out_tar_list_i

# print(out_tar_list)
# print(len(out_tar_list))
#
filepath_list = []
print(len(out_tar_list))
i = 0
with open(finished_tars_path, 'a') as f:
    for tar_path_i in out_tar_list:
        if tar_path_i in finished_tar_list:
            continue
        get_file_list_cmd = 'tar -ztvf {0}'.format(tar_path_i)
        sample_count_out = subprocess_run([get_file_list_cmd],
                                          shell=True,
                                          stdout=PIPE,
                                          stderr=PIPE)
        check_submit_script_err = sample_count_out.stderr.decode('utf-8').strip()
        check_submit_script_out = sample_count_out.stdout.decode('utf-8').strip()
        result_list = check_submit_script_out.split('\n')
        print(len(result_list))
        result_file_count = len(result_list)
        files_extracted = False
        fc_dict = get_file_counts(untar_dir)
        max_dir = 0
        for untar_subdir, file_count in fc_dict.items():
            if int(untar_subdir) > max_dir:
                max_dir = int(untar_subdir)
            untar_dir_path = os.path.join(untar_dir, untar_subdir)
            if result_file_count + file_count < fpd:
                get_file_list_cmd = 'tar -zxvf {0}'.format(tar_path_i)
                print('Untar file {0} to {1}'.format(tar_path_i, untar_dir_path))
                sample_count_out = subprocess_run([get_file_list_cmd],
                                                  shell=True,
                                                  stdout=PIPE,
                                                  stderr=PIPE,
                                                  cwd=untar_dir_path)
                f.write('{0}\n'.format(tar_path_i))
                files_extracted = True
                break
        if not files_extracted:
            untar_dir_path = os.path.join(untar_dir, '{0}'.format(max_dir+1))
            os.makedirs(untar_dir_path, exist_ok=True)
            print('Untar file {0} to {1}'.format(tar_path_i, untar_dir_path))
            get_file_list_cmd = 'tar -zxvf {0}'.format(tar_path_i)
            sample_count_out = subprocess_run([get_file_list_cmd],
                                              shell=True,
                                              stdout=PIPE,
                                              stderr=PIPE,
                                              cwd=untar_dir_path)
            f.write('{0}\n'.format(tar_path_i))
