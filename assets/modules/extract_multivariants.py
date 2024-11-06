from argparse import ArgumentParser, ArgumentTypeError
import os
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
    parser.add_argument('--untar_dir',
                        type=str,
                        help='where the extracted files will be stored',
                        required=True)
    parser.add_argument('--multivariant_py_path',
                        type=str,
                        help='where multivariant.py absolute filepath',
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


def isint(x):
    try:
        int(x)
        return True
    except ValueError:
        return False


args = get_process_arrays_args()
untar_dir = args.untar_dir
multivariant_py_path = args.multivariant_py_path
untar_list = os.listdir(untar_dir)
# untar_list = [x for x in untar_list if x in ['1']]
untar_list = [os.path.join(untar_dir, x) for x in untar_list if isint(x)]

extract_variants_cmd = 'python3 {0}'.format(multivariant_py_path)

for untar_dir_path in untar_list:
    print(untar_dir_path)
    sample_count_out = subprocess_run([extract_variants_cmd],
                                      shell=True,
                                      stdout=PIPE,
                                      stderr=PIPE,
                                      cwd=untar_dir_path)

