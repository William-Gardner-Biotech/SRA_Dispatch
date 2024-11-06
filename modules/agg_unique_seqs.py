import pandas as pd
import os
import gzip
import warnings
from datetime import datetime
from argparse import ArgumentParser, ArgumentTypeError
# import subprocess
# import glob
# import shutil


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
    parser.add_argument('--submit_dir',
                        type=str,
                        help='submit_dir',
                        required=True)
    parser.add_argument('--submit_name',
                        type=str,
                        help='submit_name',
                        required=True)
    parser.add_argument('--analysis_dir',
                        type=str,
                        help='analysis_dir',
                        required=True)
    parser.add_argument('--gdrive_path',
                        type=str,
                        help='gdrive_path from ~Library',
                        required=False,
                        default=None)
    parser.add_argument('--agg_dir',
                        type=str,
                        help='agg_dir for 1_5 perc threshold',
                        required=False,
                        default=None)


def get_process_arrays_args():
    """	Inputs arguments from bash
    Gets the arguments, checks requirements, returns a dictionary of arguments
    Return: args - Arguments as a dictionary
    """
    parser = ArgumentParser()
    parse_process_arrays_args(parser)
    args = parser.parse_args()
    return args


# Suppress FutureWarning messages
# warnings.simplefilter(action='ignore', category=FutureWarning)
# warnings.filterwarnings('ignore')
args = get_process_arrays_args()
submit_dir = args.submit_dir
gdrive_path = args.gdrive_path
submit_name = args.submit_name
analysis_dir = args.analysis_dir
agg_dir = args.agg_dir
# agg_dir = '/Volumes/T9/SRA_1_5perc_all_2024_04_10.csv.gz'
# analysis_dir = '/Volumes/T6/serpent/sra_cryptic_analysis

warnings.filterwarnings(action='once')
# submit_dir = '/Volumes/T6/serpent/sra_cryptic_lineages'
# analysis_dir = '/Volumes/T6/serpent/sra_cryptic_analysis'
extract_results_dir = os.path.join(submit_dir, '/out_files/extract_results')

root_dir = os.path.join(submit_dir, 'out_files/extract_results/unique_seqs/')

dirlist = os.listdir(root_dir)
dirlist = [x for x in dirlist if os.path.isdir(os.path.join(root_dir, x))]
meta_fp = os.path.join(submit_dir, 'in_files/stage_sra/SRA_meta.tsv')
# meta_fp = '/Volumes/T6/SRA_meta.tsv'
df_meta = pd.read_csv(meta_fp,
                      sep='\t',
                      header=None,
                      names=['RUN', 'PROJECT', 'BIONUBMER', 'INSTITUTION', 'DATE', 'LOCATION', 'FILESIZE'])

for dir_i in dirlist:
    print('Direcotry: ', dir_i)
    input_dir = os.path.join(root_dir, dir_i)
    fn_list = os.listdir(input_dir)
    filepath_list = [os.path.join(input_dir, x) for x in fn_list if
                     x.endswith('.tsv.gz') and (not x.startswith('._'))]
    df_all = pd.DataFrame()
    print('File count:', len(filepath_list))
    i = 0
    for fp in filepath_list:
        if (i % 20) == 0:
            print(i)
        i += 1

        with gzip.open(fp, 'r') as f:
            first_line = f.readline().strip().decode('utf-8')

        if len(first_line) < 1:
            continue
        sample_name_split = first_line.split('.')
        if len(sample_name_split) < 2:
            continue
        sample_name = sample_name_split[0]
        try:
            df_i = pd.read_csv(fp, sep='\t', skiprows=1)
        except:
            print('failed:', fp)
            continue
        if len(df_i) < 1:
            continue
        df_i['RUN'] = sample_name
        df_i['start'] = [int(x.split(' ')[0]) for x in df_i['Unique Sequence']]
        df_i['end'] = [int(x.split(' ')[-1]) for x in df_i['Unique Sequence']]
        df_i['MUT'] = [' '.join(x.split(' ')[1:-1]) for x in df_i['Unique Sequence']]
        df_i['MUT_PRES'] = [x != 'Reference' for x in df_i['MUT']]
        df_i.sort_values(by=['start'], inplace=True)
        df_s = df_i[(df_i['Count'] > 2) & df_i['MUT_PRES']]
        df_sagg = df_s.groupby(['RUN', 'MUT']).agg(
            {'Abundance': 'sum', 'Count': 'sum', 'start': 'min', 'end': 'max'}).reset_index()
        df_all = pd.concat([df_sagg, df_all], ignore_index=True)
    df_all = df_all.merge(df_meta, on='RUN', how='inner')
    print('saving to csv')
    df_all.to_csv(f'{analysis_dir}/{submit_name}_{dir_i}.csv.gz', index=False)
    df_all[df_all['MUT'].str.contains('sur')].to_csv(
        os.path.join(analysis_dir, f'{submit_name}_{dir_i}_surface.csv.gz'), index=False)

fn_list = os.listdir(analysis_dir)
fp_list = [os.path.join(analysis_dir, x) for x in fn_list if x.endswith('.csv.gz') and x.startswith(submit_name)]
i = 1
print(len(fp_list))
for fp in fp_list:
    df_i = pd.read_csv(fp)
    df_i = df_i[df_i['Abundance'] > .0145]
    df_i.to_csv(os.path.join(analysis_dir, '1_5perc', os.path.basename(fp)), index=False)
    if i % 25 == 0:
        print(i)
    i += 1

dir_15 = os.path.join(analysis_dir, '1_5perc')
fn_list = os.listdir(dir_15)
fn_list = [x for x in fn_list if not x.endswith('_surface.csv.gz') and x.endswith('.csv.gz')]
fp_list = [os.path.join(dir_15, x) for x in fn_list if x.startswith(submit_name)]
print(len(fp_list))
agg_list = os.listdir(agg_dir)
agg_list = [x for x in agg_list if x.startswith('SRA_1_5perc_all_') and x.endswith('.csv.gz')]
agg_list.sort(reverse=True)
if len(agg_list) > 0:
    print(agg_list[0])
    df_all = pd.read_csv(os.path.join(agg_dir, agg_list[0]))
else:
    print('no previous files found')
    df_all = pd.DataFrame()

num_i = 0
for fp_i in fp_list:
    if (num_i % 25) == 0:
        print(num_i)
    num_i += 1
    df_i = pd.read_csv(fp_i)
    df_all = pd.concat([df_all, df_i], ignore_index=True)

dt_now = datetime.now()
current_date = dt_now.strftime('%Y_%m_%d')
current_agg = f'SRA_1_5perc_all_{current_date}.csv.gz'
df_all.to_csv(os.path.join(agg_dir, current_agg), index=False)
