import os
import pandas as pd
from pathlib import Path
from argparse import ArgumentParser, ArgumentTypeError
import subprocess
import glob
import shutil


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
    parser.add_argument('--aggregated_dir',
                        type=str,
                        help='aggregated_dir',
                        required=True)
    parser.add_argument('--gdrive_path',
                        type=str,
                        help='gdrive_path from ~Library',
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


args = get_process_arrays_args()
submit_dir = args.submit_dir
gdrive_path = args.gdrive_path
submit_name = args.submit_name
aggregated_dir = args.aggregated_dir
agg_submit_dir = os.path.join(aggregated_dir, submit_name)
date = submit_name[4:]
extract_results_dir = os.path.join(submit_dir, 'out_files/extract_results')
meta_path = os.path.join(submit_dir, 'in_files/stage_sra/SRA_meta_run.tsv')
os.makedirs(agg_submit_dir, exist_ok=True)
os.makedirs(aggregated_dir, exist_ok=True)
df_files = pd.DataFrame()

os.makedirs(os.path.join(extract_results_dir, 'wgntcalls'), exist_ok=True)
os.makedirs(os.path.join(extract_results_dir, 'unique_seqs'), exist_ok=True)
os.makedirs(os.path.join(extract_results_dir, 'cram'), exist_ok=True)
print(f'----{extract_results_dir}-----')
dirlist = os.listdir(extract_results_dir)
dirlist = [x for x in dirlist if os.path.isdir(os.path.join(extract_results_dir, x)) and (len(x) < 4)]
for dir_i in dirlist:
    print(dir_i)
    parent_dir = os.path.join(extract_results_dir, dir_i)
    filelist = os.listdir(parent_dir)
    print(dir_i, len(filelist), parent_dir)

    os.makedirs(os.path.join(extract_results_dir, 'wgntcalls', dir_i), exist_ok=True)
    os.makedirs(os.path.join(extract_results_dir, 'unique_seqs', dir_i), exist_ok=True)
    os.makedirs(os.path.join(extract_results_dir, 'cram', dir_i), exist_ok=True)

    dest_dir = os.path.join(os.path.join(extract_results_dir, 'cram', dir_i))
    for file in glob.glob(r'{}/*.wg.cram'.format(parent_dir)):
        shutil.copy(file, dest_dir)

    dest_dir = os.path.join(os.path.join(extract_results_dir, 'wgntcalls', dir_i))
    for file in glob.glob(r'{}/*.wg_nt_calls.tsv.gz'.format(parent_dir)):
        shutil.copy(file, dest_dir)

    dest_dir = os.path.join(os.path.join(extract_results_dir, 'unique_seqs', dir_i))
    for file in glob.glob(r'{}/*.wg_unique_seqs.tsv.gz'.format(parent_dir)):
        shutil.copy(file, dest_dir)

unique_seq_dir = os.path.join(extract_results_dir, 'unique_seqs')

i = 0
df_files = pd.DataFrame()
filepath_list_all = []

print(f'----{unique_seq_dir}-----')
dirlist = os.listdir(unique_seq_dir)
dirlist = [x for x in dirlist if os.path.isdir(os.path.join(unique_seq_dir, x)) and (len(x) < 4)]
for dir_i in dirlist:
    i += 1
    if i % 50 == 0:
        print(i, dir_i)
    parent_dir = os.path.join(unique_seq_dir, dir_i)
    file_list = os.listdir(parent_dir)
    file_list = [x for x in file_list if not x.startswith('._') and x.endswith('.SARS2.wg_unique_seqs.tsv.gz')]
    filepath_list = [os.path.join(parent_dir, x) for x in file_list]
    filepath_list_all += filepath_list

len(filepath_list_all)
file_path_list = filepath_list_all


def filter_delta(hap, deltahap=None):
    if deltahap is None:
        deltahap = ["L452R", "notS477N", "T478K"]
    match = 0
    for PM in deltahap:
        if 'not' in PM:
            if PM.strip('not') in hap:
                match -= 1
        elif PM in hap:
            match += 1
    if (match == 2) and (len(hap.split(" ")) < 25) and (hap.count("insert") < 3) and (hap.count("Del") < 4):
        return True
    else:
        return False


# file_path_list = list(df_meta_merge['FILEPATH'].unique())
file_count = len(file_path_list)
i = 0
df_variant = pd.DataFrame()
for filepath_i in file_path_list:
    i += 1
    if i % 100 == 0:
        print(f'{i} of {file_count}')
    if i % 2000 == 0:
        df_variant.to_csv(os.path.join(agg_submit_dir, f'delta_through_{date}_{i}.csv'), index=False)
    try:
        df_variant_i = pd.read_csv(filepath_i, sep='\t', skiprows=1)
    except:
        continue
    if len(df_variant_i) < 1:
        continue
    # figure out a run name that can later be merged
    print(i)
    file = os.path.basename(filepath_i)
    run = file[:-28]
    # add a run column
    df_variant_i['Run'] = run
    # filter for at least 3 identical reads of the sequence
    df_variant_i = df_variant_i[df_variant_i['Count'] > 2]
    if len(df_variant_i) < 1:
        continue

    df_variant_i['FILTER'] = [filter_delta(hap) for hap in df_variant_i['Unique Sequence']]
    df_variant_i = df_variant_i[df_variant_i['FILTER']]
    if len(df_variant_i) < 1:
        continue
    df_variant_i.drop(columns=['FILTER'], inplace=True)
    df_variant = pd.concat([df_variant, df_variant_i], ignore_index=True)

df_meta = pd.read_csv(meta_path, sep='\t', header=None,
                      names=['Run', 'PROJECT', 'BIONUBMER', 'INSTITUTION', 'DATE', 'LOCATION', 'FILESIZE'])

if len(df_variant) > 0:
    df_variant_m = df_variant.merge(df_meta, on=['Run'])
    df_variant_m.sort_values(by=['DATE', 'Run'], ascending=False, inplace=True)
    df_variant_m.to_csv(os.path.join(agg_submit_dir, f'delta_{date}_L452R_notS477N_T478K.csv'), index=False)
    df_variant_m.to_csv(os.path.join(agg_submit_dir, f'delta_{date}_L452R_notS477N_T478K.csv.gz'), index=False)

else:
    Path(os.path.join(agg_submit_dir, f'delta_through_{date}_L452R_notS477N_T478K.csv')).touch()

content_file_list = []
dir_list = os.listdir(extract_results_dir)
dir_list = [os.path.join(extract_results_dir, x) for x in dir_list if
            os.path.isdir(os.path.join(extract_results_dir, x)) and (len(x) < 5)]
i = 0
for dir_i in dir_list:
    i = i + 1
    print(dir_i, i, len(dir_list))
    file_list = os.listdir(dir_i)
    file_list = [os.path.join(dir_i, x) for x in file_list if
                 x.endswith('AA_E484del.tsv') and not x.startswith('._')]
    for file_i in file_list:
        file_size_i = os.path.getsize(file_i)
        if file_size_i > 20:
            content_file_list.append(file_i)

df_e484 = pd.DataFrame()
for content_i in content_file_list:
    basename_i = os.path.basename(content_i)
    basename_i = basename_i[:-15]
    df_e484_i = pd.read_csv(content_i, sep='\t', skiprows=1, header=None, names=['variant', 'count', 'fraction'])
    df_e484_i['accession'] = basename_i
    df_e484 = pd.concat([df_e484, df_e484_i], ignore_index=True)

df_meta = pd.read_csv(meta_path, sep='\t', header=None, names=['accession',
                                                               'project',
                                                               'bioproj',
                                                               'instituion',
                                                               'date_collected',
                                                               'location',
                                                               'SRA_FILE_SIZE'])
print(df_meta)
print(df_e484)
if len(df_e484) > 0:
    df_e484_m = df_meta.merge(df_e484, on=['accession'], how='inner')
    df_e484_m.to_csv(os.path.join(agg_submit_dir, f'{date}_SRA_e484_del_all.csv'), index=False)

type_dir_list = ['cram',
                 'wgntcalls',
                 'unique_seqs']

for type_dir in type_dir_list:
    unique_seq_dir = os.path.join(extract_results_dir, type_dir)
    out_dir = os.path.join(agg_submit_dir, f'{type_dir}_{date}')
    i = 0
    os.makedirs(out_dir, exist_ok=True)
    print(f'----{unique_seq_dir}-----')
    dirlist = os.listdir(unique_seq_dir)
    dirlist = [x for x in dirlist if os.path.isdir(os.path.join(unique_seq_dir, x)) and (len(x) < 4)]
    for dir_i in dirlist:
        subprocess.call('tar -cf {0}/{1}.tar {1}'.format(out_dir,
                                                         dir_i),
                        shell=True,
                        cwd=unique_seq_dir)
shutil.copy(meta_path, agg_submit_dir)
if gdrive_path is not None:
    print(f'copying: {agg_submit_dir} to google drive')
    shutil.copytree(agg_submit_dir, os.path.join(gdrive_path, submit_name))
