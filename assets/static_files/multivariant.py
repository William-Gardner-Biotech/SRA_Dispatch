#!/bin/env python3

import gzip
import os

samps = os.getcwd().split("/")[-1]

multi_var = ["notN440", "Y449H", "Y449R", "Y453F", "L455W", "F456L", "notS477",
             "notT478", "T478R", "V483A", "E484E", "E484P", "E484Q", "E484K", "E484V",
             "F486P", "F486A", "F486S", "F490H", "F490Y", "Q493K", "S494P", "Q498K", "Q498H",
             "Q498Y", "notQ498", "S499S", "P499H", "N501S", "N501T", "notN501", "notY505"]


# multi_var = ["L452R", "T478K","notS477N"]
extras = ["443DEL", "444DEL", "445DEL", "446DEL", "447DEL", "448DEL", "449DEL", "482DEL", "483DEL", "484DEL"]

spike_offset = 21562
start = (((440 -1) * 3) +1) + spike_offset
end = (((505 -1) * 3) +1) + spike_offset
SRR_list = []
# with open("SraRunTale.csv", "r") as meta_fh:
# for line in meta_fh:
# if not line.startswith("Run"):
# splitline = line.split(",")
# date = ""
# try:
# date = splitline[21].split("/")[0]
# except:
# print(line)
# if date and "2022" in date:
# SRR_list.append(splitline[0])
# print(len(SRR_list))

with open(samps+"_multi_var.tsv","w") as multivar_fhs:
    multivar_fhs.write(" ".join(multi_var))
    multivar_fhs.write(" + ")
    multivar_fhs.write(" ".join(extras))
    multivar_fhs.write("\n")

    files_read = []

    for subdir, dirs, files in os.walk(os.getcwd()):
        for file in files:
            if file not in files_read and "_unique_seqs.tsv" in file:
                files_read.append(file)
                if (file.endswith("_unique_seqs.tsv")):
                    with open(os.path.join(subdir, file)) as in_file:
                        mv_matches = []
                        for line in in_file:
                            if not (line.startswith("\t") or line.startswith("Unique") or line.startswith("SRR") or line.startswith("/mnt/") or "Ref" in line):
                                splitline = line.split("\t")
                                count = 0
                                try:
                                    count = int(splitline[1])
                                except:
                                    pass
                                if count > 2:
                                    pms = splitline[0]
                                    if len(pms.split(" ")) > 4:
                                        pm_start = 0
                                        pm_end = 0
                                        try:
                                            pm_start = int(pms.split(" ")[0])
                                            pm_end = int(pms.split(" ")[-1])
                                        except:
                                            print(line)
                                            continue
                                        if pm_start < end and pm_end > start:
                                            match = 0
                                            for PM in multi_var:
                                                if "not" in PM:
                                                    if PM.strip("not") not in pms:
                                                        POS = (((int(PM[4:]) - 1) * 3) + 1) + spike_offset
                                                        if pm_start < POS and pm_end > POS:
                                                            match += 1
                                                elif PM in pms:
                                                    match += 1
                                            for extra in extras:
                                                if extra in pms.upper():
                                                    match += 1
                                                    break
                                            if (match > 3) and len(pms.split(" ")) < 25 and pms.count("insert") < 3 and pms.count("Del") < 4:
                                                try:
                                                    mv_matches.append(line)
                                                except:
                                                    mv_matches = [line]
                        if mv_matches:
                            multivar_fhs.write(subdir+"/"+file[:-4]+"\t")
                            multivar_fhs.write("\n")
                            for line in mv_matches:
                                multivar_fhs.write(line)
                if (file.endswith("_unique_seqs.tsv.gz")):
                    with gzip.open(os.path.join(subdir, file), "rb") as in_file:
                        mv_matches = []
                        for line in in_file:
                            line = line.decode()
                            if not (line.startswith("\t") or line.startswith("Unique") or line.startswith("SRR") or line.startswith("/mnt/") or "Ref" in line):
                                splitline = line.split("\t")
                                count = 0
                                try:
                                    count = int(splitline[1])
                                except:
                                    pass
                                if count > 2:
                                    pms = splitline[0]
                                    if len(pms.split(" ")) > 4:
                                        pm_start = 0
                                        pm_end = 0
                                        try:
                                            pm_start = int(pms.split(" ")[0])
                                            pm_end = int(pms.split(" ")[-1])
                                        except:
                                            print(line)
                                            continue
                                        if pm_start < end and pm_end > start:
                                            match = 0
                                            for PM in multi_var:
                                                if "not" in PM:
                                                    if PM.strip("not") not in pms:
                                                        POS = (((int(PM[4:]) -1) * 3) +1) + spike_offset
                                                        if pm_start < POS and pm_end > POS:
                                                            match += 1
                                                elif PM in pms:
                                                    match += 1
                                            for extra in extras:
                                                if extra in pms.upper():
                                                    match += 1
                                                    break
                                            if (match > 3) and len(pms.split(" ")) < 25 and pms.count("insert") < 3 and pms.count("Del") < 4:
                                                try:
                                                    mv_matches.append(line)
                                                except:
                                                    mv_matches = [line]
                        if mv_matches:
                            multivar_fhs.write(subdir+"/"+file[:-4]+"\t")
                            multivar_fhs.write("\n")
                            for line in mv_matches:
                                multivar_fhs.write(line)
