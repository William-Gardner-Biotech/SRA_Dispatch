#!/bin/bash

echo "Launching job"

# Check if $1 exists
if [ -z "$1" ]; then
    echo "Error: No input file provided."
    echo "Usage: $0 <input_file>"
    exit 1
fi

# Check if the file exists
if [ ! -f "$1" ]; then
    echo "Error: Input file '$1' does not exist."
    echo "Please provide a valid input file."
    exit 1
fi

sratxt=$1
output_folder=$2

# Install Miniconda3
wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-py39_24.9.2-0-Linux-x86_64.sh -O miniconda.sh
chmod +x miniconda.sh
./miniconda.sh -b -p miniconda
rm miniconda.sh
. miniconda/etc/profile.d/conda.sh
export PATH=$PWD/miniconda/bin:$PATH
conda install -c conda-forge -c bioconda seqkit sra-tools bbmap minimap2=2.18 htslib samtools -y

# unzip the input files
tar -xzvf modules.tar.gz
tar -xzvf static_files.tar.gz

touch successful_sra_download.txt

for SRA_NUM in `cat ${sratxt}`; do

  echo "Fetching reads for SRA accession $SRA_NUM"
  python3 SRA_fetch.py --SRA=${SRA_NUM}
  echo Fetching done
  rm -rf ${SRA_NUM}

  python3 SAM_Refiner.py -r SARS2.gb \
  --wgs 1 --collect 0 --seq 1 --indel 0 --covar 0 --nt_call 1 --min_count 1 \
  --min_samp_abund 0 --ntabund 0 --ntcover 1 --mp 4 --chim_rm 0 --deconv 0  -S ${SRA_NUM}.SARS2.wg.sam

  echo SAM SORTING
  samtools sort -o ${SRA_NUM}.SARS2.wg.srt.sam ${SRA_NUM}.SARS2.wg.sam
  samtools view -T SARS2.fasta -@4 ${SRA_NUM}.SARS2.wg.srt.sam -o ${SRA_NUM}.SARS2.wg.cram
  python3 Variant_extractor.py ${SRA_NUM}

  # mv dir_29618_AA_E484del.tsv ${SRA_NUM}_dir_29618_AA_E484del.tsv

  rm -f ${SRA_NUM}.SARS2.wg.sam
  rm -f ${SRA_NUM}.SARS2.wg.srt.sam
  gzip ${SRA_NUM}.SARS2.wg_unique_seqs.tsv
  rm -f ${SRA_NUM}.SARS2.wg_unique_seqs.tsv
  gzip ${SRA_NUM}.SARS2.wg_nt_calls.tsv
  rm -f ${SRA_NUM}.SARS2.wg_nt_calls.tsv

  echo Moving all processed files to ${output_folder}
  mv ${SRA_NUM}*.cram ${output_folder}
  mv ${SRA_NUM}*.wg_nt_calls.tsv.gz ${output_folder}
  mv ${SRA_NUM}*.wg_unique_seqs.tsv.gz ${output_folder}
  mv ${SRA_NUM}*.readlen.txt ${output_folder}


  echo $SRA_NUM >> successful_sra_download.txt
done
