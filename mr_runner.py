#!/usr/bin/env python3

"""
runner.py: Sequentially execute MRDocFreq and MRChiSquare MRJobs via MRJob on Hadoop or local.
Assumes:
  - Input data files in ./data
  - MRJob scripts in ./src/mr_doc_freq.py and ./src/mr_chi_square.py
  - Outputs written to ./output/df_counts.txt and ./output/output.txt
  - MRChiSquare takes a dummy input file and uses df_counts.txt for --dfcounts
"""
import argparse
import os
import sys
import glob
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('-r','--runner',
                    default=os.getenv('MRJOB_RUNNER','hadoop'))
parser.add_argument('--hadoop-streaming-jar',
                    dest='streaming_jar',
                    help='path to hadoop-streaming jar')
parser.add_argument('--stopwords',
                    default='stopwords.txt',
                    help='path to the stopwords file')
args, _ = parser.parse_known_args()

def main():

    #base paths
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, 'data')
    src_dir = os.path.join(base_dir, 'src')
    output_dir = os.path.join(base_dir, 'output')

    runner = args.runner 
    streaming_jar = args.streaming_jar

    #ensuring output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # collects all input files for MRDocFreq (even though there is only one)
    data_files = glob.glob(os.path.join(data_dir, '*'))
    if not data_files:
        print(f"No input files found in {data_dir}")
        sys.exit(1)

 

    # 1. Document Frequency MRJob
    df_output = os.path.join(output_dir, 'df_counts.txt')
    cmd1 = [
        sys.executable,
        os.path.join(src_dir, 'mr_doc_freq.py'),
        '-r', runner]
    if streaming_jar:
        cmd1 += ['--hadoop-streaming-jar', streaming_jar]
    cmd1 += ['--stopwords', args.stopwords]    
    cmd1 += data_files
    


    print(f"Running MRDocFreq: {' '.join(cmd1)}")
    try:
        with open(df_output, 'w', encoding='utf-8') as f_out:
            subprocess.check_call(cmd1, stdout=f_out)
    except subprocess.CalledProcessError as e:
        print(f"Error running MRDocFreq (exit code {e.returncode}): {e}")
        sys.exit(e.returncode)

    # a dummy input file for MRChiSquare
    dummy_input = os.path.join(output_dir, 'dummy_input.txt')
    with open(dummy_input, 'w', encoding='utf-8') as f_dummy:
        f_dummy.write('dummy')

    # 2. Chi-Square MRJob (uses dummy input + df_counts)
    chi_output = os.path.join(output_dir, 'output.txt')
    cmd2 = [
        sys.executable,
        os.path.join(src_dir, 'mr_chi_square.py'),
        '-r', runner]
    if streaming_jar:
        cmd2 += ['--hadoop-streaming-jar', streaming_jar]
    cmd2 += ['--dfcounts', df_output, dummy_input]
        
    print(f"Running MRChiSquare: {' '.join(cmd2)}")
    try:
        with open(chi_output, 'w', encoding='utf-8') as f_out:
            subprocess.check_call(cmd2, stdout=f_out)
    except subprocess.CalledProcessError as e:
        print(f"Error running MRChiSquare (exit code {e.returncode}): {e}")
        sys.exit(e.returncode)

    print("All jobs completed successfully.")
    print(f"Document frequencies at: {df_output}")
    print(f"Chi-square results at: {chi_output}")


if __name__ == '__main__':
    main()


