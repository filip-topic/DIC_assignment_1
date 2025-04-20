# MapReduce document frequency


## Individually running mr_doc_freq.py
To run mr_doc_freq.py individually, run:

"Get-Content data/reviews_devset.json | python src/mr_doc_freq.py | Out-File output/df_counts.txt -Encoding utf8" 

(reviews_devset.json needs to be in the ./data folder, mr_doc_freq.py needs to be in the ./src folder and ./output folder has to exist)


## Individually running mr_chi_square.py
To run the mr_chi_square.py, run:

"echo "dummy" | python ./src/mr_chi_square.py --dfcounts ./output/df_counts.txt > ./output/output.txt"

## Running the mr_runner.py
To activate the virtual environment, run:

.\venv\Scripts\Activate.ps1



To test locally, run:
$env:MRJOB_RUNNER = 'inline'
python .\mr_runner.py



To run on Hadoop, run:
$env:MRJOB_RUNNER = 'hadoop'
python .\mr_runner.py