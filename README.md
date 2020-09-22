:warning: **The Nimbus infrastructure project is no longer under development.** :warning:

For more information, please read the [news announcement](http://www.nimbusproject.org/news/#440). If you are interested in providing IaaS capabilities to the scientific community, see [CHI-in-a-Box](https://github.com/chameleoncloud/chi-in-a-box), a packaging of the [Chameleon testbed](https://www.chameleoncloud.org), which has been in development since 2014.

---

# scheduler-simulator
Run HPC simulation with a variety of scheduler algorithms. 

## Requirements

### Input file format
The input file has to be a CSV file with a header in the first line. 
Five columns are reuired -- *jobname* (the name of the job), *ctime* (job creation time / enqueue time), *start* (job epoch start time), *end* (job epoch end time), *nhosts* (number of nodes used).

### Install required python libraries
```
pip install -r requirements.txt
```

## Run
```
python simulator.py --nnodes <total number of avaialble nodes> --input <input csv file> --start <simulation start time> --end <simulation end time> --algorithm <scheduler algorithm> --result-file <output csv file>
```
