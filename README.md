# Retail transactions

How much difference does parallel computing make when processing a CSV file ?

The python script in this repo runs on a fictional retail sales CSV file located in the datasets folder. It uses pandas to store the data then does an arbitrary operation on the dataframe line by line ("calculates discount"). Given the data is in a dataframe vectorized calculation would be way quicker but would defeat the point of the exercise. How much quicker ? I might do the test another time.

As per the help flag the mode and chunksize can be set:

```
> python process_transactions.py --help
usage: process_transactions.py [-h] [--chunksize CHUNKSIZE] [--mode {serial,parallel}]

Process retail transactions serially or in parallel.

options:
  -h, --help            show this help message and exit
  --chunksize CHUNKSIZE
                        Chunksize for parallel processing (default: 25000)
  --mode {serial,parallel}
                        Run mode: 'serial' or 'parallel' (default: parallel)
```

On a 2020 Mac M1 parallel computing perfomance imroves with decreasing chunksize down to somewhere between 75,000 and 25,000.



