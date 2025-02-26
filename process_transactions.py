import pandas as pd
import ast
from multiprocessing import Pool, cpu_count
import time
import os
import argparse
from tqdm import tqdm
import zipfile

datasets_dir = "datasets"
dataset_file = 'Retail_Transactions_Dataset.csv'

def process_transaction(row):
    num_items = row['Total_Items']
    total_cost = row['Total_Cost']
    avg_cost_per_item = total_cost / num_items if num_items > 0 else 0
    discount_factor = 0.1 if row['Discount_Applied'] else 0
    discounted_amount = total_cost * (1 - discount_factor)
    num_products = len(row['Product'])
    return {
        'Transaction_ID': row['Transaction_ID'],
        'avg_cost_per_item': avg_cost_per_item,
        'discounted_amount': discounted_amount,
        'num_products': num_products,
        'customer_name': row['Customer_Name'],
        'store_type': row['Store_Type']
    }

def serial_processing(df):
    start_time = time.time()
    results = []
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Serial Processing"):
        results.append(process_transaction(row))
    end_time = time.time()
    return pd.DataFrame(results), end_time - start_time


def process_chunk(chunk):
    return [process_transaction(row) for _, row in chunk.iterrows()]

def parallel_processing(df, chunksize=25000):
    print(f"Chunksize: {chunksize}")
    n_cores = cpu_count()  # 8 on M1
    df_chunks = [df[i:i + chunksize] for i in range(0, len(df), chunksize)]
    start_time = time.time()
    with Pool(n_cores) as pool:
        results = list(tqdm(pool.imap(process_chunk, df_chunks), total=len(df_chunks), desc="Parallel"))
    flat_results = [item for sublist in results for item in sublist]
    return pd.DataFrame(flat_results), time.time() - start_time

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Process retail transactions serially or in parallel.")
    parser.add_argument('--chunksize', type=int, default=25000, help="Chunksize for parallel processing (default: 25000)")
    parser.add_argument('--mode', choices=['serial', 'parallel'], default='parallel',
                        help="Run mode: 'serial' or 'parallel' (default: parallel)")

    csv_path = os.path.join(datasets_dir, dataset_file)

    if not os.path.exists(csv_path):
        print(f"Unzipping {csv_path}.zip")
        with zipfile.ZipFile((csv_path + ".zip"), "r") as zipf:
            zipf.extractall(datasets_dir)
        
    print("Loading CSV file into dataframe...")
    loading_start_time = time.time()
    df = pd.read_csv(csv_path, parse_dates=['Date'], 
                     converters={'Product': lambda x: ast.literal_eval(x)})        
    print(f"Dataframe shape: {df.shape}")
    loading_time = time.time() - loading_start_time
    print(f"CSV to Dataframe Loading time: {loading_time:.2f}s")

    args = parser.parse_args()

    if args.mode == 'serial':
        print("Starting serial processing...")
        results, proc_time = serial_processing(df)
    else:  # parallel
        print("Starting parallel processing...")
        results, proc_time = parallel_processing(df, chunksize=args.chunksize)
        
        
    print(f"Processing time: {proc_time:.2f}s")
    print(f"Total time: {loading_time + proc_time:.2f}s")
    print(f"Resulting Dataframe shape: {results.shape}")