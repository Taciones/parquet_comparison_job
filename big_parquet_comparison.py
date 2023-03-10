import pyarrow.parquet as pq
import os
import argparse
from tqdm import tqdm
import random

def compare_files(file1, file2):
    """
    Compares randomly selected 50% of rows from two parquet files and returns True if they are identical, else False.
    """
    try:
        table1 = pq.read_table(file1)
        table2 = pq.read_table(file2)
        total_rows = min(len(table1), len(table2))
        rows_to_compare = int(total_rows/2)  # select 50% of rows to compare randomly
        indices = random.sample(range(total_rows), rows_to_compare)
        table1_sampled = table1.take(indices)
        table2_sampled = table2.take(indices)
        return table1_sampled.equals(table2_sampled)
    except OSError as e:
        print(f"Error reading {file1}, or {file2}: {e}")
        return False


def compare_folders(folder1, folder2):
    """
    Recursively compares randomly selected 50% of rows from all the parquet files in two folders and returns a dictionary
    with the result of each file comparison.
    """
    result = {}
    files = []
    for root, dirs, files_list in os.walk(folder1):
        for file in files_list:
            if file.endswith(".parquet"):
                file1 = os.path.join(root, file)
                file2 = os.path.join(root.replace(folder1, folder2), file)
                files.append((file1, file2))

    with tqdm(total=len(files)) as pbar:
        for file1, file2 in files:
            if os.path.exists(file2):
                if compare_files(file1, file2):
                    result[file1] = "Match"
                else:
                    result[file1] = "Not match"
            else:
                result[file1] = "File not found"
            pbar.update(1)

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare two folders containing parquet files.")
    parser.add_argument("folder1", type=str, help="path to first folder")
    parser.add_argument("folder2", type=str, help="path to second folder")
    args = parser.parse_args()

    folder1 = args.folder1
    folder2 = args.folder2

    result = compare_folders(folder1, folder2)

    for file, status in result.items():
        print(f"{file}: {status}")
