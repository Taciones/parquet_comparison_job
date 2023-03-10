import pyarrow.parquet as pq
import os
import argparse
from tqdm import tqdm


def compare_files(file1, file2):
    """
    Compares two parquet files and returns True if they are identical, else False.
    """
    table1 = pq.read_table(file1)
    table2 = pq.read_table(file2)
    return table1.equals(table2)


def compare_folders(folder1, folder2):
    """
    Recursively compares all the parquet files in two folders and returns a dictionary
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

    match_count = len([v for v in result.values() if v == "Match"])
    not_match_count = len([v for v in result.values() if v == "Not match"])
    not_found_count = len([v for v in result.values() if v == "File not found"])

    print(f"Summary:")
    print(f"Total files compared: {len(files)}")
    print(f"Matched files: {match_count}")
    print(f"Not matched files: {not_match_count}")
    print(f"Files not found: {not_found_count}")

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
