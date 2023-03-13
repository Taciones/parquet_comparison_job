# Parquet File Comparator
This script compares two folders containing parquet files and outputs the result of each comparison. It can be useful for verifying that data has been migrated correctly or for comparing data sets from different sources.

### Installation
1. Clone the repository: git clone https://github.com/example/parquet-file-comparator.git
2. Navigate into the directory: cd parquet-file-comparator
3. Create a virtual environment: python3 -m venv env
4. Activate the virtual environment: source env/bin/activate
5. Install the requirements: pip install -r requirements.txt
### Usage
1. Activate the virtual environment: source env/bin/activate
2. Run the script with the command: python compare_parquet.py [path_to_folder1] [path_to_folder2]
For example, if you have two folders folder1 and folder2 that you want to compare, you would run the command:
```
python compare_parquet.py folder1 folder2
```

### Output
The script outputs a list of all parquet files found in folder1 with their comparison result:

bash
```
folder1/file1.parquet: Match
folder1/file2.parquet: Not match
folder1/file3.parquet: File not found
```
Match means that the file exists in both folder1 and folder2 and the contents are identical.
Not match means that the file exists in both folder1 and folder2 but the contents are different.
File not found means that the file exists in folder1 but not in folder2.