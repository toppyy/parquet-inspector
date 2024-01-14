from src.ParquetReader import ParquetReader
import sys

args = sys.argv

n_records = 0

if len(args) == 1:
    try:
        raise
        # This weirdness is to not print the traceback
    except:
        print("Not enough arguments. Need at least a filename.")
        exit(1)
    
elif len(args) == 3:
    try:
        n_records = int(args[2])
    except Exception as e:
        print(f"Error parsing the number of records. {args[2]} cannot be interpreted as a number.")


path = args[1]
reader = ParquetReader(path)

# Info is always printed
reader.print_info()

# Print requested number of records
if n_records > 0:
    print("Records:")
    reader.print_records(n_records)

