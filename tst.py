
from pathlib import Path
import os

my_file = Path("ooof_files")
if not my_file.is_dir():
	print("directory_exists")
else:
	os.mkdir("ooo_files")

