1-way sync of folders

python3 /home/user/sync_share/sync_share.py

#!/usr/bin/env python3

py py script work in windows and whonix
at start:

SOURCE_FOLDERS = [
    r"/home/user/fluid",
    r"/home/user/galaxy-flythrough",
]

DESTINATION_FOLDER = r"/mnt/shared/1"

EXCLUDE_FOLDERS = {
    ".git",
}


hash based per file look in SOURCE_FOLDERS and copy sync to DESTINATION_FOLDER if files different.
useful for test purposes.
should not track destination files : for example do not delete logs or changes unless source actually changed.
use case:
user run py	it make full sync (allow delete in destination)
	
user change source - it update dest
user run something in dest that produce logs and change files - py does not care about this until source changed

update 2 sec after last change in folder debounce
output console
timestamp, took time
results.. (updated N files)
skip same empty entries to not noise console. return to just rewrite timestamp