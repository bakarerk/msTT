import os
from glob import glob

files = []
start_dir = "C:/"
file_name = ""
pattern   = "*.sql"

for dir,_,_ in os.walk(start_dir):
    if len(glob(os.path.join(dir,pattern)))>0:
        for i in glob(os.path.join(dir,pattern)):
            if file_name in i.split("\\")[-1]:
                print(i)
