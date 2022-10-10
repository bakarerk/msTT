import os
from glob import glob

files = []
start_dir = "C:/"

pattern   = "*.sql"

for dir,_,_ in os.walk(start_dir):
    if len(glob(os.path.join(dir,pattern)))>0:
        for i in glob(os.path.join(dir,pattern)):
            print(i.split("\\")[-1])
