import os
from glob import glob

files = []
start_dir = "C:/"
file_name = "" #filters file name if text exists, empty means no filter
pattern   = "*.zip*" # filters by file extention, *.* means no filter
filterby_size_mb = 50 #bring files greater then this size, 0 means no filter

for dir,_,_ in os.walk(start_dir):
    if len(glob(os.path.join(dir,pattern)))>0:
        for i in glob(os.path.join(dir,pattern)):
            try:
                if os.stat(i).st_size/1024/1024 > filterby_size_mb and file_name in i.split("\\")[-1]:
                    print("{} ({:.2f} mb)".format(i,os.stat(i).st_size/1024/1024))
            except:
                print("exception:: {}".format(i))

