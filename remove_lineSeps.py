import os
sf = '3'
batch_number = 1

batch_dir = "../staging/"+sf+"/Batch"+str(batch_number)+"/"

def remove_lineSeps0(batch_dir, f):
    filepath = batch_dir+f
    with open(filepath, "r") as fh:
        f_lines = fh.readlines()
    
    f_lines = [l[:-1]+'\n' for l in f_lines]

    # filepath = batch_dir+f[:-4]+'1'+f[-4:]
    
    with open(filepath, "w") as fh:
        fh.writelines(f_lines)

def remove_lineSeps1(batch_dir, f):
    filepath = batch_dir+f
    with open(filepath, "r") as fh:
        f_lines = fh.readlines()
    
    f_lines = [l.rstrip()+'\n' for l in f_lines]

    # filepath = batch_dir+f[:-4]+'1'+f[-4:]
    
    with open(filepath, "w") as fh:
        fh.writelines(f_lines)

files = [
    "Time.txt", "Date.txt", "Industry.txt", "StatusType.txt", 
    "TaxRate.txt", "TradeType.txt", "CashTransaction.txt",
    "WatchHistory.txt"]

for f in files:
    remove_lineSeps0(batch_dir, f)

for f in os.listdir(batch_dir):
    if("FINWIRE" in f and "audit" not in f):
        print(f)
        remove_lineSeps1(batch_dir, f)