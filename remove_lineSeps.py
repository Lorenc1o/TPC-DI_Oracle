sf = '3'
batch_number = 1

batch_dir = "../staging/"+sf+"/Batch"+str(batch_number)+"/"

files = [
    "Time.txt", "Date.txt", "Industry.txt", "StatusType.txt", 
    "TaxRate.txt", "TradeType.txt", "CashTransaction.txt",
    "WatchHistory.txt", ]
for f in files:
    filepath = batch_dir+f
    with open(filepath, "r") as fh:
        f_lines = fh.readlines()
    
    f_lines = [l[:-1]+'\n' for l in f_lines]

    # filepath = batch_dir+f[:-4]+'1'+f[-4:]
    
    with open(filepath, "w") as fh:
        fh.writelines(f_lines)