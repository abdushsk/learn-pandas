import pandas as pd

import time
import subprocess
import traceback
from datetime import datetime


def getBlacklist(blacklist, iSheet = None):
    link = "https://storage.googleapis.com/ohm-assets/ftp/%s/fo_secban_%s.csv"
    # dt equal to 09-05-2025
    dt = datetime(2025, 5, 9)    
    
    link = link % (dt.strftime("%d%m%Y"), dt.strftime("%d%m%Y"))
    while True:
        try:
            print("Getting Balacklist file form: " + link, flush=True)
            subprocess.check_call(["wget", link, "-O", "fo_secban.csv"])
            break
        except Exception:
            time.sleep(5)
    blacklistData = pd.read_csv("fo_secban.csv")
    print(blacklistData, flush=True)
    
    df = None
    if iSheet is not None:
        df = pd.read_excel(iSheet, sheet_name="HOUR")
        df = df.set_index("TradingSymbol")

    insts = []
    for i in list(blacklistData[blacklistData.columns[0]]):
        if (df is None) or (i not in df.index):
            print("BLACKLISTED INSTRUMENT NOT FOUND IN SHEET", flush=True)
            insts.append(i)
            continue
        insts.append(df.loc[i, "Name"])
    instsStr = ",".join(insts)

    if len(insts) == 0:
        print("No instruments found", flush=True)
        return

    dateStr = str(datetime.strptime(blacklistData.columns[0].split(' ')[-1][:-1], '%d-%b-%Y').date())

    print("Appending", dateStr, instsStr)

    blacklistDf = pd.read_csv(blacklist)._append({"Date": dateStr, "instruments": instsStr}, ignore_index=True)
    print(blacklistDf)

    blacklistDf.to_csv(blacklist, index=False)


getBlacklist("blacklist.csv")