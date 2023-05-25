set = \
[["CELLALGOSWITCH","CqiAdjAlgoSwitch","DlCqiAdjDeltaOptSwitch-0","DlCqiAdjDeltaOptSwitch-1"],
["CELLALGOSWITCH","CqiAdjAlgoSwitch","DLENVARIBLERTARGETSWITCH-0","DLENVARIBLERTARGETSWITCH-1"],
["CELLDLSCHALGO","HighIblerTargetTbsIdxThld","16","5"],
["CELLDLSCHALGO","LowIblerTargetTbsIdxThld","255","21"]]

cellidlist = [21,22,23,24,25,41,42,43,44,45]

print("RUN COMMANDS")
for i in set:
    for cellid in cellidlist:
        print("MOD {}:LOCALCELLID={},{}={};".format(i[0],cellid,i[1], i[3]))

print("RB COMMANDS")
for i in set:
    for cellid in cellidlist:
        print("MOD {}:LOCALCELLID={},{}={};".format(i[0], cellid, i[1], i[2]))
