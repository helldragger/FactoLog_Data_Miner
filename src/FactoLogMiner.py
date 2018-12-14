from logParser import parseLogFile
from statistics import genStatsPlot


if __name__ == "__main__":
    #test
    import sys
    import os
    args = sys.argv[1::]
    if (len(args) != 2):
        raise Exception("Usage: FactoLogMiner.py <logFilePath> <gameName>")


    logfilepath = args[0]
    if (not os.path.isfile(logfilepath)):
        raise Exception(logfilepath+" does not exists.")

    gamename = args[1]
    print("Parsing " + logfilepath + " ...")
    logdata, item_name_list= parseLogFile(logfilepath)
    print("Generating basic statistics...")
    genStatsPlot(logdata, item_name_list)
    pass
