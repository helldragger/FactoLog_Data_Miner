from logParser import parseLogFile


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

    args = args[2::]

    print("Parsing " + logfilepath + " ...")
    parseLogFile(logfilepath, gamename)

    print("Generating basic statistics...")

    pass
