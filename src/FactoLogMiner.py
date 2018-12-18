from logParser import parseLogFile


if __name__ == "__main__":
    #test
    import sys
    import os
    args = sys.argv[1::]
    if (len(args) != 3):
        raise Exception("Usage: FactoLogMiner.py <logFilePath> "
                        "<techDataFilePath> <gameName>")


    logfilepath = args[0]
    if (not os.path.isfile(logfilepath)):
        raise Exception(logfilepath+" does not exists.")
    techDataFilePath = args[1]
    if (not os.path.isfile(techDataFilePath)):
        raise Exception(techDataFilePath + " does not exists.")

    gamename = args[2]

    args = args[2::]

    parseLogFile(logfilepath, techDataFilePath, gamename)
    pass
