import sqlite3

valid_datatypes = ["STATS", "EVENT"]
valid_stats_type_kw = ["SOLID", "FLUID", "KILLS", "BUILD", "LAUNCHED",
                       "ROCKETS", "EVOLUTION"]
valid_stats_section_kw = ["INPUT", "OUTPUT", "STOCK"]
valid_events_unique_value_kw = ["RESEARCHED"]
item_name_set = set()


def prepareDbTables():
    global DB
    c = DB.cursor()

    # prod stats tables
    for stat_type in valid_stats_type_kw:
        for stat_section in valid_stats_section_kw:
            try:
                c.execute(
                    "CREATE TABLE " + stat_type + "_" + stat_section + " (" \
                                                                       "force text, " \
                                                                       "timestamp text,"
                                                                       "ticks numeric," \
                                                                       "dataname text , " \
                                                                       " value real)")
            except Exception:
                # alreadyExists
                pass

    # event with unique data
    for event_type in valid_events_unique_value_kw:
        try:
            c.execute("CREATE TABLE " + event_type + " (force text, timestamp " \
                                                     "text,ticks numeric, name text)")
        except Exception:
            # alreadyExists
            pass

    DB.commit()


def processData(force: str, datatype: str, data: [str], timestamp: str):
    global DB
    c = DB.cursor()

    #dealing with item names to numeric values
    if datatype == "STATS":
        current_type = None
        current_section = None
        for i in range(len(data)):
            word = data[i]
            if word in valid_stats_type_kw:
                current_type = word
            elif word in valid_stats_section_kw:
                current_section = word
            else:
                #data in form name:value
                data_name, value = word.split(":")
                #data value might be an integer or a float, so we just use a
                # float everytime to keep fluid values exact

                #We saves the data as two vectors:
                # [name1, name2, name3,...] & [value1, value2, value3,...]
                c.execute(
                    "INSERT INTO " + current_type + "_" + current_section + " " \
                                                                            "VALUES (?, ?,?, ?, ?)",
                    (
                    force, timestamp, timestampStrToTicks(timestamp), data_name,
                    float(value)))

    #Dealing with events and their types
    elif datatype == "EVENT":
        #data[0] = event type, onward is the event data
        event_type = data[0]
        if event_type in valid_events_unique_value_kw:
            c.execute('''INSERT INTO ? (?, ?, ?, ?)''', (
            event_type, force, timestamp, timestampStrToTicks(timestamp),
            data[1]))
    DB.commit()

def parseTimeStamp(timestamp:str):
    #format
    #hh:mm:ss.tt :
    hh, mm, ssntt = timestamp[:len(timestamp)-2:].split(":")
    ss, tt = ssntt.split(".")
    return (int(hh), int(mm), int(ss), int(tt))


def timestampToStr(timestamp):
    return "".join(
        [str(timestamp[0]), ":", str(timestamp[1]), ":", str(timestamp[2])])


def timestampStrToTicks(timestamp: str):
    hh, mm, ss = timestamp.split(":")
    return int(hh) * 60 * 60 * 60 + int(mm) * 60 * 60 + int(ss)*60

def parseLineData(linestr :str):
    #format:
    #timestamp: ;typeofdata;force/eventtype;data;data;data;
    linedata = linestr.split(";")
    #get the line timestamp
    timestamp = parseTimeStamp(linedata[0])
    datatype = linedata[1]
    datainfo = linedata[2]
    data = linedata [3:]

    if (datatype not in valid_datatypes):
        raise Exception("Unknown datatype:" + datatype)
    return (timestamp, datatype, datainfo, data)


def parseLogFile(filepath: str, gamename: str):
    #returns a dict with each force linked to a tuple containing an array of
    # timestamp and an array of each data contained at each timestamp
    global DB
    DB = sqlite3.connect("resources/" + gamename + ".db")
    prepareDbTables()

    with open(filepath, "r") as logfile:
        for line in logfile.readlines():
            timestamp, datatype, force, data = parseLineData(line)
            #removing the \n from the end of line
            data = data[:-1:]
            processData(force, datatype, data, timestampToStr(timestamp))

    DB.close()
    return
