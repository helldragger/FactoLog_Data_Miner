import sqlite3


valid_datatypes = ["STATS", "EVENT"]
valid_stats_type_kw = ["SOLID", "FLUID", "KILLS", "BUILD", "LAUNCHED",
                       "ROCKETS", "EVOLUTION"]
valid_stats_section_kw = ["INPUT", "OUTPUT"]
valid_events_kw = ["RESEARCHED"]
item_name_set = set()


def createTable(table_name: str, args: str):
    global DB
    c = DB.cursor()
    command = f"CREATE TABLE {table_name} ({args})"
    try:
        c.execute(command)
        print("O)   TABLE " + table_name + " has been created".upper())
    except Exception as e:
        # alreadyExists
        print("X)   TABLE " + table_name + " already exists:".upper(), e)


def prepareDbTables():
    global DB
    c = DB.cursor()
    createTable("stat_tables", "tablename text UNIQUE")
    createTable("event_tables", "tablename text UNIQUE")
    # prod stats tables
    for stat_type in valid_stats_type_kw:
        createTable(stat_type, ",".join(
            ["insertionDate text", "force text", "timestamp text",
             "ticks numeric", "type text", "input real", "output real,"
                                                         "PRIMARY KEY(insertionDate, "
                                                         "force, "
                                                         "timestamp, "
                                                         "ticks, "
                                                         "type)"]))
        try:
            c.execute(f"CREATE VIEW IF NOT EXISTS {stat_type}_INPUT as SELECT " \
                      f"insertionDate, force, timestamp, ticks, type, " \
                      f"input AS value " \
                      f"FROM {stat_type}")
            print("O)   CREATED VIEW", stat_type + "_INPUT")

            c.execute(f"CREATE VIEW IF NOT EXISTS {stat_type}_OUTPUT as " \
                      f"SELECT " \
                      f"insertionDate, force, timestamp, ticks, type, " \
                      f"output AS value " \
                      f"FROM {stat_type}")
            print("O)   CREATED VIEW", stat_type + "_OUTPUT")

            c.execute(f"CREATE VIEW IF NOT EXISTS {stat_type}_STOCK as SELECT " \
                      f"insertionDate, force, timestamp, ticks, type, " \
                      f"(input-output) AS value " \
                      f"FROM {stat_type}")
            print("O)   CREATED VIEW", stat_type + "_STOCK")

            try:
                c.execute(f"INSERT INTO stat_tables VALUES ('" \
                          f"{stat_type}_INPUT')")
                print("O)   REGISTERED VIEW", stat_type + "_INPUT")

                c.execute(f"INSERT INTO stat_tables VALUES ('" \
                          f"{stat_type}_OUTPUT')")
                print("O)   REGISTERED VIEW", stat_type + "_OUTPUT")

                c.execute(f"INSERT INTO stat_tables VALUES ('" \
                          f"{stat_type}_STOCK')")
                print("O)   REGISTERED VIEW", stat_type + "_STOCK")

            except Exception as e:
                print("X)   FAILED TO REGISTER THE VIEWS", e)
        except Exception as e:
            print("X)   FAILED TO CREATE THE VIEWS: ", e)



    # event with unique data
    for event_type in valid_events_kw:
        createTable(event_type, ",".join(
            ["insertionDate text", "force text", "timestamp text",
             "ticks numeric", "type text", "value text,"
                                           "PRIMARY KEY("
                                           "insertionDate, "
                                           "force, "
                                           "timestamp, "
                                           "ticks, "
                                           "type)"]))
        try:
            c.execute(f"INSERT INTO event_tables VALUES (?)", (event_type,))
            print("O)   REGISTERED TABLE", event_type)
        except Exception as e:
            print("X)   FAILED TO REGISTER THE TABLE", e)

    DB.commit()


def processData(force: str, datatype: str, data: [str], timestamp: str):
    global DB
    c = DB.cursor()
    cache = {}
    # dealing with item names to numeric values
    if datatype == "STATS":
        current_type = None
        current_section = None
        for i in range(len(data)):
            word = data[i]
            if word in valid_stats_type_kw:
                if current_type != None:
                    for key in cache.keys():
                        force, timestamp, timestampVal, data_name = key
                        inputv = cache[key]["INPUT"]
                        outputv = cache[key]["OUTPUT"]
                        c.execute(f"INSERT INTO {current_type} " \
                                  f"(insertionDate," \
                                  f" force," \
                                  f" timestamp," \
                                  f" ticks," \
                                  f" type," \
                                  f" input," \
                                  f" output)" \
                                  f"VALUES " \
                                  f"(datetime('now')," \
                                  f" '{force}'," \
                                  f" '{timestamp}'," \
                                  f" {timestampVal}," \
                                  f" '{data_name}'," \
                                  f" {float(inputv)}," \
                                  f" {float(outputv)})")
                    cache = {}
                current_type = word
            elif word in valid_stats_section_kw:
                current_section = word
            else:
                worddata = word.split(":")
                if len(worddata) == 1:
                    # simple stat in format value
                    data_name = current_type.lower()
                    value = worddata[0]
                else:
                    # data in form name:value
                    data_name, value = word.split(":")
                # data value might be an integer or a float, so we just use a
                # float everytime to keep fluid values exact

                # We saves the data as two vectors:
                # [name1, name2, name3,...] & [value1, value2, value3,...]
                # if data is not present:insert
                # is data is present: update
                timestampVal = timestampStrToTicks(timestamp)

                key = (force, timestamp, timestampVal, data_name)
                if key not in cache.keys():
                    cache[key] = {
                        "INPUT":  0,
                        "OUTPUT": 0
                    }

                if current_section == "INPUT":
                    cache[key]["INPUT"] = value
                else:
                    cache[key]["OUTPUT"] = value



    # Dealing with events and their types
    elif datatype == "EVENT":
        # data[0] = event type, onward is the event data
        event_type = None
        for i in range(len(data)):
            word = data[i]
            if word in valid_events_kw:
                event_type = word
            else:
                worddata = word.split(":")
                if len(worddata) == 1:
                    # simple stat in format value
                    data_name = event_type.lower()
                    value = worddata[0]
                else:
                    # data in form name:value
                    data_name, value = word.split(":")

                c.execute(
                    f"INSERT INTO {event_type} VALUES (datetime('now'), ?, "
                    "?, ?, ?, ?)", (
                    force, timestamp, timestampStrToTicks(timestamp), data_name,
                    value))
    DB.commit()


def parseTimeStamp(timestamp: str):
    # format
    # hh:mm:ss.tt :
    hh, mm, ssntt = timestamp[:len(timestamp) - 2:].split(":")
    ss, tt = ssntt.split(".")
    return (int(hh), int(mm), int(ss), int(tt))


def timestampToStr(timestamp):
    return "".join(
        [str(timestamp[0]), ":", str(timestamp[1]), ":", str(timestamp[2])])


def timestampStrToTicks(timestamp: str):
    hh, mm, ss = timestamp.split(":")
    return int(hh) * 60 * 60 * 60 + int(mm) * 60 * 60 + int(ss) * 60


def parseLineData(linestr: str):
    # format:
    # timestamp: ;typeofdata;force/eventtype;data;data;data;
    linedata = linestr.split(";")
    # get the line timestamp
    timestamp = parseTimeStamp(linedata[0])
    datatype = linedata[1]
    datainfo = linedata[2]
    data = linedata[3:]

    if (datatype not in valid_datatypes):
        raise Exception("Unknown datatype:" + datatype)
    return (timestamp, datatype, datainfo, data)


def parseLogFile(filepath: str, gamename: str):
    # returns a dict with each force linked to a tuple containing an array of
    # timestamp and an array of each data contained at each timestamp
    global DB
    DB = sqlite3.connect("resources/" + gamename + ".db")
    print("sqlite version", sqlite3.sqlite_version)
    prepareDbTables()

    with open(filepath, "r") as logfile:
        for line in logfile.readlines():
            timestamp, datatype, force, data = parseLineData(line)
            # removing the \n from the end of line
            data = data[:-1:]
            processData(force, datatype, data, timestampToStr(timestamp))

    DB.close()
    return
