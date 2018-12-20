import sqlite3


valid_data_trees = {
    "SOLID":     ["INPUT", "OUTPUT"],
    "FLUID":     ["INPUT", "OUTPUT"],
    "KILLS":     ["INPUT", "OUTPUT"],
    "BUILD":     ["INPUT", "OUTPUT"],
    "LAUNCHED":  [],
    "ROCKETS":   [],
    "EVOLUTION": [],
    "RESEARCH":  ["COMPLETED", "PREREQUISITE"],
    "UNLOCKED":  ["RECIPE", "SOLID", "FLUID", "GIVEN_ITEM", "TURRET_ATTACK",
                  "GUN_SPEED", "OTHERS"]
}
stat_data_trees_roots = filter(
    lambda key: valid_data_trees[key] == ["INPUT", "OUTPUT"],
    valid_data_trees.keys())

migration_values = {
    "RESEARCHED":               ("RESEARCH", "COMPLETED"),
    "unlocked-items":           ("UNLOCKED", "SOLID"),
    "unlocked-fluids":          ("UNLOCKED", "FLUID"),
    "prerequisites-researches": ("RESEARCH", "PREREQUISITE")
}


def createTable(table_name: str, args: str):
    global DB
    c = DB.cursor()
    command = f"CREATE TABLE {table_name} ({args})"
    try:
        c.execute(command)
        print("O)   TABLE " + table_name + " has been created".upper())
    except Exception as e:
        # alreadyExists
        print("X)   TABLE " + table_name + " could not be created:".upper(), e)


def prepareDbTables():
    global DB
    c = DB.cursor()
    createTable("stat_views", "view_name text UNIQUE")

    createTable("raw_data", "date text NOT NULL, game_name text NOT NULL, "
                            "force text NOT NULL, ig_date text NOT NULL, "
                            "ig_tick numeric NOT NULL, data_type text NOT NULL, "
                            "data_subtype text NOT NULL, data_name text NOT "
                            "NULL, "
                            "value text ")

    createTable("analyzed_data", "date text NOT NULL, game_name text NOT "
                                 "NULL, "
                                 "force text NOT NULL, ig_date text NOT NULL, "
                                 "ig_tick numeric NOT NULL, data_type text NOT NULL, "
                                 "data_subtype text NOT NULL, data_name text NOT "
                                 "NULL, "
                                 "analysis_type text NOT NULL,"
                                 "analysis_subtype text NOT NULL, analysis_name "
                                 " text NOT NULL, value text")

    try:
        c.execute("""
        CREATE VIEW IF NOT EXISTS last_update AS
        SELECT max(date) AS date, game_name
        FROM raw_data 
        GROUP BY game_name
        """)

        c.execute("""
        CREATE VIEW IF NOT EXISTS last_data AS
        SELECT r.* 
        FROM raw_data r
        JOIN last_update lu 
        WHERE r.date =lu.date
        """)

        c.execute("""
        CREATE VIEW IF NOT EXISTS forces AS 
        SELECT r.ig_tick, r.ig_date, r.force 
        FROM raw_data r
        JOIN last_update lu
        WHERE r.date=lu.date and r.game_name = lu.game_name
        GROUP BY r.force
        """)

        c.execute("""
        CREATE VIEW IF NOT EXISTS completed_researches AS 
        SELECT r.force, r.ig_tick, r.ig_date, r.data_name, r.value 
        FROM raw_data r
        JOIN last_update lu
        WHERE r.date=lu.date and r.game_name = lu.game_name and r.data_type == "RESEARCH" and r.data_subtype == 
        "COMPLETED"
        """)

        c.execute("""
        CREATE VIEW IF NOT EXISTS researches_prerequisites as 
        SELECT r.force, r.ig_tick, r.ig_date, r.data_name, r.data_value 
        FROM raw_data r
        JOIN last_update lu
        WHERE r.date=lu.date and r.game_name = lu.game_name and r.data_type == "RESEARCH" and r.data_subtype == 
        "PREREQUISITE"
        """)

        c.execute("""
        CREATE VIEW IF NOT EXISTS unlocked_items AS 
        SELECT r.force, r.ig_tick, r.ig_date, r.data_name , r.value 
        FROM raw_data r
        JOIN last_update lu
        WHERE r.date=lu.date and r.game_name = lu.game_name and r.data_type == 
        "UNLOCKED" and r.data_subtype == "SOLID"
        """)

        c.execute("""
        CREATE VIEW IF NOT EXISTS unlocked_fluids AS 
        SELECT r.force, r.ig_tick, r.ig_date, r.data_name , r.value 
        FROM raw_data r
        JOIN last_update lu
        WHERE r.date=lu.date and r.game_name = lu.game_name and r.data_type == 
        "UNLOCKED" and r.data_subtype == "FLUID"
        """)

        c.execute("""
        CREATE VIEW IF NOT EXISTS unlocked_recipes AS 
        SELECT r.force, r.ig_tick, r.ig_date, r.data_name , r.value 
        FROM raw_data r
        JOIN last_update lu
        WHERE r.date=lu.date and r.game_name = lu.game_name and r.data_type == 
        "UNLOCKED" and r.data_subtype == "RECIPE"
        """)

        c.execute("""
        CREATE VIEW IF NOT EXISTS unlocked_gifts AS 
        SELECT r.force, r.ig_tick, r.ig_date, r.data_name , r.value
        FROM raw_data r
        JOIN last_update lu
        WHERE r.date=lu.date and r.game_name = lu.game_name and r.data_type == 
        "UNLOCKED" and r.data_subtype == "ITEM_GIVEN"
        """)

        c.execute("""
        CREATE VIEW IF NOT EXISTS unlocked_turret_attack AS 
        SELECT r.force, r.ig_tick, r.ig_date, r.data_name , r.value
        FROM raw_data r
        JOIN last_update lu
        WHERE r.date=lu.date and r.game_name = lu.game_name and r.data_type == 
        "UNLOCKED" and r.data_subtype == "TURRET_ATTACK"
        """)

        c.execute("""
        CREATE VIEW IF NOT EXISTS unlocked_gun_speed AS 
        SELECT r.force, r.ig_tick, r.ig_date, r.data_name , r.value
        FROM raw_data r
        JOIN last_update lu
        WHERE r.date=lu.date and r.game_name = lu.game_name and r.data_type == 
        "UNLOCKED" and r.data_subtype == "GUN_SPEED"
        """)

        c.execute("""
        CREATE VIEW IF NOT EXISTS unlocked_others AS 
        SELECT r.force, r.ig_tick, r.ig_date, r.data_name , r.value
        FROM raw_data r
        JOIN last_update lu
        WHERE r.date=lu.date and r.game_name = lu.game_name and r.data_type == 
        "UNLOCKED" and r.data_subtype == "OTHERS"
        """)

        c.execute("""
        CREATE VIEW IF NOT EXISTS unlocked_all AS 
        SELECT r.force, r.ig_tick, r.ig_date, r.data_name, r.value 
        FROM raw_data r
        JOIN last_update lu
        WHERE r.date=lu.date and r.game_name = lu.game_name and r.data_type == 
        "UNLOCKED" 
        """)
        print("O)   CREATED OVERVIEW VIEWS")
    except Exception as e:
        print("X)   FAILED TO CREATE THE VIEWS: ", e)


    # prod stats tables
    for stat_type in filter(
            lambda key: valid_data_trees[key] == ["INPUT", "OUTPUT"],
            valid_data_trees.keys()):
        try:
            c.execute(f"""
                CREATE VIEW IF NOT EXISTS {stat_type} AS 
                SELECT ri.date, ri.game_name, ri.force, ri.ig_tick, ri.ig_date, 
                ri.data_name, ri.value AS input, ro.value AS output, 
                ri.value - ro.value AS stock
                FROM raw_data ri
                INNER JOIN (
                    SELECT date, game_name, force, ig_tick, data_name, value
                    FROM raw_data
                    WHERE data_type = '{stat_type}' and data_subtype='OUTPUT'
                ) ro
                WHERE ri.date=ro.date and ri.game_name = ro.game_name and ri.ig_tick 
                = ro.ig_tick and ri.force = ro.force and ri.data_type = '{stat_type}' and 
                data_subtype='INPUT' and ri.data_name=ro.data_name
                """)
            print("O)   CREATED VIEW", stat_type)

            try:
                c.execute(f"INSERT INTO stat_views VALUES ('{stat_type}')")
                print("O)   REGISTERED VIEW", stat_type)

            except Exception as e:
                print("X)   FAILED TO REGISTER THE VIEWS", e)
        except Exception as e:
            print("X)   FAILED TO CREATE THE VIEWS: ", e)

    DB.commit()


def processData(date: str, game_name: str, force: str, ig_date: str,
                ig_tick: int, data: [str]):
    global DB
    c = DB.cursor()

    # dealing with item names to numeric values
    data_type = None
    data_subtype = None
    for word in data:
        if word in migration_values.keys():
            data_type, data_subtype = migration_values[word]
        elif word is None:
            continue
        elif word == '':
            continue
        elif word in valid_data_trees.keys():
            data_type = word
        elif word in valid_data_trees[data_type]:
            data_subtype = word
        elif data_subtype is not None and data_type is not None:
            worddata = word.split(":")
            if len(worddata) == 1:
                # simple stat in format value
                data_name = data_type
                data_value = worddata[0]
            else:
                # data in form name:value
                data_name, data_value = word.split(":")
            # data value might be an integer or a float, so we just use a
            # float everytime to keep fluid values exact

            try:
                c.execute(f"""INSERT INTO raw_data VALUES ('{date}', 
                '{game_name}', '{force}', '{ig_date}', {ig_tick}, 
                '{data_type}', '{data_subtype}', '{data_name}', '{data_value}')""")
            except Exception as e:
                print("Could not insert following data in the raw_data table:")
                print(f"""('{date}', 
                '{game_name}', '{force}', '{ig_date}', {ig_tick}, 
                '{data_type}', '{data_subtype}', '{data_name}', '{data_value}')""")
                print(e)

    DB.commit()


def parseTimeStamp(timestamp: str):
    # format
    # hh:mm:ss.tt :
    hh, mm, ssntt = timestamp[:len(timestamp) - 2:].split(":")
    ss, tt = ssntt.split(".")
    return (int(hh), int(mm), int(ss), int(tt))


def timestampToStr(timestamp: (int, int, int, int)):
    return "".join(
        [str(timestamp[0]), ":", str(timestamp[1]), ":", str(timestamp[2])])


def timestampStrToTicks(timestamp: str):
    hh, mm, ss = timestamp.split(":")
    return int(hh) * 60 * 60 * 60 + int(mm) * 60 * 60 + int(ss) * 60


def parseLineData(linestr: str):
    # format:
    # timestamp: ;typeofdata;force/eventtype;data;data;data;
    linedata = linestr[:-1:].split(";")
    # get the line timestamp
    ig_date = timestampToStr(parseTimeStamp(linedata[0]))
    force = linedata[2]
    data = linedata[3:]

    return (ig_date, timestampStrToTicks(ig_date), force, data)


def parseTechLine(linestr: str):
    # format => research-name:name;<unlocked-items;item-1;...;><unlocked-fluids
    # ;fluid-1;...;><prerequisite_researches;r_name-1;...;>
    line = linestr[:-1:].split(";")
    r_name = line[0].split(":")[1]
    data = line[1::]

    entries = []
    data_type, data_subtype = None, None

    for word in data:
        if word in migration_values.keys():
            data_type, data_subtype = migration_values[word]
        elif word is None:
            continue
        elif word == '':
            continue
        elif word in valid_data_trees.keys():
            data_type = word
        elif word in valid_data_trees[data_type]:
            data_subtype = word
        else:
            entries.append((data_type, data_subtype, r_name, word))
    return entries


def parseLogFile(logfilepath: str, techdatapath: str, game_name: str):
    # returns a dict with each force linked to a tuple containing an array of
    # timestamp and an array of each data contained at each timestamp
    global DB

    DB = sqlite3.connect("resources/" + game_name + ".db")

    print("sqlite version", sqlite3.sqlite_version)

    prepareDbTables()
    c = DB.cursor()
    date = c.execute("SELECT DATETIME('now');").fetchone()[0]

    print("Parsing log file data...")
    with open(logfilepath, "r") as logfile:
        for line in logfile.readlines():
            ig_date, ig_tick, force, data = parseLineData(line)
            processData(date, game_name, force, ig_date, ig_tick, data)

    print("Parsing tech tree data...")
    with open(techdatapath, "r") as techfile:
        for line in techfile.readlines():
            entries = parseTechLine(line)
            for entry in entries:
                data_type, sub_type, data_name, data_value = entry
                c.execute(f"""INSERT INTO raw_data VALUES ('{date}', 
                '{game_name}', '{force}', '{ig_date}', {ig_tick}, 
                '{data_type}', '{sub_type}', '{data_name}', '{data_value}')""")
                DB.commit()


    DB.close()
    return
