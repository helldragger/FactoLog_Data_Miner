import numpy as np

valid_datatypes = ["STATS", "EVENT"]
valid_stats_type_kw = ["SOLID", "FLUID", "KILLS", "BUILD"]
valid_stats_section_kw = ["INPUT", "OUTPUT", "STOCK"]
valid_events_unique_value_kw = ["RESEARCHED"]
item_name_set = set()

def processData(datatype:str, data:[str]):
    datatree = {}
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
                if current_type not in datatree.keys():
                    datatree[current_type] = {}
                if current_section not in datatree[current_type].keys():
                    datatree[current_type][current_section] = {"names":[],
                                                               "values":[]}
                #data in form name:value
                data_name, value = word.split(":")
                #data value might be an integer or a float, so we just use a
                # float everytime to keep fluid values exact

                #We saves the data as two vectors:
                # [name1, name2, name3,...] & [value1, value2, value3,...]
                datatree[current_type][current_section]["names"].append(
                    data_name)
                datatree[current_type][current_section]["values"].append(
                    float(value))
                if data_name not in item_name_set:
                    item_name_set.add(data_name)

    #Dealing with events and their types
    elif datatype == "EVENT":
        #data[0] = event type, onward is the event data
        event_type = data[0]
        if event_type in valid_events_unique_value_kw:
            datatree[event_type] = data[1:]

    return datatree

def parseTimeStamp(timestamp:str):
    #format
    #hh:mm:ss.tt :
    hh, mm, ssntt = timestamp[:len(timestamp)-2:].split(":")
    ss, tt = ssntt.split(".")
    return (int(hh), int(mm), int(ss), int(tt))

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

def parseLogFile(filepath:str):
    #returns a dict with each force linked to a tuple containing an array of
    # timestamp and an array of each data contained at each timestamp
    logdata = {}

    with open(filepath, "r") as logfile:
        for line in logfile.readlines():
            timestamp, datatype, force, data = parseLineData(line)
            #removing the \n from the end of line
            data = data[:-1:]
            processed_data = processData(datatype, data)
            if len(processed_data.keys()) != 0:
                if force not in logdata.keys():
                    logdata[force] = {}
                if datatype not in logdata[force].keys():
                    logdata[force][datatype] = {"timestamps":[], "data":[]}

                logdata[force][datatype]["timestamps"].append(timestamp)
                logdata[force][datatype]["data"].append(processed_data)

    #data has been processed, every item name of the current game has been
    # found
    #reformatting the data with a fixed vector format
    item_name_list = list(item_name_set)
    for force in logdata:
        if "STATS" in logdata[force].keys():
            data = logdata[force]["STATS"]["data"]
            for sample in data:
                for stat_type in sample.keys():
                    data_type = sample[stat_type]
                    for stat_section in data_type.keys():
                        data_section = data_type[stat_section]
                        data_vector = np.zeros(len(item_name_list))
                        vector_index = 0
                        for item_name in item_name_list:
                            if item_name in data_section["names"]:
                                item_index = data_section["names"].index(item_name)
                                data_vector[vector_index] = data_section[
                                    "values"][item_index]
                            vector_index += 1
                        data_section["names"] = item_name_list
                        data_section["values"] = data_vector
                    #generating the stock data
                    data_section = {"names":item_name_list,
                                    "values":np.zeros(len(item_name_list))}
                    input_section = data_type["INPUT"]
                    output_section = data_type["OUTPUT"]
                    data_vector = np.zeros(len(item_name_list))
                    vector_index = 0
                    for item_name in item_name_list:
                        if item_name in input_section["names"]:
                            item_index = data_section["names"].index(item_name)
                            data_vector[vector_index] = input_section["values"][
                                item_index] - output_section["values"][item_index]
                        vector_index += 1
                    data_section["names"] = item_name_list
                    data_section["values"] = data_vector
                    sample[stat_type]["STOCK"] = data_section








    return logdata, item_name_list
