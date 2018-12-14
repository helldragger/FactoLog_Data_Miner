import matplotlib.pyplot as plt


locale = {
    "SOLID": {
        "INPUT":"items prod.",
        "OUTPUT":"items cons.",
        "STOCK":"items stocks"
    },
    "FLUID": {
        "INPUT":"fluid prod.",
        "OUTPUT":"fluid cons.",
        "STOCK":"fluid stocks"
    },
    "KILLS": {
        "INPUT":"kills",
        "OUTPUT":"deaths",
        "STOCK":"karma"
    },
    "BUILD": {
        "INPUT":"machines built",
        "OUTPUT":"machines removed",
        "STOCK":"machines placed"
    }
}


def timestampToStr(timestamp):
    return "".join([str(timestamp[0]),":", str(timestamp[1]),":",str(timestamp[2])])

def getDataPoint(stat_type, stat_section, index, data_point):
    global all_zeroes
    if stat_type in data_point.keys():
        if stat_section in data_point[stat_type].keys():
            return data_point[stat_type][stat_section]["values"][index]
    return 0.

def genStatsPlot(data, item_name_list):
    plotcount = 1
    totalforces = len(data.keys())
    for forceindex in range(totalforces):
        force = list(data.keys())[forceindex]
        if not "STATS" in data[force]:
            continue
        timestamps = data[force]["STATS"]["timestamps"]
        data_points = data[force]["STATS"]["data"]
        timestamps = list(map(timestampToStr, timestamps))



        # 1rst = input
        # 2nd = output
        # 3rd = input - output
        from logParser import valid_stats_type_kw
        from logParser import valid_stats_section_kw
        for stat_type in valid_stats_type_kw:
            for stat_section in valid_stats_section_kw:
                plt.subplot(totalforces*len(
                    valid_stats_type_kw), len(valid_stats_section_kw),
                            plotcount)
                plt.title(force + " "+locale[stat_type][stat_section])
                plt.grid(True)
                for item_index in range(len(item_name_list)):
                    plt.plot(timestamps,
                             list(
                                 map(
                                     lambda d, t=stat_type, s=stat_section,
                                            i=item_index : getDataPoint(t,
                                                                           s,
                                                                           i,
                                                                           d)
                                        , data_points)))


                plotcount += 1
                plt.subplots_adjust(wspace=0.3, hspace=0.3)
        plt.show()
