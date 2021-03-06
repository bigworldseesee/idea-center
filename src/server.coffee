
mongoose = require 'mongoose'
harvester = require './harvester'
util = require './util'

config = require './config'
logPath = config.logPath
mongoose.connect config.harvesterUrl

coolFarmer = new harvester.Harvester(logPath)
lazyLandlord = new util.FileWatcher(logPath)


# On log path setup, farmer is ready and start to harvest
coolFarmer.on 'ready', -> coolFarmer.harvest()

# When leftover log harvest done, farmer emit 'finish' and policeman start to watch.
# When file change is found, policeman emit 'change' and stops watching.
coolFarmer.on 'finish', -> lazyLandlord.watch()

# Only when 'change' is received, farmer will start to harvest.
lazyLandlord.on 'change', -> coolFarmer.harvest()
