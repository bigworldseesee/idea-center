fs = require 'fs'
events = require 'events'
mongoose = require 'mongoose'
spawn = require('child_process').spawn
db = require './db'
util = require './util'

User = db.User
Syslog = db.Syslog
Session = db.Session
FileWatcher = util.FileWatcher
TimeStamp = util.TimeStamp
sleep = util.sleep

activeSession = {}
# (TODO) Fix: if the server restarts, some active sessions could be lost.


# Havester collects the changes from syslog and
# combines the log from `last` to form the session information
class Harvester extends events.EventEmitter

  constructor: (@logPath) ->
    Syslog.findOne {'name': @logPath},  (err, syslog) =>
      throw err if err
      if not syslog
        syslog = new Syslog
        syslog.path = @logPath
        syslog.checkedSize = 0
      @_syslog = syslog
      # @prevSize = @_syslog.checkedSize
      @prevSize = 0
      syslog.save (err) =>
        throw err if err
      # Setup done, ready to harvest
      command = spawn('last', ['-w'])
      data = ''
      command.stdout.on 'data', (chunk) =>
        data += chunk
      command.on 'close', =>
        fs.writeFileSync('./last.txt', data);
        @emit 'ready'

  harvest: ->
    @currSize = fs.statSync(@logPath).size
    rstream = fs.createReadStream @logPath,
      encoding: 'utf8'
      start: @prevSize
      end: @currSize
    @prevSize = @currSize
    data = ''
    rstream.on 'data', (chunk) =>
      data += chunk
    rstream.on 'end', =>
      lines = data.split "\n"
      @process line for line in lines
      @_syslog.prevSize = @prevSize
      @_syslog.save (err) =>
        throw err if err
      # Current log processing complete, notify the world.
      @emit 'finish'

  process: (line) ->
    words = line.split(/[ ]+/)
    proc = words[4]
    return if not proc
    bracketPos = proc.indexOf('[')
    return if bracketPos is -1
    procName = proc[0..bracketPos-1]
    id = proc[bracketPos+1...-2]
    timestamp = new TimeStamp(new Date().getFullYear(), words[0], words[1], words[2])

    if procName is 'pppd'
      if words[5] is 'peer' and words[6] is 'from'
        activeSession[id] = new Object
        activeSession[id].start = timestamp.toDate()
        activeSession[id].id = id
        activeSession[id].ip = words[9]
        @_setUsername id, timestamp # This is a async function
      
      # This is a failed connection, does not count.
      else if words[9] is '(Failed' and words[10] is 'to' and words[11] is 'authenticate' and words[12] is 'ourselves'
        delete activeSession[id]

      # Many connections are not successful and failed due a few reasons.
      # They have send some packets back and forth but the size are pretty small.
      # This is really an adhoc way of determining if a session is succesful or not.
      else if activeSession[id] and words[5] is 'Sent' and words[8] is 'received'
        if Number(words[6]) < 500 and Number(words[9]) < 500
          delete activeSession[id]
        else
          activeSession[id].sent = Number(words[6])
          activeSession[id].received = Number(words[9])

      else if activeSession[id] and words[5] is 'Exit.'
        @_saveSession id, timestamp, activeSession[id].received, activeSession[id].sent

    else if procName is 'cron'
      if words[6] is 'STARTUP'
        for id, session of activeSession
          @_saveSession id, timestamp, 0, 0

  _saveSession: (id, timestamp, received, sent) ->
    activeSession[id].end = timestamp.toDate()
    activeSession[id].duration = (activeSession[id].end - activeSession[id].start) / 1000
    thisSession = new Session (
      username: activeSession[id].username
      version: 0
      id: activeSession[id].id
      interface: activeSession[id].interface
      ip: activeSession[id].ip
      start: activeSession[id].start
      end : activeSession[id].end
      duration: activeSession[id].duration
      received: received
      sent: sent
    )
    delete activeSession[id]
    thisSession.save (err) =>
      throw err if err


  _setUsername: (id, timestamp) ->
    currDate = new Date()
    if currDate - timestamp.toDate() < 10000 # If the syslog and current time larger than 10 seconds
      setTimeout( =>
        command = spawn('last', ['-w', '-10'])
        data = ''
        command.stdout.on 'data', (chunk) =>
          data += chunk
        command.on 'close', =>
          @_setUsernameCore id, data, timestamp
       , 2000)
    else
      data = fs.readFileSync './last.txt', 
        encoding: 'utf8'
      @_setUsernameCore id, data, timestamp

  _setUsernameCore: (id, data, timestamp) ->
    year = timestamp.year
    month = timestamp.month
    day = timestamp.day
    time = timestamp.time
    records = data.split "\n"
    if activeSession[id]
      for record in records
        words = record.split(/[ ]+/)
        # The last message and the syslog message could be seconds apart, here release the check to 60 seconds
        if words[2] is activeSession[id].ip and Math.abs(new Date([year, words[4], words[5],words[6]].join(' ')) - timestamp.toDate()) < 60000
          activeSession[id].username = words[0]
          break
    else
      Session.findOne {'id': id},  (err, session) =>
        return if not session
        throw err if err
        for record in records
          words = record.split(/[ ]+/)
          if words[2] is activeSession[id].ip and Math.abs(new Date([year, words[4], words[5],words[6]].join(' ')) - timestamp.toDate()) < 60000
            session.username = words[0]
            session.save (err) =>
              throw err if err




module.exports.Harvester = Harvester
