# user.coffee
mongoose = require 'mongoose'
Schema = mongoose.Schema

UserSchema = Schema (
  username : String
)

SyslogSchema = Schema (
  path: String
  checkedSize: Number
)

SessionSchema = Schema (
  username: String
  version: Number
  id: String
  interface: String
  ip: String
  start: Date
  end : Date
  duration: Number
  received: Number
  sent: Number
)

###
DAU = mongoose.Schema(
  ymd: String
  username: [String]
)
###

# create the model for users and expose it to our app
module.exports.User = mongoose.model('User', UserSchema)
module.exports.Syslog = mongoose.model('Syslog', SyslogSchema)
module.exports.Session = mongoose.model('Session', SessionSchema)
#module.exports.dauModel = mongoose.model('dauModel', dauSchema)
