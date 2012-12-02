#!/usr/bin/env ruby
require 'rubygems'
require 'json'
require 'time'
require 'date'
require 'mongo'
require 'twitter'
require 'ap'
require 'pp'

def get_connection
  return @db_connection if @db_connection
  db = URI.parse(ENV['MONGOHQ_URL'])
  db_name = db.path.gsub(/^\//, '')
  @db_connection = Mongo::Connection.new(db.host, db.port).db(db_name)
  @db_connection.authenticate(db.user, db.password) unless (db.user.nil? || db.user.nil?)
  @db_connection
end

if ARGV.length < 5
  puts "usage: #{$0} <consumer_key> <consumer_secret> <access_token> <access_token_secret> <twitter_screen_name>"
  exit
end

TWITTER_SCREEN_NAME = ARGV[4].downcase
consumer_key = ARGV[0]
consumer_secret = ARGV[1]
access_token = ARGV[2]
access_token_secret = ARGV[3]

$stderr.printf("consumer key:%s, consumer_secret:%s, access_token:%s, access_token_secret:%s\n",
              consumer_key, consumer_secret, access_token, access_token_secret)

Twitter.configure do |config|
  config.consumer_key = consumer_key
  config.consumer_secret = consumer_secret
  config.oauth_token = access_token
  config.oauth_token_secret = access_token_secret
end

db = get_connection
usersColl = db.collection("users")
user_info_object = Twitter.user(TWITTER_SCREEN_NAME)
user_info =  user_info_object.attrs
id_str  = user_info[:id_str]
$stderr.printf("Twitter API FOUND user:%s id:%s\n", TWITTER_SCREEN_NAME, id_str)
existingUser =  usersColl.find_one("id_str" => id)
if existingUser      
  usersColl.update({"id_str" =>id}, user_info)
else
  $stderr.printf("INSERTING user id:%s\n",id_str)
  user_info["user_info_initialized"] = true
  user_info["partial_following_screen_names"] = []
  usersColl.insert(user_info)
end

