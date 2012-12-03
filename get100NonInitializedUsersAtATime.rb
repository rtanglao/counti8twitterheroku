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

if ARGV.length < 4
  puts "usage: #{$0} <consumer_key> <consumer_secret> <access_token> <access_token_secret>"
  exit
end

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

def get100orLessUsers(id_str_array, usersColl)
  users = Twitter.users(id_str_array)
  users.each do |full_user_info|
    full_user_info_hash = full_user_info.attrs 
    full_user_info_hash["user_info_initialized"] = true
    id_str = full_user_info_hash[:id_str]
    mongo_user = usersColl.find_one(:id_str => id_str)
    if mongo_user
      full_user_info_hash["partial_following_screen_names"] = mongo_user["partial_following_screen_names"]
      full_user_info_hash["tweets_retrieved_at"] = mongo_user["tweets_retrieved_at"]
      $stderr.printf("UPDATING id:%s\n", id_str)
      usersColl.update({:id_str => id_str}, full_user_info_hash)
    else
      $stderr.printf("INSERTING id:%s\n", id_str)
      usersColl.insert({:id_str => id_str}, full_user_info_hash)
    end
  end
end

number_blank_users_found = 0
id_str_array = []
usersColl.find().each do |u|
  if !u["user_info_initialized"]
    id_str_array.push(u[:id_str].to_i)
    number_blank_users_found += 1
  end
  if number_blank_users_found == 100
    get100orLessUsers(id_str_array, usersColl)
    number_blank_users_found = 0
    id_str_array = []
  end
end
if number_blank_users_found != 0
  get100orLessUsers(id_str_array, usersColl)
end
