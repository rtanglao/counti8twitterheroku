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
tweetsColl = db.collection("tweets")
usersColl = db.collection("users")

cursor = "-1"
while cursor != 0 do
  followers = Twitter.follower_ids(TWITTER_SCREEN_NAME, :cursor => cursor, :stringify_ids => true)
  followers.ids.each do |id|
    $stderr.printf("FOUND user id:%s\n", id)
    existingUser =  usersColl.find_one("id_str" => id)
    if existingUser      
      if !existingUser["partial_following_screen_names"].include?(TWITTER_SCREEN_NAME)
        existingUser["partial_following_screen_names"].push(TWITTER_SCREEN_NAME)
        $stderr.printf("UPDATING user id:%s ADDING screen_name:%s\n",id, TWITTER_SCREEN_NAME )
        usersColl.update({"id_str" =>id }, existingUser)
      else
        $stderr.printf("NOT UPDATING user id:%s because screen_name:%s is PRESENT\n",id, TWITTER_SCREEN_NAME )
      end
    else
      $stderr.printf("INSERTING user id:%s\n",id)
      user = { "id_str" => id, "user_info_initialized" => false,  "partial_following_screen_names" => [TWITTER_SCREEN_NAME],
               "tweets_retrieved_at" => Time.utc(2004, 3, 27) }
      usersColl.insert(user)
    end
  end
  cursor = followers.next_cursor
end
