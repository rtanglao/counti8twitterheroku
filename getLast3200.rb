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

batch = 1
num_tweets = 0
lowest_tweet_id = 0
previous_lowest_tweet_id = 0
loop do 
  $stderr.printf("LOWEST tweet id:%s, batch:%d\n",lowest_tweet_id.to_s, batch)
  param_hash = {:count => 200, :trim_user => true, :include_rts => true,
    :include_entities => true, :contributor_details => true, :exclude_replies => false}
  if batch == 1
    param_hash[:count] = 200
  else
    param_hash[:max_id] = lowest_tweet_id - 1
  end
  tried_previously = false  
  begin 
    Twitter.user_timeline(TWITTER_SCREEN_NAME, param_hash).each do |tweet|
      t = tweet.attrs
      PP.pp(t, STDERR)
      id = t[:id_str].to_i
      if lowest_tweet_id == 0
        lowest_tweet_id = id
      elsif id < lowest_tweet_id
        lowest_tweet_id = id
      end
      id_str = t[:id_str]
      existingTweet =  tweetsColl.find_one("id_str" => id_str)
      if existingTweet      
        $stderr.printf("UPDATING tweet id:%s\n",id_str)
        tweetsColl.update({"id_str" =>id_str}, t)
      else
        $stderr.printf("INSERTING tweet id:%s\n",id_str)
        tweetsColl.insert(t)
      end
    end
    num_tweets += 200
    batch += 1
    if num_tweets == 3200 || previous_lowest_tweet_id == lowest_tweet_id
      break
    else
      previous_lowest_tweet_id = lowest_tweet_id
    end
    $stderr.printf("previous_lowest_tweet_id:%d, lowest_tweet_id:%d\n", 
      previous_lowest_tweet_id, lowest_tweet_id)
  rescue Twitter::Error::ServiceUnavailable, Twitter::Error::BadGateway
    if tried_previously
      raise
    else
      tried_previously = true
      $stderr.printf("twitter ruby exception error, re-trying in 30 seconds\n")
      sleep(30)
      retry
    end
  end   
end
usersColl = db.collection("users")
existingUser =  usersColl.find_one(:screen_name => TWITTER_SCREEN_NAME)
if existingUser  
  $stderr.printf("UPDATING tweets_retrieved_at user id:%s\n",TWITTER_SCREEN_NAME)
  existingUser["tweets_retrieved_at"] = Time.now.utc
  usersColl.update({:screen_name => TWITTER_SCREEN_NAME},existingUser)
end
$stderr.printf("DONE!\n")
