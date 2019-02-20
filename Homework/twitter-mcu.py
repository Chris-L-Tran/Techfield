from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

access_token = "69047313-gSGTCEQlGCLoTyLhSVzhTfmPydFPjTfmlok0MGrJo"
access_token_secret = "WcZkwjuLMHw2HvJdkHPBnfA8ECdFxkH7d4Xdzww9yKbyG"
consumer_key = "AJaT9gcrPaK5I08j2GOuNq3ly"
consumer_secret = "A3vpVuH5kdRWZH3dkRLOprdw1QN0z5LOcSJ9H3B2wmyc2eecJs"

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("MCU", data.encode('utf-8'))
        print (data)
        return True 
    def on_error(self, status):
        print (status)


kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track="MCU")
