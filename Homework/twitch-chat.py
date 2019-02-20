## import twitch library
import sys
import irc.bot
import requests

## KAFKA
from kafka import SimpleProducer, KafkaClient

class KafkaProducer():

    def __init__(self):
        self.topic = 'chat'
        self.kafka = KafkaClient('localhost:9092')

    def producer(self, data):
        producer = SimpleProducer(self.kafka)
        return producer.send_messages(self.topic, data.encode('utf-8'))

class TwitchBot(irc.bot.SingleServerIRCBot):

    def __init__(self, username, client_id, token, channel):
        self.username = username
        self.client_id = client_id
        self.token = token
        self.channel = '#' + channel

        url = 'https://api.twitch.tv/kraken/users?login' + channel
        headers = {'Client-ID': client_id, 'Accept': 'application.vnd.twitchtv.v5+json'}
        r = requests.get(url, headers=headers).json()
        self.channel_id = r['users'][0]['_id']

        server = 'irc.chat.twitch.tv'
        port = 6667
        print 'Connecting to ' + server + ' on port ' + str(port)
        irc.bot.SingleServerIRCBot.__init__(self, [(server, port, 'oaut:'+token)], username, username)

        def on_welcome(self, c, e):
            print 'Joining ' + self.channel

            c.cap('REQ', ':twitch.tv/membership')
            c.cap('REQ', ':twitch.tv/tags')
            c.cap('REQ', ':twitch.tv/commands')
            c.join(self.channel)

        def on_pubmsg(self, c, e):

            if e.arguments[0][:1] == '!':
                cmd = e.arguments[0].split(' ')[0][1:]
                print 'Received command: ' + cmd
                self.do_command(e, cmd)
            return


def main():

    ##
##      if len(sys.argv) != 5:
##          print("Usage: twitchbot <username> <client id> <token> <channel>")
##          sys.exit(1)
##
      username = 'teh24thson'
      client_id = 'u4q90pwgt3u47n7pmi5kbflsicq0oj'
      token = 'oauth:jtw9xhhnx09ufm5juznnn2sf75dfm6'
      channel = "#teh24thson"

      bot =  (username, client_id, token, channel)
      bot.start()


if __name__ == "__main__":
      main()
