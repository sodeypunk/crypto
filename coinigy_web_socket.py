import logging
import pandas as pd 
import json
import credentials
from collections import OrderedDict, namedtuple
from datetime import datetime
from dateutil import tz
from socketclusterclient import Socketcluster
logging.basicConfig(format="%s(levelname)s:%(message)s", level=logging.DEBUG)
pd.options.display.precision = 10

api_credentials = json.loads('{}')
api_credentials["apiKey"]=credentials.Coinigy.API
api_credentials["apiSecret"]=credentials.Coinigy.Secret

def your_code_starts_here(socket):

    ###Code for subscription
    socket.subscribe('ORDER-BTRX--BCC--BTC')                 # Channel to be subscribed
    socket.subscribe('ORDER-BTRX--XRP--BTC')                 # Channel to be subscribed
    socket.subscribe('ORDER-BTRX--XLM--BTC')                 # Channel to be subscribed
    socket.subscribe('ORDER-BTRX--LTC--BTC')                 # Channel to be subscribed

    socket.subscribe('ORDER-BINA--BCC--BTC')                 # Channel to be subscribed
    socket.subscribe('ORDER-BINA--XRP--BTC')                 # Channel to be subscribed
    socket.subscribe('ORDER-BINA--XLM--BTC')                 # Channel to be subscribed
    socket.subscribe('ORDER-BINA--LTC--BTC')                 # Channel to be subscribed

    market = namedtuple('market', ('exchange', 'symbol', 'average_price', 'average_price_min_vol', 'volume', 'is_min_vol', 'rows'))
    market_dict = {}
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()

    def getAge(utc_timestamp):
        local = datetime.strptime(utc_timestamp, '%Y-%m-%d %H:%M:%S').replace(tzinfo=from_zone).astimezone(to_zone)
        local = local.replace(tzinfo=None)
        diff = str(datetime.now() - local)

        return diff

    def calcArb(symbol, min_arb):
        # TODO convert market_dict into a pandas frame for more efficient search and calculation

        for key in market_dict:
            if (symbol in market_dict[key] and market_dict[key][symbol].is_min_vol):
                for key2 in market_dict:
                    if (key2 != key and symbol in market_dict[key2] and market_dict[key2][symbol].is_min_vol):
                        price1 = market_dict[key][symbol].average_price_min_vol
                        price2 = market_dict[key2][symbol].average_price_min_vol
                        arb = float(price2 - price1) / price1

                        print(symbol + ": price1: " + str(price1) + ", price2: " + str(price2) + ", arb1: " + str(arb))
                        if (arb > min_arb):
                            print("\n\n*** " + symbol + " Arb opportunity from " + key + " to " + key2 + " for " + str(arb*100) + "% ***")


    def channelmessage(key, data):                         # Messages will be received here
        #print ("\n\n\nGot data "+json.dumps(data, sort_keys=True)+" from channel "+key)
        print("\n\n\n")
        exchange = key[6:10]
        symbol = key[12:].replace('--','/')

        datapd = pd.DataFrame(data)
        datapd['price'] = pd.to_numeric(datapd['price'])
        datapd['cumu_total'] = datapd['total'].cumsum()

        min_vol_btc = 0.5
        min_arb =.01
        rows = len(datapd)

        #utc_timestamp = datapd['timestamp'][0]

        volume = datapd['total'].sum()
        index_list_min_vol = datapd.where(datapd['cumu_total'] >= min_vol_btc).dropna().index
        is_min_vol = len(index_list_min_vol) > 0
        average_price = datapd['price'].mean()

        if (is_min_vol):
            row_index = index_list_min_vol[0] + 1
            average_price_min_vol = datapd['price'][0:row_index].mean()
        else:
            average_price_min_vol = 0

        #print(exchange + ", " + symbol + ", " + str(average_price) + ", " + str(average_price_min_vol) + ", " + str(volume) + ", " + str(is_min_vol))
        new_market = market(exchange=exchange, symbol=symbol, average_price=average_price, rows = rows,
                            average_price_min_vol=average_price_min_vol, volume=volume, is_min_vol=is_min_vol)

        if (exchange not in market_dict):
            market_dict[exchange] = {}
        
        market_dict[exchange][symbol] = new_market

        print(market_dict)
        calcArb(symbol, min_arb)




    socket.onchannel('ORDER-BTRX--BCC--BTC', channelmessage) # This is used for watching messages over channel
    socket.onchannel('ORDER-BTRX--XRP--BTC', channelmessage) # This is used for watching messages over channel
    socket.onchannel('ORDER-BTRX--XLM--BTC', channelmessage) # This is used for watching messages over channel
    socket.onchannel('ORDER-BTRX--LTC--BTC', channelmessage) # This is used for watching messages over channel

    socket.onchannel('ORDER-BINA--BCC--BTC', channelmessage) # This is used for watching messages over channel
    socket.onchannel('ORDER-BINA--XRP--BTC', channelmessage) # This is used for watching messages over channel
    socket.onchannel('ORDER-BINA--XLM--BTC', channelmessage) # This is used for watching messages over channel
    socket.onchannel('ORDER-BINA--LTC--BTC', channelmessage) # This is used for watching messages over channel
    
    ###Code for emit

    def ack(eventname, error, data):
        print ("\n\n\nGot ack data " + json.dumps(data, sort_keys=True) + " and eventname is " + eventname)
    
    socket.emitack("exchanges",None, ack)
    socket.emitack("channels", "OK", ack)

def onconnect(socket):
    logging.info("on connect got called")

def ondisconnect(socket):
    logging.info("on disconnect got called")

def onConnectError(socket, error):
    logging.info("On connect error got called")

def onSetAuthentication(socket, token):
    logging.info("Token received " + token)
    socket.setAuthtoken(token)

def onAuthentication(socket, isauthenticated):
    logging.info("Authenticated is " + str(isauthenticated))
    def ack(eventname, error, data):
        print ("token is "+ json.dumps(data, sort_keys=True))
        your_code_starts_here(socket);

    socket.emitack("auth", api_credentials, ack)

if __name__ == "__main__":
    socket = Socketcluster.socket("wss://sc-02.coinigy.com/socketcluster/")
    socket.setBasicListener(onconnect, ondisconnect, onConnectError)
    socket.setAuthenticationListener(onSetAuthentication, onAuthentication)
    socket.setreconnection(False)
    socket.connect()
