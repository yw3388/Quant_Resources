#!/usr/bin/env python
# coding: utf-8
from functools import wraps
import hmac
import hashlib
import time
import logging
import websocket
import websockets
import asyncio
import json
import ssl
import asyncio
import websockets
import json
from websocket import create_connection, WebSocketConnectionClosedException
import pandas as pd
import influxdb
import influxdb_w2
from influxdb_w2 import Writer, Client
bitstamp_endpoint = 'wss://ws.bitstamp.net'
class Writer:
	def __init__(self, measurement):
		self.client = Client() 
		self.measurement = measurement
	def coinbase(self, msgs):
		field = dict()
		field.update(msgs)
		print(len(field))
			
		self.client.write_points_to_measurement(measurement = self.measurement,  time =None, tags = None, fields = field)



class bitstamp:

	def __init__(self, measurement):
		self.bs = []
		self.aq = []
		self.qn = []
		self.sub_params = {
			"event": "bts:subscribe",
			"data": {
				"channel": "order_book_btcusd"
		}}
		self.measurement = measurement
		self.writer = Writer(measurement)

	def subscribe_marketdata(self, ws):
		
		params ={
				'event': 'btc: subscribe',

				'data': {
					'channel': 'order_book_btcusd'
				}
			}
		market_depth_subscription = json.dumps(params)
			#send a json to web
		ws.send(market_depth_subscription)
	def on_close(self, ws):
		print("close")

	def on_open(self, ws):
		subscribe_marketdata(ws)
	def on_message(self, ws, message):
		data = json.loads(message)
		print(data)
	def on_error(self, ws, error):
		print(error)
	#ws = websocket.WebSocketApp(bitstamp_endpoint, on_message = on_message, on_error= on_error, on_close = on_close)





	def event(self):
		self.sub_params = {'event': 'btc:subscibe','data': {'channel': 'orderbook_btc_usd'}}
		return json.dumps(self.sub_params)

		self.sub_params = {
			"event": "bts:subscribe",
			"data": {
				"channel": "order_book_btcusd"
		}}
			#self.channels = [{"name": "ticker", "product_ids": ['BTC-USD']}]
		#self.sub_params = {'type': 'subscribe','product_ids': ['BTC-USD'], 'channels': ['full']}
	def connected(self, l):   
		ws = lambda url: create_connection(url)
		w = ws(l)
		w.send(json.dumps(self.sub_params))
		print(w.connected) 
		return w

	
	def orderbook(self, data):
		if 'data' in data.keys():
			d = data['data']
			
			self.qn.append(d)
			print(len(self.qn))
			if len(d) > 0:
				return d
			else:
				pass


	def c(self, function, l):
		
		w = function(l)
		while w.connected:
			w.ping("keepalive")

			data = w.recv()
			msg = json.loads(data)
			
			"""if 'data' in msg.keys():
				m = self.orderbook(msg)
				##highest bids

			if 'bids' or 'asks' in msg.keys():
				self.writer.coinbase(msg)"""
			print(msg)

	def get_order(self):
		self.bs += self.qn['bids']
		self.aq += self.qn['asks']
		return self.bs, self.aq

	
if  __name__ == "__main__":
	ccc= bitstamp("bitstamp2")	
	ccc.c(ccc.connected, "wss://ws.bitstamp.net")
	print(len(ccc.qn))

	k = pd.DataFrame(ccc.get_order()[0])
	b = pd.DataFrame(ccc.get_order()[1])
	k.to_csv('~/Desktop/bids.csv')
	b.to_csv('~/Desktop/asks.csv')