#Giorgos Mitaros AM: 2312
#Email: gmitaros@gmail.com 
#Email: cs122312@cse.uoi.gr
#
#
# python casormu.py 127.0.0.1 0 2312
# python casormu.py 127.0.0.1 1 2313
# python casormu.py 127.0.0.1 2 2314
# 
#

import optparse
import socket
import random
import time
import heapq
import copy
import sys
import threading


from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet import reactor

connections = 0
transports = []


class Message:

	def __init__(self, id, type, msg, vClock, f, originalID, newId):        
		self.msgID = id 
		self.msgType = type
		self.message = msg
		self.vClock = vClock
		self.first = f
		self.creatorID = originalID
		self.newID = newId

	def toString(self):
		string = str(self.msgID)+"."+str(self.msgType)+"."+str(self.message)+"."+str(self.vClock[0])+"."+str(self.vClock[1])+"."+str(self.vClock[2])+"."+str(self.first)+"."+str(self.creatorID)+"."+str(self.newID)
		return string

def parse_args():
	usage = """usage: %prog [options] [hostname] [thread number] [port]

	 python totormu.py 127.0.0.1 0 2312 """

	parser = optparse.OptionParser(usage)

	_, args = parser.parse_args()

	if len(args) != 3:
		print parser.format_help()
		parser.exit()

	address, threadNo, port = args

	return address, threadNo, port


class Peer(Protocol):

	connected = False
	counter = 0
	flag = 0
	f = None
	overallSequenceNumber = 0
	groupSequenceNumber = 0 # At Each process message received
	messagesReceived = 0

	#Message acks block
	acks = {}

	#vector Clock
	vClock = [0,0,0]
	vectorClockProcess = [0,0,0];

	#Queue
	orderQueue = [] #PriorityQueue
	msgWaitingQueue = {} #Hashtable with Key= msg.msgID and Value= msg.message
	globalEvent = [] #

	#Locks
	socketsLock = threading.Lock()
	queueLock = threading.Lock()

	def __init__(self, factory):
		self.factory = factory

	def connectionMade(self):
		print "Connection Happened"
		global connections
		connections += 1
		global transports
		transports.append(self.transport)
		print "procNo: " + procNo + " connections: " + str(connections)
		if (connections == 2):
			#Ftiaxnw to arxeio gia na grafw ta minimata pou lamvanw
			fileName ="delivered-messages-"+str(procNo) 
			self.f = open(fileName, 'w')
			print "\n\n" 
			self.loop()

	def loop(self):
		#Write message
		if (self.flag < 20 ):
			if (int(procNo) == 0):
				self.vClock[int(procNo)]+=1
				send = Message((self.counter), str("message"),str("Message: "+str(self.flag)), (self.vClock), False, int(procNo), int(0))
				(self.f).write("ID: "+ str(send.msgID)+" "+ str(send.message)+ "\n")
				self.counter+=1
				self.sendUpdate(send)

				self.deliverMessage()
			reactor.callLater(3, self.loop)
			self.flag+=1
		else:
			self.deliverMessage()
			reactor.callLater(2, self.loop)
		

	def deliverMessage(self):
		message = None
		self.queueLock.acquire()

		if(len(self.orderQueue) > 0):
			priority, m = self.orderQueue[0]
			print priority, self.groupSequenceNumber, self.msgWaitingQueue.keys()
			if(priority[1] == self.groupSequenceNumber and (priority[0] in self.msgWaitingQueue)):
				print "Inside msg waitig group", self.groupSequenceNumber
				msg = self.msgWaitingQueue[priority[0]]
				message = msg
				del self.msgWaitingQueue[priority[0]]
				print "exporting this: ", msg
				(self.f).write("ID: "+ str(priority[0])+" "+ str(msg)+ "\n")
				self.groupSequenceNumber += 1
				priority, m = heapq.heappop(self.orderQueue)

		self.queueLock.release()
		if (self.flag >=19 and (len(self.orderQueue) <= 0)):
			self.f.close()
		return message

	def sendUpdate(self, message):
		#Vazw paules gia na min mplekontai ta minimata metaksi tous
		msg = message.toString()
		msg = "-"+msg+"-|/"

		#Stelnw se olous kai ston eauto mou
		self.socketsLock.acquire()
		try:
			global transports
			for transport in transports:
				transport.write(msg)
		except Exception, ex1:
			print "Exception trying to send: ", ex1.args[0]
		self.socketsLock.release()


	def causallOrder(self, msg):
		
		if(str(msg.msgType) == "Order") :
			priority = (msg.msgID, msg.newID)
			heapq.heappush(self.orderQueue, ((msg.msgID, msg.newID), msg))
		else:
			id = (msg.msgID, msg.message)
			self.msgWaitingQueue[msg.msgID]=msg.message
			#print "Vector Clock: ", msg.vClock
			if(int(procNo) == 1):
				flag = False
				if (msg.vClock[msg.creatorID] == (self.vectorClockProcess[msg.creatorID] + 1) ):
					flag = True
				else:
					flag = False

				if(flag):
					vc = [-1,-1,-1]
					self.vectorClockProcess[msg.creatorID] = max(self.vectorClockProcess[msg.creatorID],msg.vClock[msg.creatorID])
					m = Message(msg.msgID, "Order", "order-order", vc, False, int(procNo), self.overallSequenceNumber)
					self.overallSequenceNumber = self.overallSequenceNumber + 1
					#print "i am sending order msg..."
					self.causallOrder(m)
					self.sendUpdate(m)
					
				else:
					self.globalEvent.append(msg)
				
				if (len(self.globalEvent) > 0) :
					for first in self.globalEvent:
						f = False
						print self.printMsg(first)
						if (first.vClock[first.creatorID] == (self.vectorClockProcess[first.creatorID] + 1) ):
							f = True
						else:
							f = False
						
						if(f):
							vc = [-1,-1,-1]
							self.vectorClockProcess[first.creatorID] = max(self.vectorClockProcess[first.creatorID],first.vClock[first.creatorID]);
							m = Message(first.msgID, "Order", "order-order", vc, False, (self.procNo), overallSequenceNumber)
							self.overallSequenceNumber += 1
							globalEvent.remove(first)
							self.causallOrder(m)
							self.sendUpdate(m)
			else:
				for i in range(0,3) :
					self.vClock[i]=max(self.vClock[i],msg.vClock[i])
				
		




	def dataReceived(self, data):
		msgs= data.split("|")
		start = '-'
		end = '-'
		for minima in msgs:
			#print minima
			if len(minima)>5:
				#Afairw tis paules ('-')
				minima = minima[minima.find(start)+len(start):minima.rfind(end)]
				minima = minima.split(".")
				#Dimiourgw ena antikeimeno tupou Message
				msg = self.createMessage(minima)
				self.printMsg(msg)
				self.causallOrder(msg)
		

	def connectionLost(self, reason):
		print "Disconnected"

	def done(self):
		self.factory.finished(self.acks)

	def createMessage(self, var):
		msgID = var[0] 
		msgType = var[1]
		message = var[2]
		VC = int(var[3])
		VC1 = int(var[4])
		VC2 = int(var[5])
		vClock = [VC,VC1,VC2]
		first = (var[6] == "True")
		creatorID = var[7]
		newID = var[8]
		msgObject = Message(int(msgID), msgType, message, vClock, first, int(creatorID), int(newID) )
		
		return msgObject

	def printMsg(self, msg):
		print "----Message Info----"
		print "ID: ", msg.msgID
		print "Type: ", msg.msgType
		print msg.message
		print "senderClock: ", msg.vClock
		print "First: ", msg.first
		print "creatorId:", msg.creatorID
		print "newID:", msg.newID
		print "---------------------\n"


class PeerFactory(ClientFactory, ReconnectingClientFactory):

	def __init__(self):
		print '@__init__'
		self.acks = 0
		self.records = []

	def finished(self, arg):
		self.acks = arg
		self.report()

	def report(self):
		print 'Received %d acks' % self.acks

	def clientConnectionFailed(self, connector, reason):
		print 'Failed to connect to:', connector.getDestination()
		self.finished(0)

	def clientConnectionLost(self, connector, reason):
		print 'Lost connection.  Reason:', reason
		# Connect to another peer with following host and port
		# Host and port could be read from a list which stores peer information
		connector.host = '127.0.0.1'
		connector.port = 9999
		ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

	def startFactory(self):
		print "@startFactory"

	def stopFactory(self):
		print "@stopFactory"

	def buildProtocol(self, addr):
		print "@buildProtocol"
		protocol = Peer(self)
		return protocol

	

if __name__ == '__main__':
	address, procNo, porta = parse_args()


	if (int(procNo) == 0):
		print "I am process " + procNo 
		print "Addr: "+ address + "\nPort: " + porta
		print "Local ip: " + socket.gethostbyname(socket.gethostname())+"\n"
		port = int(porta)
		server = PeerFactory()
		reactor.listenTCP(port, server)
		print "Starting server @" + address + " port " + str(port)
	elif(int(procNo) == 1):
		#Client things
		print "I am process " + procNo 
		factory = PeerFactory()
		port = int(porta) - 1
		print "Connecting to host " + address + " port " + str(port)
		reactor.connectTCP(address, port, factory)
		#Server things
		server2 = PeerFactory()
		reactor.listenTCP(int(porta), server2)
		print "Starting server @" + address + " port " + str(porta)
	elif(int(procNo) == 2):
		print "I am process " + procNo
		client3 = PeerFactory()
		factory2 = PeerFactory()
		port1 = int(porta) - 1
		port2 = int(porta) - 2
		print "Connecting to host " + address + " port " + str(port1)
		reactor.connectTCP(address, port1, client3)
		print "Connecting to host " + address + " port " + str(port2)
		reactor.connectTCP(address, port2, factory2)

	reactor.run()
