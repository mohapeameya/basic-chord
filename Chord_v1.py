#!/usr/bin/python

# Basic CHORD
# Bare bones CHORD protocol implementation
# Copyright (C) March 2019 Ameya Kisan Mohape
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
# See 'LICENSE' for more information.


import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
from random import randint
import socket
import hashlib
import threading
import time


def ShowCommands():
    print("Supported commands:")
    print("fing : show finger table")
    print("myid : show ID")
    print("succ : show successor node")
    print("pred : show predecessor node")
    print("cmds : show supported commands")
    print("exit : exit program")


class ChordNode:

    def __init__(self, netFlag, m, r, portNo, fingerTable):
        self.netFlag = netFlag
        self.m = m
        self.r = r
        self.portNo = portNo
        self.fingerTable = fingerTable
        self.successor = {"NodeID": -1, "NodeSocketAddress": ""}
        self.predecessor = {"NodeID": -1, "NodeSocketAddress": ""}
        self.myIP = self.MyIP()
        self.mySocketAddress = self.myIP + ":" + self.portNo

        #   myID is an integer
        self.myID = self.HexToDecimal(self.GenerateHash(self.mySocketAddress)) % (2**self.m)

        self.myNode = {"NodeID": self.myID, "NodeSocketAddress": self.mySocketAddress}
        print(self.myNode)
        self.terminate = False
        self.next = 0

    def MyIP(self):
        if self.netFlag == "local":
            hostname = socket.gethostname()
            return str(socket.gethostbyname(str(hostname)))

        elif self.netFlag == "public":
            tempSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                tempSocket.connect(("8.8.8.8", 80))
                myPublicIP = tempSocket.getsockname()[0]
                tempSocket.close()
                return str(myPublicIP)

            except OSError:
                print("\nCannot find public IP. Exiting...")
                exit(-1)
        else:
            pass

    def GenerateHash(self, text):
        return str(hashlib.sha256(text.encode()).hexdigest())

    def HexToDecimal(self, hex):
        return int(str(hex), 16)

    # rpc
    def FindSuccessor(self, ID):
        if self.BelongsTo(ID, ((self.myID + 1) % (2**m)), self.successor["NodeID"]):
            return self.successor
        else:
            node = self.FindClosestPrecedingNode(ID)

            if self.myNode == node:
                return self.myNode

            with xmlrpc.client.ServerProxy("http://"+node["NodeSocketAddress"]) as proxy:
                successor = proxy.FindSuccessor(ID)
            return successor

    def BelongsTo(self, IDToTest, ID1, ID2):
        if IDToTest < 0:
            return False
        else:
            if ID1 <= ID2:
                if ID1 <= IDToTest <= ID2:
                    return True
                else:
                    return False
            elif ID1 > ID2:
                if ID1 <= IDToTest < 2**self.m or IDToTest <= ID2:
                    return True
                else:
                    return False
            else:
                print("ID1 and ID2 are not comparable")
                return False

    def FindClosestPrecedingNode(self, ID):
        for i in reversed(range(0, self.m)):
            if self.BelongsTo(self.fingerTable[i]["NodeID"], ((self.myID+1) % (2**m)), ((ID-1) % (2**m))):
                return self.fingerTable[i]
        return self.myNode

    # rpc
    def Notify(self, node):
        if self.predecessor["NodeID"] == -1 or self.BelongsTo(node["NodeID"], ((self.predecessor["NodeID"]+1) % (2**m)),((self.myID-1) % (2**m))):
            self.predecessor = node
        return True

    def CreateRing(self):
        self.predecessor = {"NodeID": -1, "NodeSocketAddress": ""}
        self.successor = self.myNode
        pass

    def JoinRing(self, nodeSocketAddress):
        self.predecessor = {"NodeID": -1, "NodeSocketAddress": ""}
        with xmlrpc.client.ServerProxy("http://"+nodeSocketAddress) as proxy:
            successor = proxy.FindSuccessor(self.myID)
        self.successor = successor

    def GivePredecessor(self):
        return self.predecessor

    # thread for stabilization
    def Stabilize(self):
        while True:
            with xmlrpc.client.ServerProxy("http://"+self.successor["NodeSocketAddress"]) as proxy:
                node = proxy.GivePredecessor()
            if self.BelongsTo(node["NodeID"], ((self.myID+1) % (2**m)), ((self.successor["NodeID"]-1) % 2**m)):
                self.successor = node

            with xmlrpc.client.ServerProxy("http://"+self.successor["NodeSocketAddress"]) as proxy:
                status = proxy.Notify(self.myNode)
            if self.terminate:
                break
            time.sleep(2)

    # thread for fixing finger table periodically
    def FixFingerTable(self):
        while True:
            if self.next >= self.m:
                self.next = 0
            self.fingerTable[self.next] = self.FindSuccessor((self.myID + 2**self.next) % 2**self.m)
            self.next = self.next + 1
            if self.terminate:
                break
            time.sleep(1)

    # thread to check periodically if predecessor has failed
    def CheckPredecessor(self):
        while True:
            if self.predecessor["NodeID"] > 0:
                try:
                    with xmlrpc.client.ServerProxy("http://"+self.predecessor["NodeSocketAddress"]) as proxy:
                        node = proxy.GivePredecessor()
                except ConnectionRefusedError:
                    self.predecessor = {"NodeID": -1, "NodeSocketAddress": ""}
            if self.terminate:
                break
            time.sleep(2)

    # thread to activate remote procedure calls
    def RemoteProcedureCalls(self):
        server = SimpleXMLRPCServer((self.myIP, int(self.portNo)), logRequests=False)
        print("listening on port " + self.portNo + "...")

        server.register_function(self.GivePredecessor, 'GivePredecessor')
        server.register_function(self.FindSuccessor, 'FindSuccessor')
        server.register_function(self.Notify, 'Notify')

        server.serve_forever()


print("\nSimple CHORD Protocol implementation")

# m = int(input("Enter value of finger parameter 'm': "))
m = 10  # 2**m nodes possible in the network
print("Number of fingers set to", str(m))

# r = int(input("Enter value of replication parameter 'r': "))
r = 5   # replication parameter

netFlag = ""
networkChoice = str(input("\nSelect CHORD network: \n1. Local\n2. Public\nEnter choice:"))
if networkChoice == "1":
    netFlag = "local"
elif networkChoice == "2":
    netFlag = "public"
else:
    print("Invalid choice!!! Exiting...")
    exit(1)

portNo = ""
portChoice = str(input("\nSelect Port:\n1. Custom port\n2. Random port\nEnter choice: "))
if portChoice == "1":
    portNo = str(input("Enter custom port no: "))
elif portChoice == "2":
    portNo = str(randint(50000, 55000))
else:
    print("Invalid choice!!! Exiting...")
    exit(1)

# finger table as a list of dictionary
fingerTable = []
for i in range(0, m):
    fingerTable.append({"NodeID": -1, "NodeSocketAddress": ""})

MyNode = ChordNode(netFlag, m, r, portNo, fingerTable)

RemoteProcedureCallsThread = threading.Thread(target=MyNode.RemoteProcedureCalls)
# make a daemon rpc server thread
RemoteProcedureCallsThread.daemon = True
RemoteProcedureCallsThread.start()


ringChoice = str(input("\nSelect Operation:\n1. Create Chord ring\n2. Join an existing Chord ring\nEnter choice: "))
if ringChoice == "1":
    # create ring
    MyNode.CreateRing()
elif ringChoice == "2":
    # join known node of an existing ring
    nodeSocketAddress = str(input("Enter socket address of known node(IP:PORT): "))
    MyNode.JoinRing(nodeSocketAddress)
else:
    print("Invalid choice!!! Exiting...")
    exit(1)

StabilizationThread = threading.Thread(target=MyNode.Stabilize)
StabilizationThread.start()

FixFingerTableThread = threading.Thread(target=MyNode.FixFingerTable)
FixFingerTableThread.start()

CheckPredecessorThread = threading.Thread(target=MyNode.CheckPredecessor)
CheckPredecessorThread.start()

ShowCommands()

while True:
    cmd = str(input("Enter command>>>"))
    if cmd == "fing":
        for i in range(0, MyNode.m):
            print(MyNode.fingerTable[i])
    elif cmd == "myid":
        print(MyNode.myNode)
    elif cmd == "succ":
        print(MyNode.successor)
    elif cmd == "pred":
        print(MyNode.predecessor)
    elif cmd == "cmds":
        ShowCommands()
    elif cmd == "exit":
        MyNode.terminate = True

        StabilizationThread.join()
        FixFingerTableThread.join()
        CheckPredecessorThread.join()
        print("Bye!!!")
        break
    else:
        print("Invalid command. Type 'cmds'")
