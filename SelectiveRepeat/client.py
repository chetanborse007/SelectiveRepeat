#!/usr/bin/python
"""""
@File:           client.py
@Description:    This is a sender running Selective Repeat protocol
                 for reliable data transfer.
@Author:         Chetan Borse
@EMail:          chetanborse2106@gmail.com
@Created_on:     03/23/2017
@License         GNU General Public License
@python_version: 2.7
===============================================================================
"""

import os
import time
import math
import logging
import random
import socket
import struct
import select
import hashlib
from collections import namedtuple
from collections import OrderedDict
from threading import Thread
from threading import Lock
from Carbon.Aliases import true


# Set logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s SENDER [%(levelname)s] %(message)s',)
log = logging.getLogger()


# Lock for synchronized access to 'Window' class
LOCK = Lock()


class SocketError(Exception):
    pass


class FileNotExistError(Exception):
    pass


class WindowSizeError(Exception):
    pass


class Sender(object):
    """
    Sender running Selective Repeat protocol for reliable data transfer.
    """

    def __init__(self,
                 senderIP="127.0.0.1",
                 senderPort=8081,
                 sequenceNumberBits=2,
                 windowSize=None,
                 maxSegmentSize=1500,
                 www=os.path.join(os.getcwd(), "data", "sender")):
        self.senderIP = senderIP
        self.senderPort = senderPort
        self.sequenceNumberBits = sequenceNumberBits
        self.windowSize = windowSize
        self.maxSegmentSize = maxSegmentSize
        self.www = www

    def open(self):
        """
        Create UDP socket for communication with the server.
        """
        log.info("Creating UDP socket %s:%d for communication with the server",
                 self.senderIP, self.senderPort)

        try:
            self.senderSocket = socket.socket(socket.AF_INET,
                                              socket.SOCK_DGRAM)
            self.senderSocket.bind((self.senderIP, self.senderPort))
            self.senderSocket.setblocking(0)
        except Exception as e:
            log.error("Could not create UDP socket for communication with the server!")
            log.debug(e)
            raise SocketError("Creating UDP socket %s:%d for communication with the server failed!"
                              % (self.senderIP, self.senderPort))

    def send(self,
             filename,
             receiverIP="127.0.0.1",
             receiverPort=8080,
             totalPackets="ALL",
             timeout=10):
        """
        Transmit specified file to the receiver.
        """
        log.info("Transmitting file '%s' to the receiver", filename)
        filename = os.path.join(self.www, filename)

        # If file does not exist, terminate the program
        if not os.path.exists(filename):
            raise FileNotExistError("File does not exist!\nFilename: %s"
                                    % filename)

        # Create an object of 'Window', which handles packet transmission
        window = Window(self.sequenceNumberBits,
                        self.windowSize)

        # Create a thread named 'PacketHandler' to monitor packet transmission
        log.info("Creating a thread to monitor packet transmission")
        packetHandler = PacketHandler(filename,
                                      self.senderSocket,
                                      self.senderIP,
                                      self.senderPort,
                                      receiverIP,
                                      receiverPort,
                                      window,
                                      self.maxSegmentSize,
                                      totalPackets,
                                      timeout)

        # Create a thread named 'ACKHandler' to monitor acknowledgement receipt
        log.info("Creating a thread to monitor acknowledgement receipt")
        ackHandler = ACKHandler(self.senderSocket,
                                self.senderIP,
                                self.senderPort,
                                receiverIP,
                                receiverPort,
                                window)

        # Start thread execution
        log.info("Starting thread execution")
        packetHandler.start()
        ackHandler.start()

        # Wait for threads to finish their execution
        packetHandler.join()
        ackHandler.join()

    def close(self):
        """
        Close UDP socket.
        """
        try:
            if self.senderSocket:
                self.senderSocket.close()
        except Exception as e:
            log.error("Could not close UDP socket!")
            log.debug(e)
            raise SocketError("Closing UDP socket %s:%d failed!"
                              % (self.senderIP, self.senderPort))


class Window(object):
    """
    Class for assisting packet transmission.
    """

    def __init__(self, sequenceNumberBits, windowSize=None):
        self.expectedAck = 0
        self.nextSequenceNumber = 0
        self.nextPkt = 0
        self.maxSequenceSpace = int(math.pow(2, sequenceNumberBits))
        if windowSize is None:
            self.maxWindowSize = int(math.pow(2, sequenceNumberBits-1))
        else:
            if windowSize > int(math.pow(2, sequenceNumberBits-1)):
                raise WindowSizeError("Invalid window size!!")
            else:
                self.maxWindowSize = windowSize
        self.transmissionWindow = OrderedDict()
        self.isPacketTransmission = True

    def expectedACK(self):
        return self.expectedAck

    def maxSequenceNumber(self):
        return self.maxSequenceSpace

    def empty(self):
        if len(self.transmissionWindow) == 0:
            return True
        return False

    def full(self):
        if len(self.transmissionWindow) >= self.maxWindowSize:
            return True
        return False

    def exist(self, key):
        if key in self.transmissionWindow:
            return True
        return False

    def next(self):
        return self.nextPkt

    def consume(self, key):
        with LOCK:
            self.transmissionWindow[key] = [None, False]

            self.nextSequenceNumber += 1
            if self.nextSequenceNumber >= self.maxSequenceSpace:
                self.nextSequenceNumber %= self.maxSequenceSpace

            self.nextPkt += 1

    def start(self, key):
        with LOCK:
            self.transmissionWindow[key][0] = time.time()

    def restart(self, key):
        with LOCK:
            self.transmissionWindow[key][0] = time.time()

    def stop(self, key):
        if self.exist(key):
            self.transmissionWindow[key][0] = None

        if key == self.expectedAck:
            for k, v in self.transmissionWindow.items():
                if v[0] == None and v[1] == True:
                    del self.transmissionWindow[k]
                else:
                    break

            if len(self.transmissionWindow) == 0:
                self.expectedAck = self.nextSequenceNumber
            else:
                self.expectedAck = self.transmissionWindow.items()[0][0]

    def start_time(self, key):
        return self.transmissionWindow[key][0]

    def unacked(self, key):
        if (self.exist(key) and self.transmissionWindow[key][1] == False):
            return True
        return False

    def mark_acked(self, key):
        with LOCK:
            self.transmissionWindow[key][1] = True

    def stop_transmission(self):
        with LOCK:
            self.isPacketTransmission = False

    def transmit(self):
        return self.isPacketTransmission


class PacketHandler(Thread):
    """
    Thread for monitoring packet transmission.
    """

    HEADER_LENGTH = 6
    PACKET = namedtuple("Packet", ["SequenceNumber", "Checksum", "Data"])

    def __init__(self,
                 filename,
                 senderSocket,
                 senderIP,
                 senderPort,
                 receiverIP,
                 receiverPort,
                 window,
                 maxSegmentSize=1500,
                 totalPackets="ALL",
                 timeout=10,
                 bitErrorProbability=0.1,
                 threadName="PacketHandler",
                 bufferSize=2048):
        Thread.__init__(self)
        self.filename = filename
        self.senderSocket = senderSocket
        self.senderIP = senderIP
        self.senderPort = senderPort
        self.receiverIP = receiverIP
        self.receiverPort = receiverPort
        self.window = window
        self.maxSegmentSize = maxSegmentSize
        self.maxPayloadSize = maxSegmentSize - PacketHandler.HEADER_LENGTH
        self.totalPackets = totalPackets
        self.timeout = timeout
        self.bitErrorProbability = bitErrorProbability
        self.threadName = threadName
        self.bufferSize = bufferSize

    def run(self):
        """
        Start packet transmission.
        """
        # Get data from Application Layer and
        # create packets for reliable transmission
        log.info("[%s] Generating packets", self.threadName)
        packets = self.generate_packets()

        # Monitor sender
        # untill all packets are successfully transmitted and acked
        log.info("[%s] Starting packet transmission", self.threadName)
        while (not self.window.empty() or
                self.window.next() < self.totalPackets):
            # If window is full, then don't transmit a new packet
            if self.window.full():
                pass
            # If window is not full, but all packets are already transmitted;
            # then stop packet transmission
            elif (not self.window.full() and
                    self.window.next() >= self.totalPackets):
                pass
            # Transmit a new packet using underlying UDP protocol
            else:
                # Receive packet from Application Layer
                packet = packets[self.window.next()]

                # Slide transmission window by 1
                self.window.consume(packet.SequenceNumber)

                # Create a thread named 'SinglePacket'
                # to monitor transmission of single packet
                threadName = "Packet(" + str(packet.SequenceNumber) + ")"
                singlePacket = SinglePacket(self.senderSocket,
                                            self.receiverIP,
                                            self.receiverPort,
                                            self.window,
                                            packet,
                                            self.timeout,
                                            self.bitErrorProbability,
                                            threadName=threadName)

                # Start thread execution
                singlePacket.start()

        # Stop packet transmission
        log.info("[%s] Stopping packet transmission", self.threadName)
        self.window.stop_transmission()

    def generate_packets(self):
        """
        Generate packets for transmitting to receiver.
        """
        packets = []

        with open(self.filename, "rb") as f:
            i = 0

            while True:
                # Read data such that
                # size of data chunk should not exceed maximum payload size
                data = f.read(self.maxPayloadSize)

                # If not data, finish reading
                if not data:
                    break

                # Set sequence number for a packet to be transmitted
                sequenceNumber = i % self.window.maxSequenceNumber()

                # Create a packet with required header fields and payload
                pkt = PacketHandler.PACKET(SequenceNumber=sequenceNumber,
                                           Checksum=self.checksum(data),
                                           Data=data)

                packets.append(pkt)

                i += 1

        # If total packets to be transmitted is not specified by user,
        # then transmit all available packets to receiver
        if self.totalPackets == "ALL":
            self.totalPackets = len(packets)
        else:
            if int(self.totalPackets) <= len(packets):
                self.totalPackets = int(self.totalPackets)
            else:
                self.totalPackets = len(packets)

        return packets[:self.totalPackets]

    def checksum(self, data):
        """
        Compute and return a checksum of the given payload data.
        """
        # Force payload data into 16 bit chunks
        if (len(data) % 2) != 0:
            data += "0"

        sum = 0
        for i in range(0, len(data), 2):
            data16 = ord(data[i]) + (ord(data[i+1]) << 8)
            sum = self.carry_around_add(sum, data16)

        return ~sum & 0xffff

    def carry_around_add(self, sum, data16):
        """
        Helper function for carry around add.
        """
        sum = sum + data16
        return (sum & 0xffff) + (sum >> 16)


class SinglePacket(Thread):
    """
    Thread for monitoring transmission of single packet.
    """

    def __init__(self,
                 senderSocket,
                 receiverIP,
                 receiverPort,
                 window,
                 packet,
                 timeout=10,
                 bitErrorProbability=0.1,
                 threadName="Packet(?)"):
        Thread.__init__(self)
        self.senderSocket = senderSocket
        self.receiverIP = receiverIP
        self.receiverPort = receiverPort
        self.window = window
        self.packet = packet
        self.timeout = timeout
        self.bitErrorProbability = bitErrorProbability
        self.threadName = threadName

    def run(self):
        """
        Start monitoring transmission of single packet.
        """
        # Transmit a packet using underlying UDP protocol and
        # start the corresponding timer.
        log.info("[%s] Transmitting a packet with sequence number: %d",
                 self.threadName, self.packet.SequenceNumber)
        self.rdt_send(self.packet)
        self.window.start(self.packet.SequenceNumber)

        # Monitor packet transmission, until it is acked
        while self.window.unacked(self.packet.SequenceNumber):
            timeLapsed = (time.time() -
                          self.window.start_time(self.packet.SequenceNumber))

            # Retransmit packet, if its transmission times out.
            # Also, restart the corresponding timer.
            if timeLapsed > self.timeout:
                log.info("[%s] Retransmitting a packet with sequence number: %d",
                         self.threadName, self.packet.SequenceNumber)
                self.rdt_send(self.packet)
                self.window.restart(self.packet.SequenceNumber)

        # Stop monitoring packet transmission, if it is successfully acked
        with LOCK:
            self.window.stop(self.packet.SequenceNumber)

    def rdt_send(self, packet):
        """
        Reliable data transfer.
        """
        # Simulate artificial bit error
        if self.simulate_bit_error():
            log.error("[%s] Simulating artificial bit error!!",
                      self.threadName)
            log.error("[%s] Injected bit error into a packet with sequence number: %d",
                      self.threadName, packet.SequenceNumber)
            packet = self.alter_bits(packet)

        # Create a raw packet
        rawPacket = self.make_pkt(packet)

        # Transmit a packet using UDP protocol
        self.udt_send(rawPacket)

    def simulate_bit_error(self):
        """
        Simulate artificial bit error.
        """
        r = random.random()

        if r <= self.bitErrorProbability:
            return True
        else:
            return False

    def alter_bits(self, packet, alterations=5):
        """
        Alter bits in packet.
        """
        # Randomly generate error string of one byte length
        error = random.getrandbits(8)

        # Randomly pick any byte from payload data and alter its bits
        alteredData = list(packet.Data)
        for i in range(alterations):
            randomByte = random.randint(0, len(alteredData))
            alteredByte = ord(alteredData[randomByte]) & error
            alteredData[randomByte] = struct.pack("B", alteredByte)
        alteredData = "".join(alteredData)

        # Create a packet with altered payload data
        alteredPacket = PacketHandler.PACKET(SequenceNumber=packet.SequenceNumber,
                                             Checksum=packet.Checksum,
                                             Data=alteredData)

        return alteredPacket

    def make_pkt(self, packet):
        """
        Create a raw packet.
        """
        sequenceNumber = struct.pack('=I', packet.SequenceNumber)
        checksum = struct.pack('=H', packet.Checksum)
        rawPacket = sequenceNumber + checksum + packet.Data
        return rawPacket

    def udt_send(self, packet):
        """
        Unreliable data transfer using UDP protocol.
        """
        try:
            with LOCK:
                self.senderSocket.sendto(packet,
                                         (self.receiverIP, self.receiverPort))
        except Exception as e:
            log.error("[%s] Could not send UDP packet!", self.threadName)
            log.debug(e)
            raise SocketError("Sending UDP packet to %s:%d failed!"
                              % (self.receiverIP, self.receiverPort))


class ACKHandler(Thread):
    """
    Thread for monitoring acknowledgement receipt.
    """

    ACK = namedtuple("ACK", ["AckNumber", "Checksum"])

    def __init__(self,
                 senderSocket,
                 senderIP,
                 senderPort,
                 receiverIP,
                 receiverPort,
                 window,
                 timeout=10,
                 ackLossProbability=0.05,
                 threadName="ACKHandler",
                 bufferSize=2048):
        Thread.__init__(self)
        self.senderSocket = senderSocket
        self.senderIP = senderIP
        self.senderPort = senderPort
        self.receiverIP = receiverIP
        self.receiverPort = receiverPort
        self.window = window
        self.timeout = timeout
        self.ackLossProbability = ackLossProbability
        self.threadName = threadName
        self.bufferSize = bufferSize

    def run(self):
        """
        Start monitoring acknowledgement receipt.
        """
        # Monitor sender
        # untill all packets are successfully transmitted and acked
        while self.window.transmit():
            # Wait, if all the packets transmitted are acked and
            # there are packets that are yet to be transmitted
            if self.window.empty():
                continue

            # Listen for incoming acknowledgements on sender's socket
            # with the provided timeout
            ready = select.select([self.senderSocket], [], [], self.timeout)

            if not ready[0]:
                continue

            # Receive acknowledgement
            try:
                receivedAck, receiverAddress = self.senderSocket.recvfrom(self.bufferSize)
            except Exception as e:
                log.error("[%s] Could not receive UDP packet!",
                          self.threadName)
                log.debug(e)
                raise SocketError("Receiving UDP packet failed!")

            # Verify whether the acknowledgement is received from correct receiver
            if receiverAddress[0] != self.receiverIP:
                continue

            # Parse header fields from the received acknowledgement
            receivedAck = self.parse(receivedAck)

            # Check whether the received acknowledgement is not corrupt
            if self.corrupt(receivedAck):
                log.warning("[%s] Received corrupt acknowledgement!!",
                            self.threadName)
                log.warning("[%s] Discarding acknowledgement with ack number: %d",
                            self.threadName, receivedAck.AckNumber)
                continue

            # If the received acknowledgement has acknowledgement number
            # beyond expected range, then discard the received acknowledgement
            if not self.window.exist(receivedAck.AckNumber):
                log.warning("[%s] Received acknowledgement outside transmission window!!",
                            self.threadName)
                log.warning("[%s] Discarding acknowledgement with ack number: %d",
                            self.threadName, receivedAck.AckNumber)
                continue

            # Simulate artificial acknowledgement loss
            if self.simulate_ack_loss():
                log.error("[%s] Simulating artificial acknowledgement loss!!",
                          self.threadName)
                log.error("[%s] Lost a acknowledgement with ack number: %d",
                          self.threadName, receivedAck.AckNumber)
                continue

            # Mark transmitted packet as acked
            # for the corresponding received acknowledgement
            log.info("[%s] Received acknowledgement with ack number: %d",
                     self.threadName, receivedAck.AckNumber)
            self.window.mark_acked(receivedAck.AckNumber)

    def parse(self, receivedAck):
        """
        Parse header fields from the received acknowledgement.
        """
        ackNumber = struct.unpack('=I', receivedAck[0:4])[0]
        checksum = struct.unpack('=16s', receivedAck[4:])[0]

        ack = ACKHandler.ACK(AckNumber=ackNumber,
                             Checksum=checksum)

        return ack

    def corrupt(self, receivedAck):
        """
        Check whether the received acknowledgement is corrupt or not.
        """
        # Compute hash code for the received acknowledgement
        hashcode = hashlib.md5()
        hashcode.update(str(receivedAck.AckNumber))

        # Compare computed hash code
        # with the checksum of received acknowledgement
        if hashcode.digest() != receivedAck.Checksum:
            return True
        else:
            return False

    def simulate_ack_loss(self):
        """
        Simulate artificial acknowledgement loss.
        """
        r = random.random()

        if r <= self.ackLossProbability:
            return True
        else:
            return False
