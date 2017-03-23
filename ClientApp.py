#!/usr/bin/python
"""""
@File:           ClientApp.py
@Description:    Client Application running Selective Repeat protocol
                 for reliable data transfer.
@Author:         Chetan Borse
@EMail:          chetanborse2106@gmail.com
@Created_on:     03/23/2017
@License         GNU General Public License
@python_version: 2.7
===============================================================================
"""

import os
import argparse

from SelectiveRepeat.client import Sender
from SelectiveRepeat.client import SocketError
from SelectiveRepeat.client import FileNotExistError


def ClientApp(**args):
    # Arguments
    filename = args["filename"]
    senderIP = args["sender_ip"]
    senderPort = args["sender_port"]
    receiverIP = args["receiver_ip"]
    receiverPort = args["receiver_port"]
    sequenceNumberBits = args["sequence_number_bits"]
    maxSegmentSize = args["max_segment_size"]
    totalPackets = args["total_packets"]
    timeout = args["timeout"]
    www = args["www"]

    # Create 'Sender' object
    sender = Sender(senderIP,
                    senderPort,
                    sequenceNumberBits,
                    maxSegmentSize,
                    www)

    try:
        # Create sending UDP socket
        sender.open()

        # Send file to receiver
        sender.send(filename,
                    receiverIP,
                    receiverPort,
                    totalPackets,
                    timeout)

        # Close sending UDP socket
        sender.close()
    except SocketError as e:
        print("Unexpected exception in sending UDP socket!!")
        print(e)
    except FileNotExistError as e:
        print("Unexpected exception in file to be sent!!")
        print(e)
    except Exception as e:
        print("Unexpected exception!")
        print(e)
    finally:
        sender.close()


if __name__ == "__main__":
    # Argument parser
    parser = argparse.ArgumentParser(description='Selective Repeat Protocol Client Application',
                                     prog='python \
                                           ClientApp.py \
                                           -f <filename> \
                                           -a <sender_ip> \
                                           -b <sender_port> \
                                           -x <receiver_ip> \
                                           -y <receiver_port> \
                                           -m <sequence_number_bits> \
                                           -s <max_segment_size> \
                                           -n <total_packets> \
                                           -t <timeout> \
                                           -w <www>')

    parser.add_argument("-f", "--filename", type=str, default="index.html",
                        help="File to be sent, default: index.html")
    parser.add_argument("-a", "--sender_ip", type=str, default="127.0.0.1",
                        help="Sender IP, default: 127.0.0.1")
    parser.add_argument("-b", "--sender_port", type=int, default=8081,
                        help="Sender Port, default: 8081")
    parser.add_argument("-x", "--receiver_ip", type=str, default="127.0.0.1",
                        help="Receiver IP, default: 127.0.0.1")
    parser.add_argument("-y", "--receiver_port", type=int, default=8080,
                        help="Receiver Port, default: 8080")
    parser.add_argument("-m", "--sequence_number_bits", type=int, default=2,
                        help="Total number of bits used in sequence numbers, default: 2")
    parser.add_argument("-s", "--max_segment_size", type=int, default=1500,
                        help="Maximum segment size, default: 1500")
    parser.add_argument("-n", "--total_packets", type=str, default="ALL",
                        help="Total packets to be transmitted, default: ALL")
    parser.add_argument("-t", "--timeout", type=int, default=10,
                        help="Timeout, default: 10")
    parser.add_argument("-w", "--www", type=str, default=os.path.join(os.getcwd(), "data", "sender"),
                        help="Source folder for transmission, default: /<Current Working Directory>/data/sender/")

    # Read user inputs
    args = vars(parser.parse_args())

    # Run Client Application
    ClientApp(**args)
