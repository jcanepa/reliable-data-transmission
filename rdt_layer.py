from typing import Optional
from segment import Segment
from unreliable import UnreliableChannel

class RDTLayer(object):
    """
    The reliable data transfer (RDT) layer is used as a
    communication layer to resolve issues over an unreliable channel.
    """

    DATA_LENGTH = 4 # in characters                     # length of string data sent per packet
    FLOW_CONTROL_WIN_SIZE = 15 # in characters          # receive window size for flow-control

    sendChannel: Optional[UnreliableChannel]            # the channel to send data through
    receiveChannel: Optional[UnreliableChannel]         # the channel to receive data through
    dataToSend: str                                     # the data to send
    currentIteration: int                               # used for segment 'timeouts'
    # added by @jcanepa
    currentTimeouts: int                                # current segment timeout iteration
    sentData: int                                       # number of characters sent
    seqCount: int                                       # tracks current sequence number
    ackCount: int                                       # tracks current acknowledgement number
    cumulativeAck: int                                  # used for cumulative ack
    flowCheck: int                                      # ensures pipeline segments fit within flow-control window
    packetNum: int                                      # track the current packet number in the pipeline
    isServer: bool                                      # differentiate between client and server instances
    transmittedPackets: list                            # packets successfully received by the server
    transmittedSeqNums: list                            # sequence numbers successfully received by the server

    def __init__(self):
        self.sendChannel = None
        self.receiveChannel = None
        self.dataToSend = ''
        self.currentIteration = 0
        # added by @jcanepa
        self.currentTimeouts = 0
        self.sentData = 0
        self.seqCount = 1
        self.ackCount = 1
        self.cumulativeAck = 1
        self.flowCheck = 0
        self.packetNum = 0
        self.isServer = False
        self.transmittedPackets = []
        self.transmittedSeqNums = []

    def setSendChannel(self, channel):
        """
        Called by main to set the unreliable sending lower-layer channel
        """
        self.sendChannel = channel

    def setReceiveChannel(self, channel):
        """
        Called by main to set the unreliable receiving lower-layer channel
        """
        self.receiveChannel = channel

    def setDataToSend(self, data):
        """
        Called by main to set the string data to send
        """
        self.dataToSend = data

    def getDataReceived(self):
        """
        Called by main to get the currently received and buffered string data, in order
        """
        return self.dataToSend

    def processData(self):
        """
        "timeslice" called by main once per iteration
        """
        self.currentIteration += 1
        self.processSend()
        self.processReceiveAndSendRespond()

    def processSend(self):
        """
        Manages Segment sending tasks
        """
        # PROVIDED CODE START
        # segmentSend = Segment()

        # ############################################################################################################ #
        # print('processSend(): Complete this...')

        # You should pipeline segments to fit the flow-control window
        # The flow-control window is the constant RDTLayer.FLOW_CONTROL_WIN_SIZE
        # The maximum data that you can send in a segment is RDTLayer.DATA_LENGTH
        # These constants are given in # characters

        # Somewhere in here you will be creating data segments to send.
        # The data is just part of the entire string that you are trying to send.
        # The seqnum is the sequence number for the segment (in character number, not bytes)

        # seqnum = "0"
        # data = "x"

        # ############################################################################################################ #
        # Display sending segment
        # segmentSend.setData(seqnum,data)
        # print("Sending segment: ", segmentSend.to_string())

        # Use the unreliable sendChannel to send the segment
        # self.sendChannel.send(segmentSend)
        # PROVIDED CODE END

        # server mode
        if self.dataToSend == '':
            self.isServer = True
            return

        # client mode
        if not self.isServer:
            # flow control ensures that only 15 characters of data are sent in a pipeline
            while (self.flowCheck < self.FLOW_CONTROL_WIN_SIZE):

                if (self.currentTimeouts > 0):
                    # timeout handling
                    self._retransmitSegment()

                elif (self.sentData < len(self.dataToSend)):
                    self._sendNewSegment()

                else:
                    # nothing to send, close flow-control window
                    self.flowCheck = self.FLOW_CONTROL_WIN_SIZE

            # reset flow-control checker
            self.flowCheck = 0

    def processReceiveAndSendRespond(self):
        """
        Manages Segment receive tasks
        """
        # segmentAck = Segment()                  # Segment acknowledging packet(s) received

        # This call returns a list of incoming segments (see Segment class)...
        # listIncomingSegments = self.receiveChannel.receive()

        # ############################################################################################################ #
        # What segments have been received?
        # How will you get them back in order?
        # This is where a majority of your logic will be implemented
        # print('processReceive(): Complete this...')

        # ############################################################################################################ #
        # How do you respond to what you have received?
        # How can you tell data segments apart from ack segemnts?
        # print('processReceive(): Complete this...')

        # Somewhere in here you will be setting the contents of the ack segments to send.
        # The goal is to employ cumulative ack, just like TCP does...
        # acknum = "0"

        # ############################################################################################################ #
        # Display response segment
        # segmentAck.setAck(acknum)
        # print("Sending ack: ", segmentAck.to_string())

        # Use the unreliable sendChannel to send the ack packet
        # self.sendChannel.send(segmentAck)

        acknum = -1

        listIncomingSegments = self.receiveChannel.receive()

        #  server mode
        if self.isServer:
            listIncomingSegments.sort(key=lambda x: x.seqnum)

            # segments received by the server that have not been corrupted
            uncorruptedSegs = []

            for i in listIncomingSegments:
                # data that contains an 'X' is corrupted and should be discarded
                if 'X' not in i.payload:
                    if i not in self.transmittedPackets:
                        self.transmittedPackets.append(i)
                    uncorruptedSegs.append(i)

            listIncomingSegments.clear()

            # list of received segments with uncorrupted segments
            for i in uncorruptedSegs:
                listIncomingSegments.append(i)

            # sequence numbers successfully received by the server
            # prevents duplicates from being added to uncorrupted packets
            cachedSeqNums = []
            uncorruptedPackets = []

            # sort segments based on sequence number
            self.transmittedPackets.sort(key=lambda x: x.seqnum)

            # transfer only unique packets to list of uncorrupted packets
            for i in self.transmittedPackets:
                if (i.seqnum not in cachedSeqNums):
                    cachedSeqNums.append(i.seqnum)
                    uncorruptedPackets.append(i.payload)

            # empty current data to send
            self.dataToSend = ""

            # finalize data to send
            for i in uncorruptedPackets:
                self.dataToSend += i

            for i in listIncomingSegments:
                # segment acknowledging packet(s) received
                segmentAck = Segment()

                acknum = self.ackCount

                # expected segment, store its sequence number and increment ack number accordingly
                if (i.seqnum == acknum):
                    self.transmittedSeqNums.append(i.seqnum)
                    self.ackCount += len(i.payload)
                    acknum = self.ackCount

                # unexpected segment, start timeout timer
                else:
                    segmentAck.startIteration = 1

                # next expected segment has already been received, set ack to next expected (unreceived) segment
                if (acknum in self.transmittedSeqNums):
                    x = 1
                    uncachedSeq = 0
                    self.transmittedSeqNums.sort()
                    while (x != self.transmittedSeqNums[-1]):

                        x += self.DATA_LENGTH

                        if (x not in self.transmittedSeqNums):
                            uncachedSeq = 1
                            break

                        x += self.DATA_LENGTH

                        if (x not in self.transmittedSeqNums):
                            uncachedSeq = 1
                            break

                        x += self.DATA_LENGTH

                        if (x not in self.transmittedSeqNums):
                            uncachedSeq = 1
                            break

                        x += self.DATA_LENGTH - 1

                        if (x not in self.transmittedSeqNums):
                            uncachedSeq = 1
                            break

                    # set ack to cumulative ack if all previous data have been received
                    if (uncachedSeq == 0):
                        self.ackCount = self.cumulativeAck
                        acknum = self.cumulativeAck

                    else:
                        self.ackCount = x
                        acknum = x

                # increment cumulative ack
                if (i.seqnum >= self.cumulativeAck):
                    if (i.seqnum not in self.transmittedSeqNums):
                        self.transmittedSeqNums.append(i.seqnum)
                    self.cumulativeAck = i.seqnum + len(i.payload)

                # display response segment
                segmentAck.setAck(acknum)
                print("Sending ack: ", segmentAck.to_string())

                # use the unreliable send channel to transmit the ack packet
                self.sendChannel.send(segmentAck)

            # ensure that client knows current ack number even if client stops sending segments
            if not listIncomingSegments:
                segmentAck = Segment()

                segmentAck.startIteration = 1
                acknum = self.ackCount

                # display response segment
                segmentAck.setAck(acknum)
                print("Sending ack: ", segmentAck.to_string())

                # Use the unreliable send channel to transmit the ack packet
                self.sendChannel.send(segmentAck)

        # client mode
        else:
            # sort received packets from server
            listIncomingSegments.sort(
                key=lambda x: x.startIteration)

            # process received packets and find out current ack number and if a segment needs to be resent
            for i in listIncomingSegments:
                self.ackCount = i.acknum
                self.currentTimeouts += i.startIteration

                if (self.ackCount < len(self.dataToSend) and self.sentData == len(self.dataToSend)):
                    self.currentTimeouts += 1

    def _retransmitSegment(self):
        """
        Handles timeouts for segments that
        should be selectively retransmitted
        """
        data = ""
        x = 1
        # checks whether to send a full packet of 4 characters or just 3
        isComplete = False
        seqnum = self.ackCount

        # Use the seqnum to decide whether to send complete packets or not
        while (x < len(self.dataToSend) + 1):
            if (x == seqnum):
                isComplete = True
                break

            x += self.DATA_LENGTH
            if (x == seqnum):
                isComplete = True
                break

            x += self.DATA_LENGTH
            if (x == seqnum):
                isComplete = True
                break

            x += self.DATA_LENGTH
            if (x == seqnum):
                break

            x += self.DATA_LENGTH - 1

        if isComplete:
            lowerBound = seqnum - 1
            upperBound = seqnum + 3

            # increment flow-control checker
            self.flowCheck += 4

        else:
            lowerBound = seqnum - 1
            upperBound = seqnum + 2

            # increment flow-control checker
            self.flowCheck += 3

        # ensure that the string index will not be out of range
        while (upperBound > len(self.dataToSend)):
            upperBound -= 1

        # take 3 or 4 chars to send
        for i in range(lowerBound, upperBound):
            data += self.dataToSend[i]

        # reset timeout timer
        self.currentTimeouts = 0

        # display sending segment
        segmentSend = Segment()

        segmentSend.setData(seqnum, data)
        print("Retransmitting segment: ", segmentSend.to_string())

        # use the unreliable sendChannel to send the segment
        self.sendChannel.send(segmentSend)

    def _sendNewSegment(self):
        seqnum = self.seqCount
        lowerBound = self.sentData

        self.packetNum += 1

        # send 3 characters of data for every 4th new packet
        if (self.packetNum == 4):
            self.seqCount += self.DATA_LENGTH - 1
            upperBound = self.sentData + self.DATA_LENGTH - 1
            self.packetNum = 0

        # send the complete 4 characters
        else:
            self.seqCount += self.DATA_LENGTH
            upperBound = self.sentData + self.DATA_LENGTH

        # prevent index errors
        while (upperBound > len(self.dataToSend)):
            upperBound -= 1

        # 3 or 4 characters to be sent
        data = ""

        # Number of characters sent
        sentChars = 0

        for i in range(lowerBound, upperBound):
            data += self.dataToSend[i]
            sentChars += 1

        # increment total data sent with the amount that was just sent
        self.sentData += sentChars

        # increment flow-control checker
        self.flowCheck += sentChars

        # display sending segment
        segmentSend = Segment()
        segmentSend.setData(seqnum,data)
        print("Sending segment: ", segmentSend.to_string())

        # use unreliable send channel to transmit segment
        self.sendChannel.send(segmentSend)

    @property
    def countSegmentTimeouts(self):
        """
        used by main after message is reassembled,
        identical to an already provided class variable
        """
        return self.currentTimeouts