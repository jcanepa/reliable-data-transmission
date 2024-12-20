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
    isServer: bool                                      # differentiate between client and server instances
    currentTimeouts: int                                # current segment timeout iteration
    sentCount: int                                      # number of characters sent
    currentSeqenceNo: int                               # tracks current sequence number
    currentAck: int                                     # tracks current acknowledgement number
    cumulativeAck: int                                  # used for cumulative ack
    flowIndex: int                                      # ensures pipeline segments fit within flow-control window
    currentPacket: int                                  # track the current packet number in the pipeline
    transmittedPackets: list                            # packets successfully received by the server
    transmittedSeqNums: list                            # sequence numbers successfully received by the server

    def __init__(self):
        self.sendChannel = None
        self.receiveChannel = None
        self.dataToSend = ''
        self.currentIteration = 0
        # added by @jcanepa
        self.currentTimeouts = 0
        self.sentCount = 0
        self.currentSeqenceNo = 1
        self.currentAck = 1
        self.cumulativeAck = 1
        self.flowIndex = 0
        self.currentPacket = 0
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
        if self.dataToSend == '':
            # enter "server mode"
            self.isServer = True
            return

        # data to send in "client mode"
        if not self.isServer:
            # flow control ensures that only 15 characters of data are sent in a pipeline
            while (self.flowIndex < self.FLOW_CONTROL_WIN_SIZE):

                if (self.currentTimeouts > 0):
                    # handle timeouts
                    self._retransmitSegment()

                elif (self.sentCount < len(self.dataToSend)):
                    self._sendNewSegment()

                else:
                    # nothing to send, close flow-control window
                    self.flowIndex = self.FLOW_CONTROL_WIN_SIZE

            # reset index
            self.flowIndex = 0

    def processReceiveAndSendRespond(self):
        """
        Manages segment receive tasks

        Reference: https://github.com/SuperSaiyanAsian/RDTLayer/blob/main/rdt_layer.py#L308
        """
        acknum = -1

        listIncomingSegments = self.receiveChannel.receive()

        #  enter "server mode"
        if self.isServer:
            listIncomingSegments.sort(key=lambda x: x.seqnum)

            # segments received by the server that have not been corrupted
            uncorruptedSegs = []

            for i in listIncomingSegments:
                # data containing an 'X' is corrupted and should be discarded
                if 'X' not in i.payload:
                    # previously untransmitted data
                    if i not in self.transmittedPackets:
                        self.transmittedPackets.append(i)
                    uncorruptedSegs.append(i)

            listIncomingSegments.clear()

            # list of received segments with uncorrupted segments
            for i in uncorruptedSegs:
                listIncomingSegments.append(i)

            cachedSeqNums = []
            uncorruptedPackets = []

            # sort segments based on sequence number
            self.transmittedPackets.sort(key=lambda x: x.seqnum)

            # store only unique packets
            for i in self.transmittedPackets:
                if (i.seqnum not in cachedSeqNums):
                    cachedSeqNums.append(i.seqnum)
                    uncorruptedPackets.append(i.payload)

            # reset then load data to send
            self.dataToSend = ""

            for i in uncorruptedPackets:
                self.dataToSend += i

            # reference: https://github.com/SuperSaiyanAsian/RDTLayer/blob/main/rdt_layer.py#L369
            for i in listIncomingSegments:
                # segment acknowledging packet(s) received
                segmentAck = Segment()

                acknum = self.currentAck

                # expected segment, store its sequence number and increment ack number accordingly
                if (i.seqnum == acknum):
                    self.transmittedSeqNums.append(i.seqnum)
                    self.currentAck += len(i.payload)
                    acknum = self.currentAck

                # unexpected segment, start timer
                else:
                    segmentAck.startIteration = 1

                # next expected segment has already been received
                # set ack to next expected (unreceived) segment
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
                        self.currentAck = self.cumulativeAck
                        acknum = self.cumulativeAck

                    else:
                        self.currentAck = x
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
                acknum = self.currentAck

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
                self.currentAck = i.acknum
                self.currentTimeouts += i.startIteration

                if (self.currentAck < len(self.dataToSend) and self.sentCount == len(self.dataToSend)):
                    self.currentTimeouts += 1

    def _calculateBounds(self, seqNum):
        """
        Deturmine if sequence number aligns with the start
        of the packet, calculate the upper & lower bounds accordingly.
        """
        # sequence number corresponds to beginning of a complete 4-character packet
        isComplete = (seqNum - 1) % self.DATA_LENGTH == 0

        # compute bounds based on completeness
        lowerBound = seqNum - 1
        upperBound = lowerBound + (4 if isComplete else 3)

        return isComplete, lowerBound, min(upperBound, len(self.dataToSend))

    def _retransmitSegment(self):
        """
        Handles selective retransmission of timed-out packets
        """
        data = ""
        x = 1

        # checks whether to send a full packet of 4 characters or just 3
        isComplete = False
        seqnum = self.currentAck

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
            self.flowIndex += 4

        else:
            lowerBound = seqnum - 1
            upperBound = seqnum + 2

            # increment flow-control checker
            self.flowIndex += 3

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
        """
        sends new data segments into
        """
        seqnum = self.currentSeqenceNo
        lowerBound = self.sentCount

        self.currentPacket += 1

        # send 3 characters of data for every 4th new packet
        if (self.currentPacket == 4):
            self.currentSeqenceNo += self.DATA_LENGTH - 1
            upperBound = self.sentCount + self.DATA_LENGTH - 1
            self.currentPacket = 0

        # send the complete 4 characters
        else:
            self.currentSeqenceNo += self.DATA_LENGTH
            upperBound = self.sentCount + self.DATA_LENGTH

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
        self.sentCount += sentChars

        # increment flow-control checker
        self.flowIndex += sentChars

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