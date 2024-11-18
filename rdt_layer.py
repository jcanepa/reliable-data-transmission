from segment import Segment
from unreliable import UnreliableChannel
from typing import Optional

class RDTLayer(object):
    """
    The reliable data transfer (RDT) layer is used as a communication layer to resolve issues over an unreliable
    channel.
    """

    DATA_LENGTH = 4 # in characters                     # length of string data sent per packet
    FLOW_CONTROL_WIN_SIZE = 15 # in characters          # receive window size for flow-control

    sendChannel: Optional[UnreliableChannel]            # the channel to send data through
    receiveChannel: Optional[UnreliableChannel]         # the channel to receive data through
    dataToSend: str                                     # the data to send
    currentIteration: int                               # used for segment 'timeouts'

    # Add items as needed

    def __init__(self):
        self.sendChannel = None
        self.receiveChannel = None
        self.dataToSend = ''
        self.currentIteration = 0
        # Add items as needed

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
        # Identify the data that has been received...
        print('getDataReceived():' + self.dataToSend)
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
        segmentSend = Segment()

        # ############################################################################################################ #
        print('processSend(): Complete this...')

        # You should pipeline segments to fit the flow-control window
        # The flow-control window is the constant RDTLayer.FLOW_CONTROL_WIN_SIZE
        # The maximum data that you can send in a segment is RDTLayer.DATA_LENGTH
        # These constants are given in # characters

        # Somewhere in here you will be creating data segments to send.
        # The data is just part of the entire string that you are trying to send.
        # The seqnum is the sequence number for the segment (in character number, not bytes)


        seqnum = "0"
        data = "x"


        # ############################################################################################################ #
        # Display sending segment
        segmentSend.setData(seqnum,data)
        print("Sending segment: ", segmentSend.to_string())

        # Use the unreliable sendChannel to send the segment
        self.sendChannel.send(segmentSend)

    def processReceiveAndSendRespond(self):
        """
        Manages Segment receive tasks
        """
        segmentAck = Segment()                  # Segment acknowledging packet(s) received

        # This call returns a list of incoming segments (see Segment class)...
        listIncomingSegments = self.receiveChannel.receive()

        # ############################################################################################################ #
        # What segments have been received?
        # How will you get them back in order?
        # This is where a majority of your logic will be implemented
        print('processReceive(): Complete this...')






        # ############################################################################################################ #
        # How do you respond to what you have received?
        # How can you tell data segments apart from ack segemnts?
        print('processReceive(): Complete this...')

        # Somewhere in here you will be setting the contents of the ack segments to send.
        # The goal is to employ cumulative ack, just like TCP does...
        acknum = "0"


        # ############################################################################################################ #
        # Display response segment
        segmentAck.setAck(acknum)
        print("Sending ack: ", segmentAck.to_string())

        # Use the unreliable sendChannel to send the ack packet
        self.sendChannel.send(segmentAck)
