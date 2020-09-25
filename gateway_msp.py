
"""
my main page     #{mainpage}
====================
"""
import struct
import pymysql 
import serial
from datetime import datetime
import time
import sys

serialHandler = serial.Serial(
                      port=sys.argv[1],
                      baudrate=9600,
                      parity=serial.PARITY_NONE,
                      stopbits=serial.STOPBITS_ONE,
                      bytesize=serial.EIGHTBITS,
                      timeout=1
                   )

class Node:
    nodeId = 0
    long_address = ""
    measurability = ""
    nodeTitle = ""
    endpt=""

    def __init__(self,nodeId,long_address,measurability,nodeTitle,endpt):
        self.nodeTitle = nodeTitle
        self.long_address = long_address
        self.measurability = measurability
        self.nodeId = nodeId
        self.endpt = endpt

"""
##This is the title
=================

# Martn said this: 

#Variables: 

"""
class Property:
    nodeId = 0
    propertyId = 0
    propertyName = ""
    propertyXLabel = ""
    propertyYLabel = ""
    propertyXUnit = ""
    propertyYUnit = ""
    propertyFactor = 0.0

    def __init__(self,nodeId,propertyId,propertyName,propertyXLabel,propertyYLabel,propertyXUnit,propertyYUnit,propertyFactor):
        self.nodeId = nodeId
        self.propertyId = propertyId
        self.propertyName = propertyName
        self.propertyXLabel = propertyXLabel
        self.propertyYLabel = propertyYLabel
        self.propertyXUnit = propertyXUnit
        self.propertyYUnit = propertyYUnit
        self.propertyFactor = propertyFactor


def connectToDatabase():
    databaseHandler = pymysql.connect(host='mysql.hrz.tu-chemnitz.de', port=3306, user='gab_database_rw', passwd='HeiL3eeNgah', db='gab_database')
    return databaseHandler


def getAcursor(databaseHandler):
    databaseCursor = databaseHandler.cursor()
    return databaseCursor


def insertIntoNodeTable(node,databaseHandler,databaseCursor):
    try:
        if_Exists = ""
        args = (node.nodeTitle,node.endpt,node.measurability,node.nodeId,if_Exists)
        databaseCursor.callproc("SP_INSERT_INTO_node_table",(args))
        result = databaseCursor.fetchall()
        if_Exists = result[0][0]
        if(if_Exists== 1):
            print("Node already registered, Error code- 0x01 transmitted with Pre-registered node id ",result[0][1])
            sendToDevice(0x01, result[0][1], node.long_address,node.endpt)

        else:
            print('New node registered, Transmitted Nodeid is -',result[0][1])
            sendToDevice(0x00, result[0][1], node.long_address,node.endpt)

        databaseHandler.commit()
    except pymysql.Error() as e:
        databaseHandler.rollback()
        raise

## fuction1
def insertIntoPropertyTable(propertyObj,databaseHandler,databaseCursor):
    try:
        print("Debug: ",propertyObj.nodeId,"|",propertyObj.propertyId,"|", propertyObj.propertyName,"|", propertyObj.propertyXLabel,"|", propertyObj.propertyYLabel,"|", propertyObj.propertyXUnit,"|", propertyObj.propertyYUnit,"|",propertyObj.propertyFactor)
        args = (propertyObj.nodeId, propertyObj.propertyId, propertyObj.propertyName, propertyObj.propertyXLabel, propertyObj.propertyYLabel, propertyObj.propertyXUnit, propertyObj.propertyYUnit,propertyObj.propertyFactor)
        databaseCursor.callproc("SP_INSERT_INTO_property_table",(args))
        databaseHandler.commit()
    except pymysql.Error() as e:
        databaseHandler.rollback()
        raise
"""
## function2 """
def insertIntoValueTable(node_id,property_id,property_val,time,datex,databaseHandler,databaseCursor):
    try:
        args = (node_id,property_id,property_val,time,datex)
        databaseCursor.callproc("SP_INSERT_INTO_value_table",(args))
        databaseHandler.commit()
    except pymysql.Error() as e:
        databaseHandler.rollback()
        raise


def sendToDevice(code,nodeid,address,endpt):
    lowbyte = address and 0xff
    highbyte = (address and 0xff00) >> 8
    newline = '\n'
    transmitbytes = [code,nodeid, lowbyte, highbyte,int(endpt,16)]
    serialHandler.write(transmitbytes)
    serialHandler.write(newline.encode('utf=8'))
    print(transmitbytes)


def getPropertyFromTable(property_Obj,status,Source_Address,endpt,databaseHandler,databaseCursor):
    try:
        exists_in_master = ""
        args = (property_Obj.nodeId, property_Obj.propertyId,property_Obj.propertyName,property_Obj.propertyXLabel,property_Obj.propertyYLabel,property_Obj.propertyXUnit,property_Obj.propertyYUnit,property_Obj.propertyFactor)
        databaseCursor.callproc("SP_SELECT_FROM_property_table",(args))
        result = databaseCursor.fetchone()
        if result != None:
            str_now = datetime.now().strftime('%Y-%m-%d')
            insertIntoValueTable(property_Obj.nodeId,property_Obj.propertyId,status,time.strftime('%H:%M:%S'),str_now,databaseHandler,databaseCursor)

        else:
            args = (property_Obj.nodeId,exists_in_master)
            databaseCursor.callproc("SP_SELECT_FROM_node_table",(args))
            result = databaseCursor.fetchone()
            exists_in_master = result[0]
            if exists_in_master == 1:
                print("Error : No entry in property table.",property_Obj.propertyId,property_Obj.nodeId)
                databaseCursor.callproc("SP_DELETE_FROM_node_table",[property_Obj.nodeId])
                databaseHandler.commit()
                sendToDevice(0x02,0,Source_Address,endpt)
            elif exists_in_master == 0:
                print("Error : No entry in both tables.")
                sendToDevice(0x02,0,Source_Address,endpt)


    except pymysql.Error() as e:
        databaseHandler.rollback()
        raise


def handlerfn(incomingZigBeePacketString,databaseHandler,databaseCursor):
    if int(incomingZigBeePacketString[2:4],16) == 68:
        if int(incomingZigBeePacketString[4:6],16) == 129:
            Len = incomingZigBeePacketString[38:40]
            incomingGabPacketString = incomingZigBeePacketString[40:40 + ((int(Len, 16)) * 2)]
            if int(incomingZigBeePacketString[40:42],16) ==0:
                parseInfo(incomingZigBeePacketString,databaseHandler,databaseCursor)
            elif int(Len,16)>10:
                parseProperty(incomingGabPacketString,databaseHandler,databaseCursor)
            elif int(Len,16) ==2:
                print('Final packet Recieved')
            else :
                parseStatus(incomingZigBeePacketString,databaseHandler,databaseCursor)
        elif int(incomingZigBeePacketString[4:6],16)==128:
            if int(incomingZigBeePacketString[6:8],16) == 0:
                print('Packet Transmission Acknowledged Successfully')
            elif int(incomingZigBeePacketString[6:8],16) != 0:
                print('Acknowledge packet Reveals failure')
    elif int(incomingZigBeePacketString[2:4],16)==69:
        if int(incomingZigBeePacketString[4:6],16) == 193:
            parseAnnouncement(incomingZigBeePacketString)


def parseProperty(incomingGabPacketString,databaseHandler,databaseCursor):
    node_Id = int(incomingGabPacketString[0:2],16)
    print("Node ID: ",node_Id)
    property_Id = incomingGabPacketString[4:6]
    property_Name = bytearray.fromhex(incomingGabPacketString[8:8 + (int(incomingGabPacketString[6:8], 16) * 2)]).decode("ISO-8859-1")
    Xlabel = bytearray.fromhex(incomingGabPacketString[(8 + (len(property_Name) * 2) + 2): 8 + (len(property_Name) * 2) + 2 + (int(incomingGabPacketString[8 + (len(property_Name) * 2):8 + (len(property_Name) * 2) + 2], 16) * 2)]).decode("ISO-8859-1")
    index1 = 8 + (len(property_Name) * 2) + 2 + (int(incomingGabPacketString[8 + (len(property_Name) * 2):8 + (len(property_Name) * 2) + 2], 16) * 2)
    Ylabel = bytearray.fromhex(incomingGabPacketString[index1 + 2:index1 + 2 + (int(incomingGabPacketString[index1:index1 + 2], 16) * 2)]).decode("ISO-8859-1")
    index2 = index1 + 2 + (int(incomingGabPacketString[index1:index1 + 2], 16) * 2)
    Xunit = bytearray.fromhex(incomingGabPacketString[index2 + 2:index2 + 2 + (int(incomingGabPacketString[index2:index2 + 2], 16) * 2)]).decode("ISO-8859-1")
    index3 = index2 + 2 + (int(incomingGabPacketString[index2:index2 + 2], 16) * 2)
    Yunit = bytearray.fromhex(incomingGabPacketString[index3 + 2:index3 + 2 + (int(incomingGabPacketString[index3:index3 + 2], 16) * 2)]).decode("ISO-8859-1")
    index4 = index3 + 2 + (int(incomingGabPacketString[index3:index3 + 2], 16) * 2)
    factor = struct.unpack('f', bytearray.fromhex(incomingGabPacketString[index4:index4 + 8]))[0]
    property = Property(node_Id, property_Id, property_Name, Xlabel, Ylabel, Xunit, Yunit, factor)
    insertIntoPropertyTable(property,databaseHandler,databaseCursor)
    print('Property packet Recieved')
    print("Node Id : ", property.nodeId)
    print("Property Id : ", property.propertyId)
    print("Property Name : ", property.propertyName)
    print("XLabel : ", property.propertyXLabel)
    print("YLabel : ", property.propertyYLabel)
    print("Xunit : ", property.propertyXUnit)
    print("Yunit : ", property.propertyYUnit)
    print("Factor:", property.propertyFactor)

def parseInfo(incomingGabPacketString,databaseHandler,databaseCursor):
    try:
        Source_Address = incomingGabPacketString[14:18]
        Source_Endpoint = incomingGabPacketString[18:20]
        print("Announced NodeId is ",int(incomingGabPacketString[40:42],16))
        version = struct.unpack('f',bytearray.fromhex(incomingGabPacketString[44:52]))[0]
        measurability = int(incomingGabPacketString[52:54],16)
        nodetitle = bytearray.fromhex(incomingGabPacketString[56:56+ (int(incomingGabPacketString[54:56],16) * 2)]).decode()
        node = Node(0, Source_Address, measurability, nodetitle,Source_Endpoint)
        insertIntoNodeTable(node,databaseHandler,databaseCursor)
        print('Network Address -',node.long_address)
        print('Endpoint Address -', node.endpt)
        print("version number recieved - ",version)
        print("Measuring Abilities - ",node.measurability)
        print("Node title - ",node.nodeTitle)
    except ValueError as e:
        print("Value Error exception - Corrupted packet")


def parseAnnouncement(incomingZigBeePacketString):
    SrcAddr = incomingZigBeePacketString[6:10]
    NwkAddr = incomingZigBeePacketString[10:14]
    i_e_e_e_address = incomingZigBeePacketString[14:30]
    Capabilities = incomingZigBeePacketString[30:32]
  
"""
## Test """
def parseStatus(incomingZigBeePacketString,databaseHandler,databaseCursor):
    Source_Address = incomingZigBeePacketString[14:18]
    Source_Endpoint = incomingZigBeePacketString[18:20]
    WasBroadcast = incomingZigBeePacketString[22:24]
    Link_Quality = incomingZigBeePacketString[24:26]
    Len = incomingZigBeePacketString[38:40]
    ## Extract the GAB packet from position 40 to position 40 
    ## plus the length of it (saved in variable Len)
    incomingGabPacketString = incomingZigBeePacketString[40:40 + ((int(Len, 16)) * 2)]
    node_Id  = int(incomingGabPacketString[0:2],16)
    property_Id = incomingGabPacketString[2:4]
    choice = int(incomingGabPacketString[4:6],16)
    if choice==1:
        property = Property(node_Id, property_Id, "", "", "", "", "", "")
        value =int(incomingGabPacketString[6:8],16)
        getPropertyFromTable(property,value,Source_Address,Source_Endpoint,databaseHandler,databaseCursor)
    elif choice ==2:
        property = Property(node_Id, property_Id, "", "", "", "", "", "")
        value = struct.unpack('f', bytearray.fromhex(incomingGabPacketString[6:14]))[0]
        getPropertyFromTable(property, value, Source_Address, Source_Endpoint,databaseHandler,databaseCursor)
    elif choice == 3:
        property = Property(node_Id, property_Id, "", "", "", "", "", "")
        value1 = int(incomingGabPacketString[6:8],16)
        value = int(incomingGabPacketString[8:10],16) <<8 | value1
        getPropertyFromTable(property, value, Source_Address, Source_Endpoint,databaseHandler,databaseCursor)


def main():
    print('Started Coordinator Gateway application.... Waiting for Serial Data')
    print ('Selected COM port ------>',sys.argv[1])
    databaseHandler = connectToDatabase()
    databaseCursor = getAcursor(databaseHandler)
    try:
        while True:
            if (serialHandler.inWaiting()>0):
                databaseHandler.ping(True)
                databaseCursor=databaseHandler.cursor()
                incomingZigBeePacketString = serialHandler.readline().decode("ISO-8859-1")
                handlerfn(incomingZigBeePacketString,databaseHandler,databaseCursor)

            
    except KeyboardInterrupt as e:
        print("Exception caught as keyboard interrupt")
        serialHandler.close()
        print("<----------------Program terminated followed by closing the active serial port------------->")
        exit

"""
## MG: If python program is called as a script, excute main function. """
if __name__=="__main__" : main()





			
