/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();
    log->printinfo(joinaddr.addr);
    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
        #ifdef DEBUGLOG
            log->LOG(&memberNode->addr, "init_this node failed. Exit.");
        #endif
        exit(1);
    }

    if(!introduceSelfToGroup(&joinaddr)) {
        finishUpThisNode();
        #ifdef DEBUGLOG
            log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
        #endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
//	int id = *(int*)(&memberNode->addr.addr);
//	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	//memberNode->inGroup = false;
    // node is up!
//	memberNode->nnb = 0;
//	memberNode->heartbeat = 0;
//	memberNode->pingCounter = TFAIL;
//	memberNode->timeOutCounter = -1;
//    initMemberListTable(memberNode);
    cleanUpNodeState();

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {

    if (isAddressEqualToNodeAddress(joinaddr)) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    } else {
        sendJOINREQMessage(joinaddr);
    }

    return 1;
}
//
//	MessageHdr *msg;
//    #ifdef DEBUGLOG
//        static char s[1024];
//    #endif
//
//    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
//        // I am the group booter (first process to join the group). Boot up the group
//        #ifdef DEBUGLOG
//            log->LOG(&memberNode->addr, "Starting up group...");
//        #endif
//        memberNode->inGroup = true;
//    }
//    else {
//        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
//        msg = (MessageHdr *) malloc(msgsize * sizeof(char));
//
//        // create JOINREQ message: format of data is {struct Address myaddr}
//        msg->msgType = JOINREQ;
//        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
//        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
//
//        #ifdef DEBUGLOG
//            sprintf(s, "Trying to join...");
//            log->LOG(&memberNode->addr, s);
//        #endif
//
//        // send JOINREQ message to introducer member
//        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
//
//        free(msg);
//    }
//    return 1;
//}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
   memberNode->inited = false;
   cleanUpNodeState();
   return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;
    string s = "in check Message";
    log->printinfo(memberNode->addr.addr);
    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */

    MessageHdr* recvMsg = (MessageHdr*)malloc(size * sizeof(char));
    memcpy(recvMsg, data, sizeof(MessageHdr));

    if (recvMsg->msgType == JOINREP) {

        memberNode->inGroup = true;

        deserializeMemberListTableForJOINREPMsgReceiving(data);
        string tmp = " : JOINREP";
        //string reqstr = strcat(const_cast<char *>(to_string(id).c_str()), tmp.c_str());
        log->printinfo((char*)tmp.c_str());

    } else if (recvMsg->msgType == JOINREQ) {

        int id;
        short port;
        long heartbeat;
        memcpy(&id, data + sizeof(MessageHdr), sizeof(int));
        memcpy(&port, data + sizeof(MessageHdr) + sizeof(int), sizeof(short));
        memcpy(&heartbeat, data + sizeof(MessageHdr) + sizeof(int) + sizeof(short), sizeof(long));

        string tmp = " : JOINREQ";
        string reqstr = strcat(const_cast<char *>(to_string(id).c_str()), tmp.c_str());
        log->printinfo((char*)reqstr.c_str());
        //log->printinfo();
        addNodeToMemberListTable(id, port, heartbeat, memberNode->timeOutCounter);

        Address address = getNodeAddress(id, port);

        sendJOINREPMsg(&address);

    } else if (recvMsg->msgType == HEARTBEAT) {
        int id;
        short port;
        long heartbeat;

        memcpy(&id, data + sizeof(MessageHdr), sizeof(int));
        memcpy(&port, data + sizeof(MessageHdr) + sizeof(int), sizeof(short));
        memcpy(&heartbeat, data + sizeof(MessageHdr) + sizeof(int) + sizeof(short), sizeof(long));

        string tmp = " : JOINHEARTBEAT";
        string reqstr = strcat(const_cast<char *>(to_string(id).c_str()), tmp.c_str());
        log->printinfo((char*)reqstr.c_str());

        if (!existsNodeInMemberListTable(id)) {
            addNodeToMemberListTable(id, port, heartbeat, memberNode->timeOutCounter);
        } else {
            MemberListEntry* node = getNodeInMemberListTable(id);

            node->heartbeat = heartbeat;
            node->settimestamp(memberNode->timeOutCounter);
        }
    }
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

	//check if this node should send a new heartbeat
	if (memberNode->pingCounter == 0) {
        //should send heartbeat
        memberNode->heartbeat++;
        log->printinfo((char*)to_string(memberNode->memberList.size()).c_str());

        for (vector<MemberListEntry>::iterator iter = memberNode->memberList.begin(); iter != memberNode->memberList.end(); iter++) {
            Address address = getNodeAddress(iter->id, iter->getport());
            if (!isAddressEqualToNodeAddress(&address)) {
                sendHEARTBEATMessage(&address);
            }
        }

        memberNode->pingCounter = TFAIL;
	} else {
        memberNode->pingCounter--;
	}
    //check if any node fails
    for (vector<MemberListEntry>::iterator iter = memberNode->memberList.begin(); iter != memberNode->memberList.end(); iter++) {
        Address address = getNodeAddress(iter->id, iter->port);

        if (!isAddressEqualToNodeAddress(&address)) {
            if (memberNode->timeOutCounter - iter->timestamp> TREMOVE) {

                memberNode->memberList.erase(iter);
                #ifdef DEBUGLOG
                    log->logNodeRemove(&memberNode->addr, &address);
                #endif

                break;
            }
        }
    }

    memberNode->timeOutCounter++;
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;
    string s = "sbsbsb";
    log->printinfo((char*)s.c_str());
    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

void MP1Node::sendJOINREQMessage(Address* joinAddr) {
    size_t msgsize = sizeof(MessageHdr) + sizeof(joinAddr->addr) + sizeof(long) + 1;
    MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    // Create JOINREQ message: format of data is {struct Address myaddr}
    msg->msgType = JOINREQ;
    memcpy((char*)(msg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char*)(msg + 1) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

    #ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Trying to join...");
    #endif

    // Send JOINREQ message to introducer member
    emulNet->ENsend(&memberNode->addr, joinAddr, (char *)msg, msgsize);

    free(msg);
}

void MP1Node::cleanUpNodeState() {
    memberNode->inGroup = false;

    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->timeOutCounter = -1;
    memberNode->pingCounter = TFAIL;
    initMemberListTable(memberNode);
}

void MP1Node::sendHEARTBEATMessage(Address* destinationAddr) {
    size_t msgsize = sizeof(MessageHdr) + sizeof(destinationAddr->addr) + sizeof(long) + 1;
    MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    // Create HEARTBEAT message
    msg->msgType = HEARTBEAT;
    memcpy((char*)(msg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char*)(msg + 1) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
    string str = "from: ";
    strcat((char*)str.c_str(), memberNode->addr.addr);
    strcat((char*)str.c_str(), " to : ");
    strcat((char*)str.c_str(), destinationAddr->addr);

    log->printinfo(memberNode->addr.addr);
    // Send HEARTBEAT message to destination node
    emulNet->ENsend(&memberNode->addr, destinationAddr, (char *)msg, msgsize);

    free(msg);
}

bool MP1Node::isAddressEqualToNodeAddress(Address *address) {
    return (memcmp((char*)&(memberNode->addr.addr), (char*)&(address->addr), sizeof(memberNode->addr.addr)) == 0);
}

void MP1Node::sendJOINREPMsg(Address* address) {
    size_t memberListEntrySize = sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long);

    size_t msgsize = sizeof(MessageHdr) + sizeof(int) + (memberNode->memberList.size() * memberListEntrySize);
    MessageHdr* msg= (MessageHdr*) malloc(msgsize * sizeof(char));
    msg->msgType = JOINREP;

    serializeMemberListTableForJOINREPMessageSending(msg);

    emulNet->ENsend(&memberNode->addr, address, (char*)msg, msgsize);

    free(msg);
};

void MP1Node::serializeMemberListTableForJOINREPMessageSending(MessageHdr* data) {
    int numOfItems = (int)memberNode->memberList.size();
    memcpy((char*)(data + 1), &numOfItems, sizeof(int));

    int offset = sizeof(int);
    for (vector<MemberListEntry>::iterator iter = memberNode->memberList.begin(); iter != memberNode->memberList.end(); iter++) {
        memcpy((char*)(data + 1) + offset, &iter->id, sizeof(int));
        offset += sizeof(int);

        memcpy((char*)(data + 1) + offset, &iter->port, sizeof(short));
        offset += sizeof(short);

        memcpy((char*)(data + 1) + offset, &iter->heartbeat, sizeof(long));
        offset += sizeof(long);

        memcpy((char*)(data + 1) + offset, &iter->timestamp, sizeof(long));

    }
}

MemberListEntry* MP1Node::getNodeInMemberListTable(int id) {
    MemberListEntry* entry = NULL;

    for(vector<MemberListEntry>::iterator iter = memberNode->memberList.begin(); iter != memberNode->memberList.end(); iter++){
        if (iter->id == id) {
            entry = iter.base();
            break;
        }
    }
    return entry;
}

bool MP1Node::existsNodeInMemberListTable(int id) {
    return this->getNodeInMemberListTable(id) != NULL;
}

Address MP1Node::getNodeAddress(int id, short port) {
    Address nodeAddress;

    memset(&nodeAddress, 0, sizeof(Address));
    *(int*)(&nodeAddress.addr) = id;
    *(short*)(&nodeAddress.addr[4]) = port;

    return nodeAddress;
}

void MP1Node::addNodeToMemberListTable(int id, short port, long heartbeat, long timestamp) {
    Address address = getNodeAddress(id, port);

    if (existsNodeInMemberListTable(id)) {
        MemberListEntry* newEntry = new MemberListEntry(id, port, heartbeat, timestamp);

        memberNode->memberList.insert(memberNode->memberList.end(), *newEntry);

        #ifdef DEBUGLOG
            log->logNodeAdd(&memberNode->addr, &address);
        #endif

        delete newEntry;
    }

}

void MP1Node::deserializeMemberListTableForJOINREPMsgReceiving(char* data) {
    int numOfItems;
    memcpy(&numOfItems, data + sizeof(MessageHdr), sizeof(int));

    for (int i = 0; i < numOfItems; ++i) {
        int id;
        short port;
        long heartbeat, timestamp;

        memcpy(&id, data + sizeof(MessageHdr) + sizeof(int), sizeof(int));
        memcpy(&port, data + sizeof(MessageHdr) + sizeof(int) * 2, sizeof(short));
        memcpy(&heartbeat, data + sizeof(MessageHdr) + sizeof(int) * 2 + sizeof(short), sizeof(long));
        memcpy(&timestamp, data + sizeof(MessageHdr) + sizeof(int) * 2 + sizeof(short) + sizeof(long), sizeof(long));

        // Create and insert new entry
        addNodeToMemberListTable(id, port, heartbeat, timestamp);
    }
}
