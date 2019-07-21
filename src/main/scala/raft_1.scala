import akka.actor._
import com.typesafe.config.ConfigFactory
import java.io._
import scala.collection.mutable.{HashMap, HashSet}
import scala.concurrent.duration._
import scala.util.Random

// RaftMessages are the messages that a raft node may send/receive
// All the non-client messages include "term".  If a node detects that it's term is less than the sender's term
// it will update it's term to the later and also step down from leader role or abandon leader candidacy if either
// is the case.
// AppendRequest has prevLogIndex and prevLogTerm which the recipient uses in attempts of finding a common root
// between the sender's and recipient's logs.  "entries" are the log elements to append to the recipient's log.
// "leaderCommit" is the index into the log to the highest element known to be stored by a majority of nodes.
// Both AppendRequest and VoteRequest supply an ActorRef.  AppendRequest's is used by the leader to provide
// follower nodes with it's reference.  Follower's supply this to clients if they receive a ClientRequest when
// they are not leader.  Follower's will use this in VoteRequest to compare against future VoteRequest's in a term.
// If it receives from a different candidate the follower will deny a vote.  If from the same (shouldn't happen)
// a vote will be granted.
// Vote reqeust supplies "logLength" and "lastLogTerm" so the recipient may compare their log against the
// candidate's to determine if the candidate's log is at least as up to date as the follower's.
// Responses return a boolean which the original sender uses to determine if their request has been
// accepted or rejected.
// ClientRequest includes only the message to commit.  The response variant includes an ActorRef to the leader.
// Client uses this to locate the leader.  NOTE:  In this model client will not cache this information
// between different successive messages, but will only use it for the current message.  This is done
// to generate more interesting situations in "CHAOS" mode.
// The boolean in ClientResponse indicates whether or not a majority of nodes have received the message.
trait RaftMessage
case class AppendRequest(val term:Int, val leaderID:ActorRef, val prevLogIndex:Int, val prevLogTerm:Int,
                         val entries:Vector[LogElement], val leaderCommit:Int) extends RaftMessage
case class VoteRequest(val term:Int, val candidateID:ActorRef, val logLength:Int, val lastLogTerm:Int) extends RaftMessage
case class VoteResponse(val term:Int, val voteGranted:Boolean) extends RaftMessage
case class AppendResponse(val term:Int, val success:Boolean)   extends RaftMessage
case class ClientRequest(val msg:Any) extends RaftMessage
case class ClientResponse(val msg:Any, val leader:Option[ActorRef], val msgApplied:Boolean)

// GroupList is used initially to provide each raft node with the references to its group mates.
case class GroupList(val nodeSet:HashSet[ActorRef])

// LogElements are serialized to disk and include a client message (payload) as well as the term that the message
// was received.
case class LogElement(val term:Int, val payload:Payload)

// LeaderLedger is held by a leader to track to progress of the follower nodes in receiving/correcting
// their logs.
// "nextIndex" stores an index for each node which represents the position in the log to next send.
// "matchIndex" represents the position in the log up to which the leader's and a particular follower's
// entries match.  "lastSendLength" is used to detrmine how far to advance the next and match indexes
// if the follower accepts the append request.
case class LeaderLedger(var nextIndex:HashMap[ActorRef, Int], var matchIndex:HashMap[ActorRef, Int],
                        var lastSendLength:HashMap[ActorRef, Int])

// Payloads are different ways client messages are stored in the log.
// Typically the client's ActorRef is stored with the message to provide a return reference when informing
// the client that their message has been committed.
// "MsgWNoRef" is only used once for each nodes initial log header.
trait Payload
case class MsgPlusReturnRef(val msg:Any, val returnTo:ActorRef) extends Payload
case class MsgWNoRef(val msg:Any)                               extends Payload

// term, votedFor, and the log are serialized to disk at every alteration.
// This model does not provide for reading in the serialized state at node start,
// but this could be added in with realitively little effort.
@SerialVersionUID(7L)
case class SerializableNodeState(var term:Int, var votedFor:Option[ActorRef], var log:Vector[LogElement]) extends Serializable
// VolatileState (as the name suggests) is not serialized to disk.  The fields are used to track which
// elements may be committed and which have been committed.
case class VolatileState(var commitIndex:Int, var lastApplied:Int)

// The following case objects are timer handles and trigger messages
case object ElectionTimerKey
case object ElectionTimerPop
case object HeartbeatTimerKey
case object HeartbeatTimerPop
case object HiatusTimerKey
case object HiatusTimerPop

// The main actor in the model -- the Raft node.
class RaftNode(val state:SerializableNodeState) extends Actor with Timers {
  // node may be a follower, a candidate, or a leader -- exclusively
  // These RaftRoles impact how a node behaves upon recipt of messages.
  trait RaftRole
  case object Follower  extends RaftRole
  case object Candidate extends RaftRole
  case object Leader    extends RaftRole

  val vState                       = VolatileState(0, 0)
  // Next tow are Options becuase a node will eliminate its ledger when stepping down from leader role
  // and will save a None for leader if no leader is known.
  var lLedger:Option[LeaderLedger] = None
  var leader:Option[ActorRef]      = None
  var role:RaftRole                = Follower
  // used to serialize non-volitile state to disk
  val diskOut                      = new ObjectOutputStream(new FileOutputStream(f"raft_node_${self.path.name}%s_state"))
  // random number generator used to create variance in timer events and supply variability in "CHAOS" mode
  val rand                         = Random
  // nodeSet stores the references for other nodes in the raft group
  var nodeSet                      = HashSet[ActorRef]()
  // voteSet stores the votes received when a candidate
  var voteSet                      = HashSet[ActorRef]()
  // if onHiatus == true the node will not respond to or generate messages
  // EXCEPT if the node is leader, the node will accept requests from client, however it will not forward them.
  // onHiatus alternates between true and false in "CHAOS" mode (mode == 1) based upon timer events.
  // If mode == 0 (normal/boring mode), onHiatus will always be false
  var onHiatus                     = false
  // timer parameters.  "Base" parameters specify a minimum time before the next timer event
  // "Variance" is the upper bound on a random additional time to the minimum before the next timer event.
  // For "hiatus" timers, the "hiatusTime" pair specifies the time to the next potential hiatus.
  // The "timeInHiatus" (perhaps it should be timeOnHiatus), specifies how long the node will remain on hiatus.
  // "potentialForHiatus" spsecifies the percent chance that a node will go on hiatus when the
  // "hiatusTime" timer triggers.
  val electionTimeBase             = ConfigFactory.load.getInt("election-time-base")
  val electionTimeVariance         = ConfigFactory.load.getInt("election-time-variance")
  val heartbeatTimeBase            = ConfigFactory.load.getInt("heartbeat-time-base")
  val heartbeatTimeVariance        = ConfigFactory.load.getInt("heartbeat-time-variance")
  val mode                         = ConfigFactory.load.getInt("mode")
  val hiatusTimeBase               = ConfigFactory.load.getInt("hiatus-time-base")
  val hiatusTimeVariance           = ConfigFactory.load.getInt("hiatus-time-variance")
  val timeInHiatusTimeBase         = ConfigFactory.load.getInt("time-in-hiatus-base")
  val timeInHiatusTimeVariance     = ConfigFactory.load.getInt("time-in-hiatus-variance")
  val potentialForHiatus           = ConfigFactory.load.getInt("potential-for-hiatus")

  // start the elction timer and if in "CHAOS" mode start the hiatus timer.
  resetElectionTimer
  if (mode == 1) setHiatusTimer

  // depending on state and message received execute these functions
  def receive = {
    case HiatusTimerPop                        => handleHiatus
    case cr:ClientRequest  if (role == Leader) => leaderAppendLog(cr, sender)
    case GroupList(ns)                         => nodeSet = ns
    case ar:AppendRequest  if (!onHiatus)      => appendEntriesRPC(ar)
    case vr:VoteRequest    if (!onHiatus)      => requestVoteRPC(vr)
    case vr:VoteResponse   if (!onHiatus)      => handleVoteResponse(vr, sender)
    case ElectionTimerPop  if (!onHiatus)      => becomeCandidate
    case HeartbeatTimerPop if (!onHiatus)      => if (role == Leader) sendHeartbeats
    case ar:AppendResponse if (!onHiatus)      => if (role == Leader) handleAppendResponse(ar, sender)
    case cr:ClientRequest  if (!onHiatus)      => sender ! ClientResponse(cr.msg, leader, false)
  }

  // this function appends a client request to the a leader's log and serializes to disk
  def leaderAppendLog(cr:ClientRequest, client:ActorRef) {
    print(f"Leader ${self.path.name} receives ${cr.msg} from client -- appending to log\n")
    state.log = state.log :+ LogElement(state.term, MsgPlusReturnRef(cr.msg, client))
    diskOut.writeObject(state)
    diskOut.flush
  }

  // when a follower recieves a heartbeat/append it will....
  // reset its election timer, validate its current term and then determine
  // if it can append the sent entries.  If a later commitIndex is sent than what
  // the node has, it will apply the diffrence.
  def appendEntriesRPC(ar:AppendRequest): Unit = {
    resetElectionTimer
    checkAndUpdateTerm(ar.term)
    // we set leader to none, if we get to the "else" we will reset the leader reference
    // this way if the sender is not a valid leader we can stop advertising it as such.
    leader = None
    // if so-called leader's term is stale reject request and return current term
    if (ar.term < state.term) { ar.leaderID ! AppendResponse(state.term, false) }
    // if leader's adverstise previous term does not match our previous term, reject the update
    // leader will send one more next time
    // NOTE:  We cannot have two different leaders from the same term so if our previous entry is from
    // the same term as the leader's and its position matches ours we know all previous entries are good
    else if (state.log.length <= ar.prevLogIndex || state.log(ar.prevLogIndex).term != ar.prevLogTerm) {
      sender ! AppendResponse(state.term, false)
    }
    // if we have additional new entries add them to our log
    else {
      if (state.log.length == ar.prevLogIndex + 1 ||
          ar.entries.length > 0 && state.log(ar.prevLogIndex + 1) != ar.entries.head) {
        state.log = state.log.take(ar.prevLogIndex + 1) ++: ar.entries
        diskOut.writeObject(state)
        diskOut.flush
      }
      // if leader advertises a higher commitIndex than ours, update ours and commit entries
      if (ar.leaderCommit > vState.commitIndex) {
        vState.commitIndex = Math.min(ar.leaderCommit, state.log.length - 1)
        applyCommitted
      }
      // save leader reference and send accept response to leader
      leader = Some(ar.leaderID)
      ar.leaderID ! AppendResponse(state.term, true)
    }
  }

  // leader calls this function when it receives a response to its heartbeat/append request
  def handleAppendResponse(ar:AppendResponse, sender:ActorRef): Unit = {
    val ledger = lLedger.get
    // If follower agrees to the request advance the next and match indexes,
    // determine the highest matchIndex held by a majority of nodes and update commitIndex
    // latest commitIndex will be sent at next heartbeat/append.
    if (ar.success) {
      ledger.nextIndex(sender) += ledger.lastSendLength(sender)
      ledger.matchIndex(sender) = ledger.nextIndex(sender) - 1
      // unload the matchIndex HashMap, take the value, add leader's postion in log,
      // sort and drop first n/2.  The first element in the resultant sequence will
      // be the index of the log element known by a majority of nodes
      val N = ledger.matchIndex.toSeq.map(kvPair => kvPair._2)
                                    .union(Seq(state.log.length - 1))
                                    .sorted.drop(nodeSet.size / 2).head
      // If we've made progress in advancing our commitIndex (and the latest majority index is of current
      // term for safety) update the commitIndex and apply committed messages to app.
      if (N > vState.commitIndex && state.log(N).term == state.term) {
        vState.commitIndex = N
        print(f"Leader ${self.path.name} determines that log entries up to position $N are known by a majority\n")
        print(f"Leader commit index ${vState.commitIndex}\n")
        applyCommitted

      }
    }
    // if follower rejects request either the leader is in an obsolete term or else the follower's
    // log up to the element sent does not match leader's
    else {
      // checkAndUpdateTerm will detect obsolete term -- leader will step down inside this function
      // if it is the case
      checkAndUpdateTerm(ar.term)
      // if we're still leader here we should decrement our nextIndex (send one more prvious entry) and
      // try again with next heartbeat/append
      if (role == Leader) ledger.nextIndex(sender) -= 1
    }
  }

  // if heartbeat timer triggers and node is leader send heartbeat/appends to all followers
  // also set up next heartbeat timer
  def sendHeartbeats(): Unit = {
    val ledger = lLedger.get
    nodeSet.filter(node  => node != self)
           .foreach(node => {
                              val prevIndex = Math.max(ledger.nextIndex(node) - 1, 0)
                              val toSend = state.log.drop(prevIndex + 1)
                              ledger.lastSendLength(node) = toSend.length
                              node ! AppendRequest(state.term, self, prevIndex, state.log(prevIndex).term,
                                                   toSend, vState.commitIndex)
           })
    scheduleHeartbeat
  }

  // if leader informs a node (or itself) that the valid commit index is higher than what
  // the node has previously committed, commit the uncommitted in the order they exist in the log
  // In this model this means printing to console that "node is applying message"
  def applyCommitted():Unit = {
    for (entry <- vState.lastApplied + 1 to vState.commitIndex) {
      var outS = state.log(entry).payload
      outS match {
        case MsgPlusReturnRef(m, _) => print(f"${self.path.name}%s applying ${m}\n")
        case _ => ()
      }
      vState.lastApplied += 1
      // if we are leader we notify the client that their message has been committed.
      (role, outS) match {
        case (Leader, MsgPlusReturnRef(ms, rtrn2)) => rtrn2 ! ClientResponse(ms, Some(self), true)
        case (_,_) => ()
      }
    }
    // This foldLeft simply accumulates the messages into the log into a space separated string
    // for display.  Showing this to demonstrate consistency between all nodes.
    val applied:String = state.log.take(vState.lastApplied + 1)
                                  .foldLeft(" ")((accum, i) => i.payload match {
                                    case MsgPlusReturnRef(ms, _) => accum + ms + " "
                                    case _                       => accum
                                    })
    print(f"${self.path.name} has applied the following sequence: ${applied}\n")
  }

  // function to reset the hiatus timer to a ranodm future time depending on whether a node
  // is going onHiatus(top) or leaving(bottom)

  // if election timer triggers become a candidate and send vote requests to all nodes
  // ... but first vote for self, update the term, and indicate that there is no valid leader.
  // also set up next election timer
  def becomeCandidate(): Unit = {
    role   = Candidate
    leader = None
    voteSet.clear
    voteSet    += self
    state.term += 1
    print(f"${self.path.name} is holding election at term ${state.term}\n")

    state.votedFor = Some(self)
    diskOut.writeObject(state)
    diskOut.flush
    resetElectionTimer
    nodeSet.filter(node  => node != self)
           .foreach(node => node ! VoteRequest(state.term, self, state.log.length, state.log.last.term))
  }

  // if this is received, update term if necessary, and either grant or reject vote request
  def requestVoteRPC(vr:VoteRequest): Unit = {
    checkAndUpdateTerm(vr.term)
    // if candidate's term is less than ours reject the vote and supply the latest term
    if (vr.term < state.term) { vr.candidateID ! VoteResponse(state.term, false) }
    // otherwise grant the vote if this node has not voted for anyone else and candidates
    // log is up to date.  NOTE:  votedFor will be None'd out in checkAndUpdateTerm if needed
    // to allow it to vote in a subsequent election
    else if ((!state.votedFor.isDefined || vr.candidateID == state.votedFor.get)
      && isUpToDate(vr.logLength, vr.lastLogTerm, state.log.length, state.log.last.term)) {
      resetElectionTimer
      state.votedFor = Some(vr.candidateID)
      diskOut.writeObject(state)
      diskOut.flush
      sender ! VoteResponse(state.term, true)
    }
  }

  // if a candidate and we get a vote response to a vote request
  // verify that we're in correct term -- if not update term and give up candidacy
  // else if vote granted increment our vote count.
  // if this results in gaining a majority vote become leader, set up ledger and
  // send first heartbeat/appends to all nodes.
  // also start heratbeat timer
  def handleVoteResponse(vr:VoteResponse, sender:ActorRef): Unit = {
    checkAndUpdateTerm(vr.term)
    if (role == Candidate && vr.voteGranted) {
      voteSet += sender
      print(f"${self.path.name} gets a new vote from ${sender}\n")

      if (voteSet.size > nodeSet.size / 2) {
        timers.cancel(ElectionTimerKey)
        print(f"\n${self.path.name} is elected leader!\n")
        role               = Leader
        leader             = Some(self)
        var newNextIndex   = HashMap[ActorRef, Int]()
        var newMatchIndex  = HashMap[ActorRef, Int]()
        var lastSendLength = HashMap[ActorRef, Int]()
        val ar = AppendRequest(state.term, self, state.log.length - 1, state.log.last.term, Vector(), vState.commitIndex)
        nodeSet.filterNot(node => node == self)
               .foreach(node   => {
                                    newNextIndex(node)   = state.log.length
                                    newMatchIndex(node)  = 0
                                    lastSendLength(node) = 0
                                    node ! ar
                })
        lLedger = Some(LeaderLedger(newNextIndex, newMatchIndex, lastSendLength))
        scheduleHeartbeat
      }
    }
  }

  // if our term is behind the current term, demote self to follower and update term.
  def checkAndUpdateTerm(receivedTerm:Int): Unit =
    if (receivedTerm > state.term) {
      timers.cancel(HeartbeatTimerKey)
      state.term = receivedTerm
      state.votedFor = None
      diskOut.writeObject(state)
      diskOut.flush
      voteSet.clear
      if (role == Leader) { print(f"${self.path.name} detects the term has advanced -- stepping down from leader\n")}
      role    = Follower
      lLedger = None
    }

  // if cand term > our term or (cand term == our term and length is >= ours)
  // candidate is up to date
  def isUpToDate(candLength:Int, candTerm:Int, followerLength:Int, followerTerm:Int) =
    (candTerm, followerTerm) match {
      case (ct, ft) if (ct == ft) => if (candLength >= followerLength) true else false
      case (ct, ft) if (ct > ft)  => true
      case _                      => false
    }

  def setHiatusTimer():Unit = {
    if (onHiatus)
    timers.startSingleTimer(HiatusTimerKey, HiatusTimerPop,
                            Duration(rand.nextInt(timeInHiatusTimeVariance) + timeInHiatusTimeBase, MILLISECONDS))
    else
    timers.startSingleTimer(HiatusTimerKey, HiatusTimerPop,
                            Duration(rand.nextInt(hiatusTimeVariance) + hiatusTimeBase, MILLISECONDS))
  }
  // function to reset the election timer
  def resetElectionTimer():Unit = {
    timers.startSingleTimer(ElectionTimerKey, ElectionTimerPop,
                            Duration(rand.nextInt(electionTimeVariance) + electionTimeBase, MILLISECONDS))
  }
  // function to schedule the next heartbeat/append message from the leader
  def scheduleHeartbeat():Unit = {
    timers.startSingleTimer(HeartbeatTimerKey, HeartbeatTimerPop, Duration(rand.nextInt(heartbeatTimeVariance) + heartbeatTimeBase, MILLISECONDS))
  }

  // function to enter and leave hiatus state.  if returning from hiatus timers are restarted.
  def handleHiatus():Unit = {
    if (onHiatus) {
      print(f"....<${self.path.name}>... 'Sorry everyone, not sure where I've been but I'm back now'\n")
      onHiatus = false
      resetElectionTimer
      if (role == Leader) scheduleHeartbeat
    }
    else if (rand.nextInt(100) < potentialForHiatus) {
      print(f"${self.path.name} appears to have gone on hiatus!!!!\n")
      onHiatus = true
    }
    setHiatusTimer
  }
}

// client code for the model
// client will send messages of a monitionically increasing integer to a random node
// if the node is not the leader the node will respond with a reference to the leader
// the client will resend the message.  However, if no leader is known the client will abandon
// the message attempt and move on the next integer in the sequence.
// Messages are sent at at random time intervals.
// After a number of messages (dictated in application.conf) the client will
// shut the actor system down, but first will print its believed sequence of committed
// messages for comparison against each live raft nodes sequence.
class Client (val nodes:Seq[ActorRef]) extends Actor with Timers {
  case object TimerKey
  case object TimerPop
  val rand                   = Random
  val clientTimeBase         = ConfigFactory.load.getInt("client-time-base")
  val clientTimeVariance     = ConfigFactory.load.getInt("client-time-variance")
  val numMessages            = ConfigFactory.load.getInt("number-client-messages")
  var msgCounter             = 0
  var leaderTry:ActorRef     = nodes(rand.nextInt(nodes.length - 1))
  var msgsIKnowToBeCommitted = Vector[Any]()
  resetTimer

  def resetTimer = timers.startPeriodicTimer(TimerKey, TimerPop,
                                             Duration(rand.nextInt(clientTimeBase) + clientTimeBase, MILLISECONDS))

  def receive = {
    case ClientResponse(msg, ldr, success) => (ldr, success) match {
      case (_, true)     =>{
        print(f"\nClient reports commit of msg $msg\n")
        msgsIKnowToBeCommitted = msgsIKnowToBeCommitted :+ msg
      }
      case (None, _)     => print(f"Client gives up on msg $msg.  No leader to communicate with\n")
      case (Some(ld), _) => {
        print (f"Client learns ${leaderTry} is not leader.\nClient retries at ${ld}%s\n")
        leaderTry = ld
        leaderTry ! ClientRequest(msg)
      }
    }
    case TimerPop => {
      msgCounter += 1
      if (msgCounter > numMessages){
        print("\n\nclient shutting the system down\n")
        print(f"client thinks these messages have been commited\n")
        print(f"${msgsIKnowToBeCommitted}\n")
        context.system.terminate
      }
      leaderTry = nodes(rand.nextInt(nodes.length - 1))
      print(f"Client sends $msgCounter to ${leaderTry}%s\n")
      leaderTry ! ClientRequest(msgCounter)
    }
  }
}

object Main extends App {
  val numNodes   = ConfigFactory.load.getInt("number-nodes")
  var nodeSet    = HashSet[ActorRef]()
  val system     = ActorSystem("Raft")
  val headEle    = LogElement(0, MsgWNoRef("HEAD"))


  for (node <- 1 to numNodes) {
    var newNode = system.actorOf(Props(new RaftNode(SerializableNodeState(0, None, Vector(headEle)))), name = f"node$node%s")
    nodeSet += newNode
  }
  nodeSet.foreach(node => node ! GroupList(nodeSet))

  val client = system.actorOf(Props(new Client(nodeSet.toSeq)), name = "client")
}
