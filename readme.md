
video: https://www.youtube.com/watch?v=YbZ3zDzDnrw

ppt: https://www.slideserve.com/madge/raft-a-consensus-algorithm-for-replicated-logs

## approaches to consensus

leader-less

leader-based

raft uses a leader:

- decomposes the problem to 1) normal operation, 2) leader changes

- simple normal operation, no conflicts, only need to deal with leader changes

- more efficient than leader-less approaches


## 0. introduction

### server states

- leader: handler all the client interactions

- follower: completely passive, only responds to incomming RPCs

- candidate: temp mode, only used in leader election, used to elect a new leader

normal opertion: 1 leader, N-1 followers

leader changes: ??

### term

time divided to terms, each term starts with election, followed by normal operation unader a single leader

- at most 1 leader per term

- some terms have no leader (vote failed, split vote)

each server maintains CURRENT TERM value

## 1. leader election

server starts as a  follower, expect to receive RPCs from leader or candidates.

leader MUST send `heartbeats` (empty AppendEntries RPC) to maintain authority.

if `electionTimeout` elapses with no RPCs, a follower assumes leader is dead, and starts new election.

### election basics

increment current term & change state to candidate

vote for self, sending RequestVote RPC to all other servers, retry until either:

- (most tme) receive votes from majority servers & becoming leader, sending AppendEntries heartbeat to all other servers

- receive RPC from valid leader & becoming follower

- no-one wins election (electionTimeout elapses), increment term & start new election

**safety guarantee: at most one winner per term**: eacher server only give out ONE VOTE per term,

**liveness guarantee: some candidates must eventually win**: choose `electionTimeout` randomly in [T, 2T], works well if T >> broadcast time

## 2. normal operations (basic log replication)

### log structure

log file has many entries. log entry contains: index & (term, command)

entry `commited` if known to be stored on **majority servers**.

### normal operation

client -> leader, leader appends command to its log, leader -> followers using AppendEntries RPC, once entry `commited`, leader execute command & return to the client. Leader then notifies followers to execute command.

The AppendEntries RPC includes the following important components:

```
    Term: The term number that the leader is serving in. It helps the follower nodes to identify the correctness of the leader and handle inconsistencies.

    Leader ID: The ID of the leader node that is sending the AppendEntries RPC.

    Previous Log Index and Term: These represent the index and term of the log entry that immediately precedes the new entries being sent. They are used by the follower nodes to check the consistency of their logs with the leader's log.

    Entries: This contains the new log entries that the leader wants to append to the follower's log.

    Leader Commit Index: The index of the highest log entry that the leader has committed. It helps the followers determine which log entries can be safely applied to their state machines.
```

## 3. safety and consistency after leader changes

### safety (make sure leader's log is correct)

Raft safety property: if a leader has decided that a log entry is `commited`, that entry will be present in the logs of all future leaders.

- leaders never overwrite entries in their logs

- only entries in leader's log can be commited

- entries must be commited before applying to state machine


#### picking the best leader

new election rules: during elections, choosing candidates with log most likely to contain all committed entries

new commitment rules:

- must be stored on majority servers

- at least one new entry from leader's term must also be stored on majority servers

### consistency (make sure followers' logs match leader's log)

leader keeps nextIndex for each followers, followers overwrite inconsistent entry, delete all subsequent entries.

## 4. neutralizing old leaders

TODO

## 5. client protocol

send commands to leader, if leader unkonwn, contact any server, which will redirect to leader.

if request times out, client reissues command to other server.

**risk**: command will be executed twice!

solution: client embeds a unique id in each command, server check log entry with that id before accepting command

result: `exactly-once semantics`, each command be executed EXACTLY ONCE.

## 6. configuration changes

TOOD
