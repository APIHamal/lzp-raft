package com.lizhengpeng.lraft.core;

import com.lizhengpeng.lraft.request.AppendLogMsg;
import com.lizhengpeng.lraft.request.RequestVoteMsg;
import com.lizhengpeng.lraft.response.AppendLogRes;
import com.lizhengpeng.lraft.response.RequestVoteRes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * raft核心算法的实现
 * @author lzp
 */
public class RaftNode implements MessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    private static final int minElectionTimeout = 1000; // 最小选举超时时间

    private static final int maxElectionTimeout = 3000; // 最大选举超时时间

    private static final int heartbeatInterval = 500; // 心跳时间间隔

    private RaftRole nodeRole = RaftRole.FOLLOWER; // 当前节点的角色

    private String currentId;

    private long voteFor; // 表示为某个任期中的节点已经投过票了

    private long voteCount; // 当前候选者节点得到的票数

    private AtomicLong currentTerm = new AtomicLong(0); // 当前节点的任期

    private RaftGroupTable raftGroupTable = new RaftGroupTable(); // 集群成员表

    private AtomicLong receiveCount = new AtomicLong(0);

    private RpcServer rpcServer = new RpcServer();

    private TaskExecutor taskExecutor = new TaskExecutor();

    private ScheduledFuture<?> followerSchedule;

    private ScheduledFuture<?> candidateSchedule;

    private ScheduledFuture<?> leaderSchedule;

    private SecureRandom secureRandom = new SecureRandom();

    /**
     * 添加集群中的
     * @param endpoint
     */
    public NodeId addGroupMember(Endpoint endpoint) {
        return raftGroupTable.addEndpoint(endpoint);
    }

    /**
     * 添加集群中的
     * @param endpointList
     */
    public void addGroupMember(List<Endpoint> endpointList) {
        endpointList.forEach(item -> {
            raftGroupTable.addEndpoint(item);
        });
    }

    /**
     * 获取随机超时时间
     * @return
     */
    public int randomTimeOut() {
        return minElectionTimeout + secureRandom.nextInt(maxElectionTimeout - minElectionTimeout);
    }

    /**
     * 启动rpc服务
     * @param endpoint
     */
    public void startRaftServer(Endpoint endpoint) {
        currentId = endpoint.getNodeId().getText();
        rpcServer.setMessageHandler(this);
        rpcServer.setRaftGroupTable(raftGroupTable);
        rpcServer.startRpcServer(endpoint); // 启动rpc的服务
        // 启动时都是follower状态
        registerFollowerTimeoutTask();
    }

    /**
     * 注册follower节点的超时任务
     * @return
     */
    public void registerFollowerTimeoutTask() {
        // 注意这里将异步的调用转为了同步的调用
        followerSchedule = taskExecutor.submit(() -> {
            voteCount = 0; // 重新设置当前获取的票数
            nodeRole = RaftRole.CANDIDATE; // 切换角色为候选者
            currentTerm.addAndGet(1); // term首先增加1

            // 发送投票的请求到各个节点中
            RequestVoteMsg message = new RequestVoteMsg();
            message.setNodeId(currentId);
            message.setTerm(currentTerm.get());
            message.setLastLogIndex(0L);
            message.setLastLogIndex(0L); // 最后一条日志的任期和索引
            rpcServer.broadcastMsg(message); // 广播请求投票的消息
            logger.info("node send vote => {},{},{},{}", nodeRole.name(), currentId, currentTerm.get(), message);

            // 变为candidate后需要设置候选者定时器
            // 如果发生了平票或者网络分区则需要继续进行选举操作
            registerCandidateTimeoutTask();
            }, randomTimeOut());
    }

    /**
     * 注册follower节点的超时任务
     * @return
     */
    public void registerCandidateTimeoutTask() {
        // 注意这里将异步的调用转为了同步的调用
        candidateSchedule = taskExecutor.submit(() -> {
            voteCount = 0; // 重新设置当前获取的票数
            currentTerm.addAndGet(1); // term首先增加1

            // 发送投票的请求到各个节点中
            RequestVoteMsg message = new RequestVoteMsg();
            message.setNodeId(currentId);
            message.setTerm(currentTerm.get());
            message.setLastLogIndex(0L);
            message.setLastLogIndex(0L); // 最后一条日志的任期和索引
            logger.info("node send vote => {},{},{},{}", nodeRole.name(), currentId, currentTerm.get(), message);

            rpcServer.broadcastMsg(message); // 广播请求投票的消息

            // candidate超时后需要重新开始随机超时发起选举
            registerCandidateTimeoutTask();
        }, randomTimeOut());
    }

    /**
     * 注册follower节点的超时任务
     * @return
     */
    public void registerLeaderTask() {
        // 注意这里将异步的调用转为了同步的调用
        leaderSchedule = taskExecutor.submit(() -> {
            // 心跳消息
            AppendLogMsg appendLogMsg = new AppendLogMsg();
            appendLogMsg.setLeaderId(currentId);
            appendLogMsg.setTerm(currentTerm.get());
            appendLogMsg.setPreLogIndex(0l);
            appendLogMsg.setPreLogTerm(0l); // 实际上应该是发送日志的消息
            rpcServer.broadcastMsg(appendLogMsg);
            logger.info("node append log => {},{},{},{}", nodeRole.name(), currentId, currentTerm.get(), appendLogMsg);

            // 重复的发送数据
            registerLeaderTask();
        }, heartbeatInterval);
    }

    /**
     * 取消已经注册的定时任务
     */
    public void cancelTimeoutTask() {
        cancelTimeoutTask(followerSchedule);
        cancelTimeoutTask(candidateSchedule);
        cancelTimeoutTask(leaderSchedule);
    }

    /**
     * 取消指定的定时任务
     * @param scheduledFuture
     */
    private void cancelTimeoutTask(ScheduledFuture<?> scheduledFuture) {
        if (scheduledFuture != null) {
            try {
                scheduledFuture.cancel(Boolean.TRUE);
            } catch (Exception e) {
                logger.info("cancel timeout task exception", e);
            }
        }
    }

    @Override
    public void onAppendLog(NodeId nodeId, AppendLogMsg appendLogMsg) {
        taskExecutor.submit(() -> {
            logger.info("receive append log {},{}", nodeId, appendLogMsg);
            if (appendLogMsg.getTerm() < currentTerm.get()) {
                AppendLogRes response = new AppendLogRes();
                response.setTerm(currentTerm.get());
                response.setSuccess(false);
                rpcServer.sendMsg(nodeId, response);
                return;
            }
            if (appendLogMsg.getTerm() > currentTerm.get()) {
                nodeRole = RaftRole.FOLLOWER;
                currentTerm.set(appendLogMsg.getTerm());
                // 重新注册follower节点的定时器
                cancelTimeoutTask();
                registerFollowerTimeoutTask();
                return;
            }
            if (nodeRole == RaftRole.LEADER) {
                logger.error("raft cluster found two leader, fatal error!!!");
                return;
            }
            if (nodeRole == RaftRole.FOLLOWER) {
                // follower当前无条件接收
                AppendLogRes response = new AppendLogRes();
                response.setTerm(currentTerm.get());
                response.setSuccess(true);
                rpcServer.sendMsg(nodeId, response);
                // 重新设置选举定时器
                cancelTimeoutTask();
                registerFollowerTimeoutTask();
            } else {
                // candidate节点需要退化成follower节点
                // candidate节点目前不需要返回数据
                // 等待下次收到心跳时在follower节点时候再进行处理
                nodeRole = RaftRole.FOLLOWER;
                currentTerm.set(appendLogMsg.getTerm());
                // 重新注册follower节点的定时器
                cancelTimeoutTask();
                registerFollowerTimeoutTask();
            }
        });
    }

    @Override
    public void onAppendLogCallback(AppendLogRes appendLogRes) {
        taskExecutor.submit(() -> {
//            logger.info("receive append log callback [{}]", appendLogRes);
            if (appendLogRes.getTerm() > currentTerm.get()) {
               // 当前的leader节点退化成follower节点并等待心跳
                nodeRole = RaftRole.FOLLOWER;
                currentTerm.set(appendLogRes.getTerm());
                // 重新注册follower节点的定时器
                cancelTimeoutTask();
                registerFollowerTimeoutTask();
                return;
            }
        });
    }

    @Override
    public void onRequestVote(NodeId nodeId, RequestVoteMsg requestVoteMsg) {
        taskExecutor.submit(() -> {
            logger.info("receive vote request {},{}", nodeId, requestVoteMsg);
            // 请求投票的节点的term小于当前的节点则返回当前节点的任期给对端节点
            if (requestVoteMsg.getTerm() < currentTerm.get()) {
                RequestVoteRes response = new RequestVoteRes();
                response.setTerm(currentTerm.get());
                response.setSuccess(false);
                rpcServer.sendMsg(nodeId, response);
                return;
            }
            // 如果当前的term小于对端节点则立即变为follower节点并进行投票
            // 例如一个节点超时较快变成了candidate但是当前节点超时较慢
            // 目前仍然是一个follower节点
            if (requestVoteMsg.getTerm() > currentTerm.get()) {
                nodeRole = RaftRole.FOLLOWER;
                currentTerm.set(requestVoteMsg.getTerm());
                // 记录当前节点给某个任期投过票
                // 这个属性很关键raft中要保证每个任期中
                // 只会给节点投递一票的数据
                voteFor = requestVoteMsg.getTerm();
                cancelTimeoutTask(); // 取消当前状态下关联的定时任务
                registerFollowerTimeoutTask(); // 注册follower节点的定时任务
                // 返回成功的响应
                RequestVoteRes response = new RequestVoteRes();
                response.setTerm(currentTerm.get());
                response.setSuccess(true);
                rpcServer.sendMsg(nodeId, response);
                return;
            }
            // 如果当前是follower节点则根据情况进行投票
            // 如果前后有两个节点升级为了candidate则可能出现
            // 为一个节点投票后立马又来一个相同的term的candidate节点要求投票
            // voteFor来区分是否已经发生过了投票
            if (nodeRole == RaftRole.FOLLOWER && voteFor < requestVoteMsg.getTerm()) {
                RequestVoteRes response = new RequestVoteRes(); // 要比较节点之间的日志的新旧程度
                response.setTerm(currentTerm.get());
                response.setSuccess(true);
                rpcServer.sendMsg(nodeId, response);
                // 重新设置选举定时器
                cancelTimeoutTask(); // 取消当前状态下关联的定时任务
                registerFollowerTimeoutTask(); // 注册follower节点的定时任务
            } else {
                // term相同的情况下如果当前是候选者或者领导则直接拒绝
                // leader节点不会给其他节点投票(同时有两个candidate情况下会发生)
                // candidate只会给自己投票
                // 或者在当前的term下已经给集群中的一个节点投过票则拒绝再次投票
                RequestVoteRes response = new RequestVoteRes();
                response.setTerm(currentTerm.get());
                response.setSuccess(false);
                rpcServer.sendMsg(nodeId, response);
            }
        });
    }

    @Override
    public void onRequestVoteCallback(RequestVoteRes res) {
//        logger.info("receive vote callback [{}]", res);
        taskExecutor.submit(() -> {
            if (currentTerm.get() < res.getTerm()) {
                currentTerm.set(res.getTerm());
                nodeRole = RaftRole.FOLLOWER;
                cancelTimeoutTask(); // 取消当前状态下关联的定时任务
                registerFollowerTimeoutTask(); // 注册follower节点的定时任务
                return;
            }
            // 当前如果仍然是候选者节点
            // 则判断票数是否过半了
            if (res.getSuccess() == Boolean.TRUE) {
                logger.info("leader election => {},{}", currentId, currentTerm.get());
                voteCount++;
                // 如果超过了半数的选票(加1是因为加上candidate节点自身)
                if (raftGroupTable.electionSuccess(voteCount + 1)) {
                    nodeRole = RaftRole.LEADER;
                    cancelTimeoutTask(); // 取消所有的定时任务
                    registerLeaderTask(); // 注册leader节点的心跳任务
                }
            }
        });
    }
}
