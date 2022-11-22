package com.lizhengpeng.lraft.core;

import cn.hutool.core.util.StrUtil;
import com.lizhengpeng.lraft.exception.RaftCodecException;
import com.lizhengpeng.lraft.request.AppendLogMsg;
import com.lizhengpeng.lraft.request.ClientRequestMsg;
import com.lizhengpeng.lraft.request.RefreshLeaderMsg;
import com.lizhengpeng.lraft.request.RequestVoteMsg;
import com.lizhengpeng.lraft.response.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.lizhengpeng.lraft.core.MemoryLogManager.compare;
import static com.lizhengpeng.lraft.core.MemoryLogManager.great;

/**
 * raft核心算法的实现
 * @author lzp
 */
public class RaftNode implements MessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    private static final int minElectionTimeout = 1000; // 最小选举超时时间

    private static final int maxElectionTimeout = 2000; // 最大选举超时时间

    private static final int heartbeatInterval = 50; // 心跳时间间隔

    public static final int connectTimeout = 20; // 连接超时时间

    private volatile RaftRole nodeRole = RaftRole.FOLLOWER; // 当前节点的角色

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

    private LogManager logManager = new MemoryLogManager();

    private volatile String raftLeaderId;

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
        // 启动时都是follower状态
        // 较后加入集群的节点可能会立即接收到心跳消息
        // onAppendLog在接收到消息时也会创建超时定时器
        // 导致在当前registerFollowerTimeoutTask调用前创建
        // 超时任务无法使用cancelTimeoutTask方法取消任务
        // 因为当前是主线程和rpc线程是并发的关系
        // 如果不先注册定时器则可能重复创建follower超时定时器
        registerFollowerTimeoutTask();
        // startRpcServer的顺序必须在follower超时任务设置完成后
        rpcServer.startRpcServer(endpoint); // 启动rpc的服务
    }

    /**
     * 注册follower节点的超时任务
     * @return
     */
    public void registerFollowerTimeoutTask() {
        // 注意这里将异步的调用转为了同步的调用
        followerSchedule = taskExecutor.submit(() -> {
            raftLeaderId = null; // 重置leaderId
            voteCount = 0; // 重新设置当前获取的票数
            nodeRole = RaftRole.CANDIDATE; // 切换角色为候选者
            currentTerm.addAndGet(1); // term首先增加1

            // 发送投票的请求到各个节点中
            RequestVoteMsg message = new RequestVoteMsg();
            message.setNodeId(currentId);
            message.setTerm(currentTerm.get());
            // 获取最后一条日志用来选举
            // 首先比较日志的term再比较日志的index
            // term较大或者term相等但是index较大的则进行投票
            LogEntry lastLog = logManager.getLastLog();
            message.setLastLogTerm(lastLog.getTerm()); // 最后一条日志的任期和索引
            message.setLastLogIndex(lastLog.getIndex());

            logger.info("node send vote => {},{},{},{}", nodeRole.name(), currentId, currentTerm.get(), message);
            rpcServer.broadcastMsg(message); // 广播请求投票的消息

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
            raftLeaderId = null; // 重置集群leader
            voteCount = 0; // 重新设置当前获取的票数
            nodeRole = RaftRole.CANDIDATE; // 切换角色为候选者
            currentTerm.addAndGet(1); // term首先增加1

            // 发送投票的请求到各个节点中
            RequestVoteMsg message = new RequestVoteMsg();
            message.setNodeId(currentId);
            message.setTerm(currentTerm.get());
            // 获取最后一条日志用来选举
            // 首先比较日志的term再比较日志的index
            // term较大或者term相等但是index较大的则进行投票
            LogEntry lastLog = logManager.getLastLog();
            message.setLastLogTerm(lastLog.getTerm()); // 最后一条日志的任期和索引
            message.setLastLogIndex(lastLog.getIndex());

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
            // 编码要发送的消息
            try {
                List<Endpoint> endpoints = raftGroupTable.getBroadcastList(NodeId.of(currentId));
                for (Endpoint endpoint : endpoints) {
                    // 获取指定的复制进度
                    // 如果复制进度不存在则表示成员表异常
                    // ReplicateProgress成员复制进度表会在节点成为leader的时候初始化
                    ReplicateProgress progress = raftGroupTable.getReplicate(endpoint.getNodeId());
                    if (progress == null) {
                        logger.info("node => {} replicate process not found", endpoint);
                        continue; // 跳过当前节点的处理
                    }
                    // 获取对应的matchIndex的日志数据
                    // matchIndex与nextIndex最开始是相同的位置
                    // 其实这里只有根据复制进度查询日志只有两种情况
                    // (1)日志不存在
                    //    日志不存在时候可能是日志列表为空或者日志复制追赶上了leader节点
                    //    都可能不存在此时应该发送的时心跳消息
                    // (2)日志存在则进行发送并附带上一条日志的term和index以便follower根据这两个
                    //    属性来确定日志的存放位置(主要是依靠index)
                    AppendLogMsg appendLogMsg = new AppendLogMsg();
                    appendLogMsg.setLeaderId(currentId);
                    appendLogMsg.setTerm(currentTerm.get());

                    LogEntry raftLog = logManager.getLogEntry(progress.getMatchIndex());
                    if (raftLog != null) {
                        // entries为空则表示是一个心跳数据
                        // 接收到心跳消息时需要判断对应的preLogIndex|preLogTerm是否存在
                        // 如果不存在则进行回退操作
                        appendLogMsg.setLogEntry(raftLog);
                    }
                    // 获取这条日志的上一个日志
                    LogEntry preLog = logManager.getLogEntry(progress.getMatchIndex() - 1);
                    if (preLog == null) { // 整个集群中的第一条数据
                        appendLogMsg.setPreLogTerm(0L);
                        appendLogMsg.setPreLogIndex(0L);
                    } else {
                        // 上一条日志的具体的term和位置的信息
                        appendLogMsg.setPreLogTerm(preLog.getTerm());
                        appendLogMsg.setPreLogIndex(preLog.getIndex());
                    }
                    rpcServer.sendMsg(endpoint, appendLogMsg); // 发送给指定的节点数据
                    logger.info("node append log => {},{},{},{}", nodeRole.name(), currentId, currentTerm.get(), appendLogMsg);
                }
            } catch (RaftCodecException e) {
                logger.info("broadcast codec exception", e);
            }
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
            // 收到来自leader节点的心跳消息后
            // 需要记录当前raft集群的leaderId
            // 然后去具体的成员表查询leader的地址
            raftLeaderId = nodeId.getNodeId();
            if (nodeRole == RaftRole.FOLLOWER) {
                // 如果当前是简单的心跳消息则重置定时器即可
                // 否则进入日志复制的流程
                // preLogIndex|preLogTerm为0表示的是第一条数据
                // 在无日志可以复制或者复制进度从commitIndex追上了nextLogIndex的时候
                // 转为发送心跳消息不携带任何的数据
                if (Objects.isNull(appendLogMsg.getLogEntry())) { // 当前是心跳消息
                    if (compare(appendLogMsg.getPreLogTerm(), 0L) && compare(appendLogMsg.getPreLogIndex(), 0L)) {
                        // 无任何可用的消息等待同步
                        // 当前leader节点未接收到任何的有效消息
                        // 这个时候不存在日志的同步单纯就是一个心跳防止follower超时
                        // 重新设置选举定时器
                        AppendLogRes response = new AppendLogRes();
                        response.setNodeId(currentId); // leader需要根据这个标识来确定对应的节点从而更新复制进度
                        response.setTerm(currentTerm.get());
                        response.setSuccess(true);
                        rpcServer.sendMsg(nodeId, response); // 回复leader节点的心跳消息
                        cancelTimeoutTask();
                        registerFollowerTimeoutTask();
                        return;
                    } else {
                        // 这个时候有两种情况一种是节点完成了全部消息的同步
                        // 还有一种是节点刚当前leader还未进行同步
                        LogEntry logEntry = logManager.getLogEntry(appendLogMsg.getPreLogIndex());
                        AppendLogRes response = new AppendLogRes();
                        response.setNodeId(currentId); // leader需要根据这个标识来确定对应的节点从而更新复制进度
                        response.setTerm(currentTerm.get());
                        if (logEntry == null || !compare(appendLogMsg.getPreLogTerm(), logEntry.getTerm())) {
                            response.setSuccess(false);
                        } else {
                            response.setSuccess(true); // 当有新的日志时可以进行同步
                        }
                        rpcServer.sendMsg(nodeId, response); // 发送rpc消息给follower对象
                        cancelTimeoutTask();
                        registerFollowerTimeoutTask();
                    }
                } else {
                    // follower当前无条件接收
                    AppendLogRes response = new AppendLogRes();
                    response.setNodeId(currentId); // leader需要根据这个标识来确定对应的节点从而更新复制进度
                    response.setTerm(currentTerm.get());

                    // 追加从leader节点接收到的消息并进行持久化操作
                    // 如果日志匹配则添加否则返回false表示进行日志的回退操作
                    if (!logManager.replicateLog(appendLogMsg.getPreLogTerm(), appendLogMsg.getPreLogIndex(), appendLogMsg.getLogEntry())) { // 写入日志失败可能需要回退操作
                        response.setSuccess(false);
                    } else {
                        response.setSuccess(true); // 成功的写入这个时候leader根据复制进度可以提交日志了
                    }
                    rpcServer.sendMsg(nodeId, response);
                    // 重新设置选举定时器
                    cancelTimeoutTask();
                    registerFollowerTimeoutTask();
                }
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
            // 获取节点的复制进度表
            // 根据节点的复制进度来推进commit的提交或者nextLogIndex的回退操作
            ReplicateProgress progress = raftGroupTable.getReplicate(appendLogRes.getNodeId());
            if (progress == null) {
                logger.warn("append log callback get node => {} progress not found", appendLogRes.getNodeId());
                return;
            }
            // 更新客户端的nextIndex指标
            // leader的定时任务会不断的给follower/candidate发送消息
            progress.setNextIndex(logManager.getNextLogIndex());
            if (appendLogRes.getSuccess() == Boolean.FALSE) {
                progress.decrMatchIndex(); // leader节点开始回退操作
            } else {
                progress.incrMatchIndex(); // leader节点推进日志的复制
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
                // 获取当前节点与对端节点日志的新旧
                RequestVoteRes response = new RequestVoteRes();
                LogEntry lastLog = logManager.getLastLog();
                // 节点自身的最后一条日志的term比对端大或者term相等但是logIndex大的获得投票
                // 首先比较term当term相等的时候比较index
                if (great(requestVoteMsg.getLastLogTerm(), lastLog.getTerm()) ||
                        (compare(requestVoteMsg.getLastLogTerm(), lastLog.getTerm()) && great(requestVoteMsg.getLastLogIndex(), lastLog.getIndex()))) {
                    response.setTerm(currentTerm.get());
                    response.setSuccess(true);
                } else {
                    // 对端节点的日志没有自己的新则不进行投票
                    response.setTerm(currentTerm.get());
                    response.setSuccess(false);
                }
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
                    nodeRole = RaftRole.LEADER; // 节点选举成功变成了leader
                    // 写入一条NoOp的日志
                    logManager.appendLog(currentTerm.get(), "LEADER_ELECTION_NOOP");
                    raftGroupTable.initReplicateProgress(logManager.getNextLogIndex()); // 初始化节点的复制进度表(始终指向leader最后一条日志的下一个位置)
                    cancelTimeoutTask(); // 取消所有的定时任务
                    registerLeaderTask(); // 注册leader节点的心跳任务
                }
            }
        });
    }

    @Override
    public void onLeaderAppendLog(ClientRequestMsg clientRequestMsg, RpcClient rpcClient) {
        taskExecutor.submit(() -> {
            // 如果当前不是leader节点则直接返回
            // raft是强领导者模型任何的写入都需要通过leader节点
            // Follower\Candidate节点只能接收来自leader节点
            // 的日志进行数据同步
            // 这个地方写入其实也没问题最终还是会通过leader节点进行日志的同步
            // 这样直接过滤掉等于省去一次回退日志的rpc调用
            if (nodeRole != RaftRole.LEADER) {
                // 如果当前不存在leader节点
                // 可能正在选举或者发生了leaderShip
                if (StrUtil.isBlank(raftLeaderId)) {
                    RedirectRes refreshRes = new RedirectRes();
                    refreshRes.setRedirect(Boolean.FALSE);
                    refreshRes.setReason("there is currently no leader");
                    rpcClient.sendMessage(refreshRes);
                } else {
                    // 获取leader节点的地址
                    Endpoint endpoint = raftGroupTable.getEndpoint(NodeId.of(raftLeaderId));
                    if (endpoint == null) {
                        logger.info("refresh leader => {} endpoint not found", NodeId.of(raftLeaderId));
                        // 返回错误响应
                        RedirectRes redirectRes = new RedirectRes();
                        redirectRes.setRedirect(Boolean.FALSE);
                        redirectRes.setReason("leader endpoint not found");
                        rpcClient.sendMessage(redirectRes);
                    } else {
                        // 返回leader节点的地址
                        RedirectRes redirectRes = new RedirectRes();
                        redirectRes.setRedirect(Boolean.TRUE);
                        redirectRes.setReason("refresh leader success");
                        redirectRes.setEndpoint(endpoint);
                        rpcClient.sendMessage(redirectRes);
                    }
                }
            } else {
                try {
                    // 写入日志到存储中
                    logger.info("leader received client msg => {}", clientRequestMsg.getMsg());
                    logManager.appendLog(currentTerm.get(), clientRequestMsg.getMsg());
                    // 返回成功的消息
                    ClientRequestRes res = new ClientRequestRes();
                    res.setReason("rpc cluster append log success");
                    rpcClient.sendMessage(res);
                } catch (Exception e) {
                    logger.info("write leader log exception", e);
                }
            }
        });
    }

    @Override
    public void onRefreshLeader(RefreshLeaderMsg refreshLeaderMsg, RpcClient rpcClient) {
        taskExecutor.submit(() -> {
            String leaderId = raftLeaderId;
            if (nodeRole == RaftRole.LEADER) {
                leaderId = currentId;
            }
            // 如果当前不存在leader节点
            if (StrUtil.isBlank(leaderId)) {
                // 返回错误响应
                RefreshLeaderRes refreshRes = new RefreshLeaderRes();
                refreshRes.setRefreshed(Boolean.FALSE);
                refreshRes.setErrorMsg("there is currently no leader");
                rpcClient.sendMessage(refreshRes);
                return;
            }
            // 获取leader节点的地址
            Endpoint endpoint = raftGroupTable.getEndpoint(NodeId.of(leaderId));
            if (endpoint == null) {
                logger.info("refresh leader => {} endpoint not found", NodeId.of(leaderId));
                // 返回错误响应
                RefreshLeaderRes refreshRes = new RefreshLeaderRes();
                refreshRes.setRefreshed(Boolean.FALSE);
                refreshRes.setErrorMsg("leader endpoint not found");
                rpcClient.sendMessage(refreshRes);
            } else {
                // 返回leader节点的地址
                RefreshLeaderRes refreshRes = new RefreshLeaderRes();
                refreshRes.setRefreshed(Boolean.TRUE);
                refreshRes.setErrorMsg("refresh leader success");
                refreshRes.setEndpoint(endpoint);
                rpcClient.sendMessage(refreshRes);
            }
        });
    }
}
