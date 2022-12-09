package com.lizhengpeng.lraft.core;

import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import com.lizhengpeng.lraft.exception.RaftCodecException;
import com.lizhengpeng.lraft.exception.RaftException;
import com.lizhengpeng.lraft.request.*;
import com.lizhengpeng.lraft.response.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * raft核心算法的实现
 * @author lzp
 */
public class RaftNode implements MessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    private volatile RaftRole nodeRole = RaftRole.FOLLOWER; // 当前节点的角色

    private String currentId;

    private volatile long voteCount; // 当前候选者节点得到的票数

    private RaftGroupTable raftGroupTable = new RaftGroupTable(); // 集群成员表

    private RpcServer rpcServer;

    private TaskExecutor taskExecutor = new TaskExecutor();

    private ScheduledFuture<?> electionTimeoutSchedule;

    private ScheduledFuture<?> replicateLogSchedule;

    private ScheduledFuture<?> leaderDownSchedule; // leader节点一定时间内未收到follower节点的回复则可能follower宕机或者网络出现异常

    private ScheduledFuture<?> snapshotTask;

    private SecureRandom secureRandom = new SecureRandom();

    private FileLogManager logManager;

    private volatile String raftLeaderId;

    private Set<NodeId> heartbeatBox = new HashSet<>();

    private RaftOptions raftOptions; // raft相关的配置

    private volatile RaftMeta raftMeta; // raft相关元数据

    private StateMachine stateMachine; // 状态机的定义

    private Snapshot snapshot;

    private volatile long lastApplied; // 状态机应用的最后一条日志的索引

    private Lock appendLogLock = new ReentrantLock();

    private Condition appendLogCondition = appendLogLock.newCondition();


    /**
     * 默认的构造函数初始化相关的组件
     * @param raftOptions
     */
    public RaftNode(RaftOptions raftOptions, StateMachine stateMachine) {
        this.raftOptions = raftOptions;
        this.logManager = new FileLogManager(raftOptions);
        this.snapshot = new Snapshot(raftOptions.getLogDir());
        this.stateMachine = stateMachine;
        this.raftMeta = this.reloadRaftMeta();
        rpcServer = new RpcServer(raftOptions);
//        registerSnapshotTask(); // 执行固定的创建snapshot任务
        applyStateMachine(reloadRaftMeta().getCommittedIndex()); // 应用当前的日志到状态机中
    }

    /**
     * 获取raft相关的元数据
     * @return
     */
    public synchronized RaftMeta reloadRaftMeta() {
        return raftMeta = logManager.reloadRaftMeta();
    }

    /**
     * 更新元数据信息
     */
    public synchronized void updateRaftMeta(RaftMeta raftMeta) {
        logManager.updateRaftMeta(raftMeta);
    }

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
     * 获取leader节点当前的任期
     * @return
     */
    public long getCurrentTerm() {
        return raftMeta.getCurrentTerm();
    }

    /**
     * 获取随机超时时间
     * @return
     */
    public int randomTimeOut() {
        return raftOptions.getMinElectionTimeout() + secureRandom.nextInt(raftOptions.getMaxElectionTimeout() - raftOptions.getMinElectionTimeout());
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
     * follower超时后会增加自己的任期并转为candidate角色
     * 然后向集群中其他节点发起投票请求
     */
    public void registerFollowerTimeoutTask() {
        nodeRole = RaftRole.FOLLOWER; // 切换角色为候选者
        registerElectionTimeoutTask(); // follower节点超时后会变成candidate并发起投票
    }

    /**
     * 注册follower节点的超时任务
     * @return
     */
    public void registerElectionTimeoutTask() {
        // 注意这里将异步的调用转为了同步的调用
        electionTimeoutSchedule = taskExecutor.submit(() -> {
            try {
                raftLeaderId = null; // 重置leaderId
                voteCount = 0; // 重新设置当前获取的票数
                nodeRole = RaftRole.CANDIDATE; // 切换角色为候选者

                // 增加当前的任期数据
                reloadRaftMeta();
                raftMeta.setCurrentTerm(raftMeta.getCurrentTerm() + 1);
                // candidate只会给自己进行投票
                raftMeta.setVoteFor("");
                updateRaftMeta(raftMeta);

                // 发送投票的请求到各个节点中
                RequestVoteMsg voteMsg = new RequestVoteMsg();
                voteMsg.setNodeId(currentId);
                voteMsg.setTerm(raftMeta.getCurrentTerm());
                // 获取最后一条日志用来选举
                // 首先比较日志的term再比较日志的index
                // term较大或者term相等但是index较大的则进行投票
                LogEntry lastLog = logManager.getLastLog();
                voteMsg.setLastLogTerm(lastLog.getTerm()); // 最后一条日志的任期和索引
                voteMsg.setLastLogIndex(lastLog.getIndex());

                logger.debug("send vote => {},{},{},{}", nodeRole, currentId, raftMeta.getCurrentTerm(), voteMsg);
                rpcServer.broadcastMsg(voteMsg); // 广播请求投票的消息

                // 变为candidate后需要设置候选者定时器
                // 如果发生了平票或者网络分区当前没有选举出leader节点
                // 则需要继续进行选举操作直到选出leader节点
                registerElectionTimeoutTask();
            } catch (Exception e) {
                logger.error("election timeout task execute exception", e);
            }
        }, randomTimeOut());
    }

    @Override
    public void onRequestVote(NodeId nodeId, RequestVoteMsg requestVoteMsg) {
        taskExecutor.submit(() -> {
            try {
                logger.debug("receive vote request {},{}", nodeId, requestVoteMsg);
                // 请求投票的节点的term小于当前的节点则返回当前节点的任期给对端节点
                if (requestVoteMsg.getTerm() < reloadRaftMeta().getCurrentTerm()) {
                    RequestVoteRes response = new RequestVoteRes();
                    response.setTerm(raftMeta.getCurrentTerm());
                    response.setSuccess(false);
                    rpcServer.sendMsg(nodeId, response);
                    return;
                }
                // 如果当前的term小于对端节点则立即变为follower节点并进行投票(无条件进行投票!!!!)
                // 例如一个节点超时较快变成了candidate但是当前节点超时较慢
                // 目前仍然是一个follower节点
                if (requestVoteMsg.getTerm() > reloadRaftMeta().getCurrentTerm()) {
                    // 更新自己的任期与对端节点同步
                    raftMeta.setCurrentTerm(requestVoteMsg.getTerm());
                    // 记录当前节点给某个任期投过票
                    // 这个属性很关键raft中要保证每个任期中只会给节点投递一票的数据
                    // raft中只有follower或这当遇到term比自己大的投票请求时才会进行投票
                    raftMeta.setVoteFor(nodeId.getNodeId());
                    updateRaftMeta(raftMeta);

                    // 当前节点成为follower节点
                    cancelTimeoutTask(); // 取消当前状态下关联的定时任务
                    registerFollowerTimeoutTask(); // 注册follower节点的定时任务

                    // 返回成功的响应
                    RequestVoteRes response = new RequestVoteRes();
                    response.setTerm(raftMeta.getCurrentTerm());
                    response.setSuccess(true);
                    rpcServer.sendMsg(nodeId, response);
                    return;
                }
                // 如果当前是follower节点则根据情况进行投票
                // 如果前后有两个节点升级为了candidate则可能出现
                // 为一个节点投票后立马又来一个相同的term的candidate节点要求投票
                // voteFor来区分是否已经发生过了投票
                // voteFor不为空其实表示在当前term中节点已经进行过一次投票
                // raft中规定在整个term中只允许一票制!!!!!(voteFor其实不一定要保存节点的Id其实仅仅设置一个已经投过票的标识即可)
                if (nodeRole == RaftRole.FOLLOWER && StrUtil.isBlank(reloadRaftMeta().getVoteFor())) {
                    // 获取当前节点与对端节点日志的新旧
                    RequestVoteRes response = new RequestVoteRes();
                    LogEntry lastLog = logManager.getLastLog();
                    // 节点自身的最后一条日志的term比对端大或者term相等但是logIndex大的获得投票
                    // 首先比较term当term相等的时候比较index
                    if (requestVoteMsg.getLastLogTerm() > lastLog.getTerm() ||
                            (requestVoteMsg.getLastLogTerm() == lastLog.getTerm() && requestVoteMsg.getLastLogIndex() >= lastLog.getIndex())) {
                        // 记录当前的投票的信息
                        reloadRaftMeta().setVoteFor(nodeId.getNodeId());
                        updateRaftMeta(raftMeta);
                        response.setTerm(reloadRaftMeta().getCurrentTerm());
                        response.setSuccess(true);
                    } else {
                        // 对端节点的日志没有自己的新则不进行投票
                        response.setTerm(reloadRaftMeta().getCurrentTerm());
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
                    response.setTerm(reloadRaftMeta().getCurrentTerm());
                    response.setSuccess(false);
                    rpcServer.sendMsg(nodeId, response);
                }
            } catch (Exception e) {
                logger.error("request vote handler exception", e);
            }
        });
    }

    @Override
    public void onRequestVoteCallback(RequestVoteRes voteRes) {
        logger.debug("receive vote response => {}", voteRes);
        taskExecutor.submit(() -> {
            try {
                // 如果当前不是candidate节点则直接忽略
                // 可能是节点错误发送也可能是当前节点之前是candidate节点并且发起了投票但是对端节点网络较慢
                // 当前节点状态变化后才接收到数据
                if (nodeRole != RaftRole.CANDIDATE) {
                    logger.warn("receive vote callback => {} but current node non candidate role", voteRes);
                    return;
                }
                // 如果任期比自己小则进行忽略可能节点网络较慢
                // 收到了自己在上一个任期发送的投票请求
                if (reloadRaftMeta().getCurrentTerm() > voteRes.getTerm()) {
                    logger.warn("receive vote callback term => {} less than current term => {} ignore response", voteRes.getTerm(), raftMeta.getCurrentTerm());
                    return;
                }
                // 判断term是否一致
                if (reloadRaftMeta().getCurrentTerm() < voteRes.getTerm()) {
                    // 更新当前的任期数据
                    raftMeta.setCurrentTerm(voteRes.getTerm());
                    updateRaftMeta(raftMeta);

                    cancelTimeoutTask(); // 取消当前状态下关联的定时任务
                    registerFollowerTimeoutTask(); // 注册follower节点的定时任务
                    return;
                }
                // 如果获得的票数过半则当前节点成功竞选leader
                if (voteRes.getSuccess() == Boolean.TRUE) {
                    voteCount++;
                    // 如果超过了半数的选票(加1是因为加上candidate节点自身的一票数据)
                    if (raftGroupTable.electionSuccess(voteCount + 1)) {
                        logger.debug("current node success election => {},{}", currentId, raftMeta.getCurrentTerm());
                        nodeRole = RaftRole.LEADER; // 节点选举成功变成了leader
                        raftLeaderId = currentId; // 当前节点成功竞选

                        // 写入一条NoOp的日志用来快速提交整个集群的日志
                        logManager.appendLog("LEADER_ELECTION_NOOP");
                        raftGroupTable.initReplicateProgress(reloadRaftMeta().getLastLogIndex() + 1); // 初始化节点的复制进度表(始终指向leader最后一条日志的下一个位置)

                        cancelTimeoutTask(); // 取消所有的定时任务
                        registerReplicateLogTask(); // 注册leader节点的心跳任务
                        registerLeaderDownSchedule(); // 注册leadership超时任务
                    }
                }
            } catch (Exception e) {
                logger.error("request vote response handler exception", e);
            }
        });
    }

    /**
     * 注册follower节点的超时任务
     * @return
     */
    public void registerLeaderDownSchedule() {
        heartbeatBox.clear(); // 清空接收到心跳的节点的Id列表
        // 注意这里将异步的调用转为了同步的调用
        leaderDownSchedule = taskExecutor.submit(() -> {
            try {
                // 判断当前是否是leader节点如果不是则不需要进行执行
                if (nodeRole != RaftRole.LEADER) {
                    logger.warn("current node role => {} cancel leader down task execute", nodeRole);
                    return;
                }
                // 在一个随机超时的时间内leader没有收到follower/candidate的心跳消息
                // 并且没有收到一个term比自己大的投票或者新leader的心跳
                // 则有可能当前出现了网络分区或者follower/candidate发生了宕机
                // 这个时候集群其实已经没有了过半的服务器需要停止写入支持
                // 退化成leader也可以
                if (!raftGroupTable.greatHalf(heartbeatBox.size())) {
                    logger.warn("raft happen leader down !!!");
                    // 节点变成了follower节点停止写入支持
                    // 重新注册follower节点的定时器
                    cancelTimeoutTask();
                    // 清空当前leader节点的Id
                    raftLeaderId = null;
                    registerFollowerTimeoutTask();
                } else {
                    // 重复的注册
                    registerLeaderDownSchedule();
                }
            } catch (Exception e) {
                logger.error("leader down check task exception", e);
            }
        }, raftOptions.getHeartbeatInterval() * 10); // 10个心跳周期内未收到heartbeat心跳回复则退化成follower
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
                logger.debug("cancel timeout task exception", e);
            }
        }
    }

    /**
     * 取消当前已经关联的所有任务
     */
    public void cancelTimeoutTask() {
        cancelTimeoutTask(electionTimeoutSchedule);
        cancelTimeoutTask(replicateLogSchedule);
        cancelTimeoutTask(leaderDownSchedule);
    }

    /**
     * 注册follower节点的超时任务
     * @return
     */
    public void registerReplicateLogTask() {
        // 注意这里将异步的调用转为了同步的调用
        replicateLogSchedule = taskExecutor.submit(() -> {
            try {
                // 判断当前是否是leader节点只有leader节点才需要进行日志的复制
                if (nodeRole != RaftRole.LEADER) {
                    logger.warn("current node role => {} so cancel replicate log task execute", nodeRole);
                    return;
                }
                List<Endpoint> endpoints = raftGroupTable.getBroadcastList(NodeId.of(currentId));
                for (Endpoint endpoint : endpoints) {
                    // 获取指定的复制进度
                    // 如果复制进度不存在则表示成员表异常
                    // ReplicateProgress成员复制进度表会在节点成为leader的时候初始化
                    ReplicateProgress progress = raftGroupTable.getReplicate(endpoint.getNodeId());
                    if (progress == null) {
                        logger.warn("endpoint => {} replicate process not found", endpoint);
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
                    appendLogMsg.setTerm(reloadRaftMeta().getCurrentTerm());
                    appendLogMsg.setLastCommitted(reloadRaftMeta().getCommittedIndex()); // 当前可以提交的索引的位置

                    LogEntry raftLog = logManager.getLogEntry(progress.getMatchIndex());
                    if (raftLog != null) {
                        // entries为空则表示是一个心跳数据
                        // 接收到心跳消息时需要判断对应的preLogIndex|preLogTerm是否存在
                        // 如果不存在则进行回退操作
                        appendLogMsg.setLogEntry(raftLog);
                    }
                    // 获取这条日志的上一个日志
                    LogEntry preLog = logManager.getLogEntry(progress.getMatchIndex() - 1);
                    if (preLog == null) { // 整个集群中的第一条数据 todo 加入了日志快照后这里需要进行修改
                        appendLogMsg.setPreLogTerm(0L);
                        appendLogMsg.setPreLogIndex(0L);
                    } else {
                        // 上一条日志的具体的term和位置的信息
                        appendLogMsg.setPreLogTerm(preLog.getTerm());
                        appendLogMsg.setPreLogIndex(preLog.getIndex());
                    }
                    rpcServer.sendMsg(endpoint, appendLogMsg); // 发送给指定的节点数据
                    logger.debug("send append log to => {},{}", endpoint, appendLogMsg);
                }
            } catch (RaftCodecException e) {
                logger.debug("replicate log occur codec exception", e);
            } catch (Exception e) {
                logger.debug("replicate log task exception", e);
            }
            // 重复的发送数据
            registerReplicateLogTask();
        }, raftOptions.getHeartbeatInterval());
    }

    @Override
    public void onAppendLog(NodeId nodeId, AppendLogMsg appendLogMsg) {
        taskExecutor.submit(() -> {
            try {
                logger.debug("receive append log from => {} msg => {}", nodeId, appendLogMsg);
                // term比自己小的时候返回自身的
                // 以便于对端退化成follower节点
                if (appendLogMsg.getTerm() < reloadRaftMeta().getCurrentTerm()) {
                    AppendLogRes response = new AppendLogRes();
                    response.setTerm(raftMeta.getCurrentTerm());
                    response.setSuccess(false);
                    rpcServer.sendMsg(nodeId, response);
                    return;
                }
                // 当前节点的term小于对端的时候
                // 退化成follower节点进行跟随
                if (appendLogMsg.getTerm() > reloadRaftMeta().getCurrentTerm()) {
                    raftMeta.setCurrentTerm(appendLogMsg.getTerm());
                    updateRaftMeta(raftMeta);
                    cancelTimeoutTask();
                    registerFollowerTimeoutTask();
                    return;
                }
                // 这里应该不会发生如果同时存在了两个leader节点
                // 则代表集群发生了脑裂致命的错误
                if (nodeRole == RaftRole.LEADER) {
                    throw new RaftException("raft cluster found two leader, fatal error!!!");
                }
                // 收到来自leader节点的心跳消息后
                // 需要记录当前raft集群的leaderId
                // 然后去具体的成员表查询leader的地址
                raftLeaderId = nodeId.getNodeId();
                if (nodeRole == RaftRole.FOLLOWER) {
                    // 更新当前集群中可以提交的索引
                    reloadRaftMeta().setCommittedIndex(appendLogMsg.getLastCommitted()); // 持久化committedIndex
                    updateRaftMeta(raftMeta);

                    // 如果当前是简单的心跳消息则重置定时器即可
                    // 否则进入日志复制的流程
                    // preLogIndex|preLogTerm为0表示的是第一条数据
                    // 在无日志可以复制或者复制进度从commitIndex追上了nextLogIndex的时候
                    // 转为发送心跳消息不携带任何的数据
                    if (Objects.isNull(appendLogMsg.getLogEntry())) {
                        AppendLogRes response = new AppendLogRes();
                        response.setNodeId(currentId); // leader需要根据这个标识来确定对应的节点从而更新复制进度
                        response.setTerm(raftMeta.getCurrentTerm());
                        // 当前集群中不存在任何的日志数据
                        // 单纯就是一个心跳消息
                        if (appendLogMsg.getPreLogTerm() == 0 && appendLogMsg.getPreLogIndex() == 0) {
                            // 无任何可用的消息等待同步
                            // 当前leader节点未接收到任何的有效消息
                            // 这个时候不存在日志的同步单纯就是一个心跳防止follower超时
                            // 重新设置选举定时器
                            response.setSuccess(true);
                            response.setLogIndex(-1); // 当前是心跳消息并没有收到实际的日志数据
                        } else {
                            // 这个时候有两种情况一种是节点完成了全部消息的同步
                            // 还有一种是节点刚当前leader还未进行同步
                            // 如果preLogIndex和preLogTerm不匹配则需要进行回退操作
                            LogEntry logEntry = logManager.getLogEntry(appendLogMsg.getPreLogIndex());
                            if (logEntry == null || appendLogMsg.getPreLogTerm() != logEntry.getTerm()) {
                                response.setSuccess(false); // 日志不匹配进行回退操作直到找到一个匹配的位置或者达到第一个日志
                                response.setLogIndex(-1); // 当前是心跳消息并没有收到实际的日志数据
                            } else {
                                // 对于follower来说可引用到状态的日志为
                                // 实际写入的日志数量和committedIndex的最小值
                                applyStateMachine(Math.min(raftMeta.getLastLogIndex(), raftMeta.getCommittedIndex()));
                                response.setSuccess(true); // 当有新的日志时可以进行同步
                                response.setLogIndex(appendLogMsg.getPreLogIndex()); // 当前最后实际复制的日志索引其实是上一个
                            }
                        }
                        // 回复leader节点的心跳消息
                        // leader会根据响应来判断follower是否宕机
                        rpcServer.sendMsg(nodeId, response);
                        cancelTimeoutTask(); // 取消定时任务
                        registerFollowerTimeoutTask(); // 重新注册follower超时任务
                    } else {
                        // follower当前无条件接收
                        AppendLogRes response = new AppendLogRes();
                        response.setNodeId(currentId); // leader需要根据这个标识来确定对应的节点从而更新复制进度
                        response.setTerm(reloadRaftMeta().getCurrentTerm());

                        // 追加从leader节点接收到的消息并进行持久化操作
                        // 如果日志匹配则添加否则返回false表示进行日志的回退操作
                        if (!logManager.replicateLog(appendLogMsg.getPreLogTerm(), appendLogMsg.getPreLogIndex(), appendLogMsg.getLogEntry())) { // 写入日志失败可能需要回退操作
                            response.setSuccess(false);
                            response.setLogIndex(-1l);
                        } else {
                            // 对于follower来说可引用到状态的日志为
                            // 实际写入的日志数量和committedIndex的最小值
                            applyStateMachine(Math.min(raftMeta.getLastLogIndex(), raftMeta.getCommittedIndex()));
                            response.setSuccess(true); // 成功的写入这个时候leader根据复制进度可以提交日志了
                            response.setLogIndex(appendLogMsg.getLogEntry().getIndex()); // 当前实际复制的日志的索引
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
                    // 重新注册follower节点的定时器
                    cancelTimeoutTask();
                    registerFollowerTimeoutTask();
                }
            } catch (Exception e) {
                logger.error("append log exception", e);
            }
        });
    }

    /**
     * 应用客户端的命令到状态机
     * @param committedIndex
     */
    public synchronized void applyStateMachine(long committedIndex) { // todo 可以优化成异步的应用状态机
        if (lastApplied >= committedIndex) { // lastApplied默认值为0从0开始增长
            return;
        }
        long index = lastApplied + 1;
        for (; index <= committedIndex; index++) {
            LogEntry logEntry = logManager.getLogEntry(index);
            if (logEntry == null) { // 为空时不进行引用
                logger.warn("apply index => {} failed, because log is empty", index);
                continue;
            }
            stateMachine.apply(logEntry.getEntries(), index); // 传入状态机数据和对应的索引
        }
        // raft启动的时候会使用日志来初始化整个状态机
        // lastApplied随着应用到的日志索引增长
        lastApplied = committedIndex;
    }

    @Override
    public void onAppendLogCallback(AppendLogRes appendLogRes) {
        taskExecutor.submit(() -> {
            try {
                logger.debug("node receive append log response => {}", appendLogRes);
                // 首先获取锁对象这里获取锁对象主要是为了唤醒等待写入成功响应的客户端线程
                // 客户端写入后应该在日志被复制到大部分节点的时候返回写入成功或者写入超时返回失败
                appendLogLock.lock();
                // 如果当前不是leader节点则直接忽略
                // 情况同节点接收到投票反阔响应的时候相同
                if (nodeRole != RaftRole.LEADER) {
                    logger.warn("receive append log callback => {} response but node non leader role", appendLogRes);
                    return;
                }
                // 节点term小于当前则直接忽略
                // 必须相同的时候才进行处理
                if (appendLogRes.getTerm() < reloadRaftMeta().getCurrentTerm()) {
                    logger.warn("receive append log term => {} less than current term => {} ignore response", appendLogRes.getTerm(), raftMeta.getCurrentTerm());
                    return;
                }
                // 当前的leader节点退化成follower节点并等待心跳
                if (appendLogRes.getTerm() > reloadRaftMeta().getCurrentTerm()) {
                    reloadRaftMeta().setCurrentTerm(appendLogRes.getTerm());
                    updateRaftMeta(raftMeta);

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
                // 这部分代码用来计算节点的复制百分比进度
                double percent= (double) progress.getMatchIndex() / progress.getNextIndex();
                String percentFormat = NumberUtil.decimalFormat("#.##%", percent);
                logger.debug("node => {} replicate progress matchIndex => {} nextIndex => {} percent => {}", NodeId.of(appendLogRes.getNodeId()), progress.getMatchIndex(), progress.getNextIndex(), percentFormat);

                // 收到leader节点的心跳消息
                // 则更新heartbeatBox用来判断当前集群是否正常的工作
                // 目的是为了让leader节点可以感知到和follower节点的
                // 通信是否正常
                heartbeatBox.add(NodeId.of(appendLogRes.getNodeId()));
                // 更新客户端的nextIndex指标
                // leader的定时任务会不断的给follower/candidate发送消息
                progress.setNextIndex(reloadRaftMeta().getLastLogIndex() + 1); // lastLogIndex为实际写入的最后一条日志的索引
                // 日志被复制过半后
                // 触发对应的回调通知客户端对象
                // 集群稳定后nextIndex和matchIndex相等并且指向下一条待写入的日志索引
                // 这个时候其实发送的是空的心跳数据但是success仍然返回true标识
                // 并且在节点初次变为leader的时候都将matchIndex和nextIndex设置为相同的lastLogIndex+1
                // 此时replicateGreatHalf一定是返回的true
                if (appendLogRes.getSuccess() == Boolean.TRUE) {
                    // 日志过半则可以进行提交进行对应matchIndex日志的提交 这里应该分别统计每个节点
                    // >= progress.getLastLog是防止对端处理过慢收到了上一个同步消息的响应
                    if (appendLogRes.getLogIndex() != -1 && appendLogRes.getLogIndex() >= progress.getLastLog() && appendLogRes.getLogIndex() > reloadRaftMeta().getCommittedIndex()) {
                        // appendLogRes.logIndex为实际复制到的日志的索引
                        // 遍历对端计算当前可以提交的索引的位置
                        long replicateIndex = appendLogRes.getLogIndex();
                        progress.setLastLog(replicateIndex); // 更新节点当前实际复制的索引位置

                        // 遍历所有的复制进度判断是否可以提交
                        List<ReplicateProgress> progresses = raftGroupTable.getReplicateProgress(NodeId.of(currentId));
                        long count = progresses.stream()
                                .map(ReplicateProgress::getLastLog)
                                .filter(item -> item >= replicateIndex)
                                .reduce(0l, Long::sum);
                        // 如果过半的集群都已经复制了并且日志所属当前的term则进行实际的提交操作
                        if (raftGroupTable.replicateGreatHalf(count + 1)) {
                            // 判断日志的term是否是当前的任期
                            LogEntry logEntry = logManager.getLogEntry(replicateIndex);
                            if (logEntry == null) { // 通常这种情况是不会发生的
                                logger.warn("replicate log committed check failed, node get index => {} log failed", replicateIndex);
                                throw new RaftException("replicate log committed check failed");
                            } else {
                                // term相同可以进行提交操作
                                if (logEntry.getTerm() == reloadRaftMeta().getCurrentTerm()) {
                                    raftMeta.setCommittedIndex(replicateIndex);
                                    updateRaftMeta(raftMeta);
                                    // 日志提交之后可以立即运行状态机进行日志的应用
                                    applyStateMachine(replicateIndex);
                                } else {
                                    // term不相同的不允许提交否则会造成日志的丢失
                                    logger.warn("replicate log committed check failed, log term mismatching, log term => {}, current term => {}", logEntry.getTerm(), raftMeta.getCurrentTerm());
                                }
                            }
                        }
                    }
                    // leader节点推进日志的复制
                    progress.incrMatchIndex();
                } else {
                    progress.decrMatchIndex(); // leader节点开始回退操作
//                    // 如果回退的进度小于第一条日志则需要发送快照
//                    if (progress.getMatchIndex() < raftMeta.getFirstLogIndex()) {
//                        progress.setSnapshotOffset(0L); // 从位置0开始进行日志复制
//                    }
                }
                // 唤醒正在等待写入成功响应的客户端线程
                // 唤醒队列
                appendLogCondition.signalAll();
            } catch (Exception e) {
                logger.error("append log callback exception", e);
            } finally {
                appendLogLock.unlock();
            }
        });
    }

    @Override
    public void onInstallSnapshot(NodeId nodeId, InstallSnapshotMsg installSnapshotMsg) {
        taskExecutor.submit(() -> {
            logger.debug("node receive install snapshot {},{}", nodeId, installSnapshotMsg);
            if (installSnapshotMsg.getTerm() < raftMeta.getCurrentTerm()) {
                InstallSnapshotRes response = new InstallSnapshotRes();
                response.setTerm(raftMeta.getCurrentTerm());
                response.setSuccess(false);
                rpcServer.sendMsg(nodeId, response);
                return;
            }
            if (installSnapshotMsg.getTerm() > raftMeta.getCurrentTerm()) {
                raftMeta.setCurrentTerm(installSnapshotMsg.getTerm()); // 持久化meta消息
                nodeRole = RaftRole.FOLLOWER;
//                updateRaftMeta();
                // 重新注册follower节点的定时器
                cancelTimeoutTask();
                registerFollowerTimeoutTask();
                return;
            }
            if (nodeRole == RaftRole.LEADER) {
                logger.error("install snapshot raft cluster found two leader, fatal error!!!");
                return;
            }
            // 收到来自leader节点的心跳消息后
            // 需要记录当前raft集群的leaderId
            // 然后去具体的成员表查询leader的地址
            raftLeaderId = nodeId.getNodeId();
            if (nodeRole == RaftRole.FOLLOWER) {
                logger.info("receive install snapshot => {}", installSnapshotMsg);
                InstallSnapshotRes response = new InstallSnapshotRes(); // 返回接收成功的响应
                response.setTerm(raftMeta.getCurrentTerm());
                response.setNodeId(currentId);
                response.setSuccess(true);
                // 默认直接接收数据
                if (installSnapshotMsg.getOffset() == 0) {
                    if (snapshot.getLocalWriter() != null) { // 首先释放资源
                        snapshot.getLocalWriter().release();
                    } else { // 创建一个本地快照的写入
                        snapshot.setLocalWriter(snapshot.getSnapshotWriter());
                    }
                }
                // 写入快照数据
                // 判断是否是最后一个数据
                if (installSnapshotMsg.getLastPart() == Boolean.TRUE) {
                    // 写入快照持久化
                    SnapshotMeta snapshotMeta = SnapshotMeta.builder()
                            .lastLogIndex(installSnapshotMsg.getLastLogIndex())
                            .lastLogTerm(installSnapshotMsg.getLastLogTerm())
                            .build();
                    snapshot.getLocalWriter().release();
                    snapshot.getLocalWriter().writeSnapshotMeta(snapshotMeta);
                    snapshot.reloadSnapshot();
                    // 应用状态机快照数据
                    stateMachine.readSnapshot(snapshot);
                    lastApplied = snapshotMeta.getLastLogIndex(); // 最后应用的日志索引位置
                    // 更新第一条索引的位置
                    raftMeta.setFirstLogIndex(lastApplied + 1);
//                    updateRaftMeta(); // 持久化raft元数据
                } else {
                    // 在当前的流基础上继续进行写入
                    snapshot.getLocalWriter().write(installSnapshotMsg.getData().getBytes(StandardCharsets.UTF_8));
                }
                rpcServer.sendMsg(nodeId, response);
                // 重新设置选举定时器
                cancelTimeoutTask();
                registerFollowerTimeoutTask();
            } else {
                // candidate节点需要退化成follower节点
                // candidate节点目前不需要返回数据
                // 等待下次收到心跳时在follower节点时候再进行处理
                nodeRole = RaftRole.FOLLOWER;
                raftMeta.setCurrentTerm(installSnapshotMsg.getTerm());
//                updateRaftMeta();
                // 重新注册follower节点的定时器
                cancelTimeoutTask();
                registerFollowerTimeoutTask();
            }
        });
    }

    @Override
    public void onInstallSnapshotCallback(InstallSnapshotRes installSnapshotRes) {
        taskExecutor.submit(() -> {
            logger.debug("node receive install snapshot response {}", installSnapshotRes);
            if (installSnapshotRes.getTerm() > raftMeta.getCurrentTerm()) {
                // 当前的leader节点退化成follower节点并等待心跳
                nodeRole = RaftRole.FOLLOWER;
                raftMeta.setCurrentTerm(installSnapshotRes.getTerm());
//                updateRaftMeta();
                // 重新注册follower节点的定时器
                cancelTimeoutTask();
                registerFollowerTimeoutTask();
                return;
            }
            // 获取节点的复制进度表
            // 根据节点的复制进度来推进commit的提交或者nextLogIndex的回退操作
            ReplicateProgress progress = raftGroupTable.getReplicate(installSnapshotRes.getNodeId());
            if (progress == null) {
                logger.warn("append log callback get node => {} progress not found", installSnapshotRes.getNodeId());
                return;
            }
            if (installSnapshotRes.getSuccess() == Boolean.TRUE) { // 快照未发送完成则继续按照当前的offset进行发送
                // 增加复制的偏移量
                progress.setSnapshotOffset(progress.getSnapshotOffset() + progress.getLastReadLength());
                // 如果发送完整则正常的开始复制过程
                if (progress.getLastReadLength() == 0) { // 正常的开始复制流程
                    progress.setNextIndex(raftMeta.getLastLogIndex() + 1);
                    progress.setMatchIndex(raftMeta.getLastLogIndex() + 1);
                }
            } else {
                // todo 这里需要优化
                // 如果发送失败了则重置offset
                progress.setSnapshotOffset(0l);
            }
        });
    }

    /**
     * leader节点接收到客户端的写入请求触发
     * @param clientRequestMsg
     * @param rpcClient
     */
    @Override
    public void onLeaderAppendLog(ClientRequestMsg clientRequestMsg, RpcClient rpcClient) {
        rpcClient.sendMessage(this.appendLogToLeader(clientRequestMsg));
    }

    /**
     * 将日志写入到客户端中
     * @param clientRequestMsg
     * @return
     */
    public AppendResult appendLogToLeader(ClientRequestMsg clientRequestMsg) {
        AppendResult result = new AppendResult();
        result.setStatus(Boolean.FALSE);
        result.setReason("append log failed");
        try {
            // 客户端写入数据的时候需要获取锁当日志被复制到
            // 大多数的节点时返回成功的数据
            appendLogLock.lock();
            if (nodeRole != RaftRole.LEADER) {
                result.setReason("current node not leader");
            } else {
                // 写入相关数据到raft集群并返回该日志条目的索引
                // 如果被提交到大部分的节点则committedIndex会大于当前的appendedIndex
                long appendedIndex = logManager.appendLog(clientRequestMsg.getMsg());
                // 等待指定的时间判断提交的日志索引是否超过当前写入的索引位置
                long endTime = System.currentTimeMillis() + raftOptions.getWriteTimeout();
                while (System.currentTimeMillis() < endTime) {
                    try {
                        long duration = endTime - System.currentTimeMillis();
                        if (duration <= 0) { // 到达指定的时间后退出
                            break;
                        }
                        appendLogCondition.await(duration, TimeUnit.MILLISECONDS);
                        // 判断达到指定的索引位置
                        if (reloadRaftMeta().getCommittedIndex() >= appendedIndex) {
                            result.setStatus(Boolean.TRUE);
                            result.setReason("append log success");
                            return result;
                        }
                    } catch (Exception e) {
                        // Ignore exception
                    }
                }
                // 判断达到指定的索引位置
                if (reloadRaftMeta().getCommittedIndex() >= appendedIndex) {
                    result.setStatus(Boolean.TRUE);
                    result.setReason("append log success");
                }
            }
        } catch (Exception e) {
            logger.info("append log failed");
        } finally {
            appendLogLock.unlock();
        }
        return result;
    }

    /**
     * 客户段获取当前raft集群leader节点的信息
     * @param refreshLeaderMsg
     * @param rpcClient
     */
    @Override
    public void onRefreshLeader(RefreshLeaderMsg refreshLeaderMsg, RpcClient rpcClient) {
        RefreshLeaderRes refreshResponse = new RefreshLeaderRes();
        String leaderId = raftLeaderId;
        if (nodeRole == RaftRole.LEADER) {
            leaderId = currentId;
        }
        // 当前raft集群可能并不存在leader节点
        // 可能集群刚启动或者发生了leadership
        if (StrUtil.isBlank(leaderId)) {
            refreshResponse.setRefreshed(Boolean.FALSE);
            refreshResponse.setErrorMsg("there is currently no leader");
        } else {
            Endpoint endpoint = raftGroupTable.getEndpoint(NodeId.of(leaderId));
            if (endpoint == null) {
                logger.warn("refresh leader => {} endpoint not found", NodeId.of(leaderId));
                refreshResponse.setRefreshed(Boolean.FALSE);
                refreshResponse.setErrorMsg("leader endpoint not found");
            } else {
                refreshResponse.setRefreshed(Boolean.TRUE);
                refreshResponse.setErrorMsg("refresh leader success");
                refreshResponse.setEndpoint(endpoint);
            }
        }
        rpcClient.sendMessage(refreshResponse); // 刷新leader节点获取raft主节点的信息
    }

}
