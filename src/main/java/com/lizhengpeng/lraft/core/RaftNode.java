package com.lizhengpeng.lraft.core;

import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import com.lizhengpeng.lraft.exception.RaftCodecException;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;

import static com.lizhengpeng.lraft.core.RaftUtils.compare;
import static com.lizhengpeng.lraft.core.RaftUtils.great;

/**
 * raft核心算法的实现
 * @author lzp
 */
public class RaftNode implements MessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    private volatile RaftRole nodeRole = RaftRole.FOLLOWER; // 当前节点的角色

    private String currentId;

    private long voteCount; // 当前候选者节点得到的票数

    private RaftGroupTable raftGroupTable = new RaftGroupTable(); // 集群成员表

    private RpcServer rpcServer;

    private TaskExecutor taskExecutor = new TaskExecutor();

    private ScheduledFuture<?> followerSchedule;

    private ScheduledFuture<?> candidateSchedule;

    private ScheduledFuture<?> leaderSchedule;

    private ScheduledFuture<?> leadershipSchedule; // leader节点未收到消息时自动退化

    private ScheduledFuture<?> snapshotTask;

    private SecureRandom secureRandom = new SecureRandom();

    private FileLogManager logManager;

    private volatile String raftLeaderId;

    private Set<NodeId> heartbeatBox = new HashSet<>();

    private RaftOptions raftOptions; // raft相关的配置

    private RaftMeta raftMeta; // raft相关元数据

    private StateMachine stateMachine; // 状态机的定义

    private Snapshot snapshot;

    private long lastApplied;

    private ConcurrentHashMap<Long, Future<Void>> waitTask = new ConcurrentHashMap<>();

    /**
     * 默认的构造函数初始化相关的组件
     * @param raftOptions
     */
    public RaftNode(RaftOptions raftOptions, StateMachine stateMachine) {
        this.raftOptions = raftOptions;
        this.logManager = new FileLogManager(raftOptions);
        this.snapshot = new Snapshot(raftOptions.getLogDir());
        this.raftMeta = logManager.getRaftMeta();
        this.stateMachine = stateMachine;
        rpcServer = new RpcServer(raftOptions);
        registerSnapshotTask(); // 执行固定的创建snapshot任务
        applyStateMachine(logManager.getRaftMeta().getCommittedIndex());
    }

    /**
     * 更新元数据信息
     */
    public void updateRaftMeta() {
        logManager.updateRaftMeta();
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
        return raftOptions.minElectionTimeout + secureRandom.nextInt(raftOptions.maxElectionTimeout - raftOptions.minElectionTimeout);
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
            raftMeta.setCurrentTerm(raftMeta.getCurrentTerm() + 1); // 增加任期
            updateRaftMeta();

            // 发送投票的请求到各个节点中
            RequestVoteMsg message = new RequestVoteMsg();
            message.setNodeId(currentId);
            message.setTerm(raftMeta.getCurrentTerm());
            // 获取最后一条日志用来选举
            // 首先比较日志的term再比较日志的index
            // term较大或者term相等但是index较大的则进行投票
            LogEntry lastLog = logManager.getLastLog();
            message.setLastLogTerm(lastLog.getTerm()); // 最后一条日志的任期和索引
            message.setLastLogIndex(lastLog.getIndex());

            logger.debug("node send vote => {},{},{},{}", nodeRole.name(), currentId, raftMeta.getCurrentTerm(), message);
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
            raftMeta.setCurrentTerm(raftMeta.getCurrentTerm() + 1); // 增加任期
            updateRaftMeta();

            // 发送投票的请求到各个节点中
            RequestVoteMsg message = new RequestVoteMsg();
            message.setNodeId(currentId);
            message.setTerm(raftMeta.getCurrentTerm());
            // 获取最后一条日志用来选举
            // 首先比较日志的term再比较日志的index
            // term较大或者term相等但是index较大的则进行投票
            LogEntry lastLog = logManager.getLastLog();
            message.setLastLogTerm(lastLog.getTerm()); // 最后一条日志的任期和索引
            message.setLastLogIndex(lastLog.getIndex());

            logger.debug("node send vote => {},{},{},{}", nodeRole.name(), currentId, raftMeta.getCurrentTerm(), message);
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
                        logger.warn("node => {} replicate process not found", endpoint);
                        continue; // 跳过当前节点的处理
                    }
                    // 如果当前matchIndex小于第一条索引的位置则需要进行发送快照数据
                    if (progress.getMatchIndex() < raftMeta.getFirstLogIndex()) {
                        // 发送快照数据
                        InstallSnapshotMsg snapshotMsg = new InstallSnapshotMsg();
                        snapshotMsg.setTerm(raftMeta.getCurrentTerm());
                        snapshotMsg.setNodeId(currentId);
                        snapshotMsg.setLastLogTerm(snapshot.readMeta().getLastLogTerm()); // 快照中对应最后一条数据的term
                        snapshotMsg.setLastLogIndex(snapshot.readMeta().getLastLogIndex()); // 快照中对应最后一条日志的索引
                        snapshotMsg.setOffset(progress.getSnapshotOffset()); // 从指定的索引位置开始读取数据
                        // 从指定索引位置开始读取数据
                        byte[] buffer = snapshot.readSnapshot(progress.getSnapshotOffset());
                        snapshotMsg.setData(new String(buffer, StandardCharsets.UTF_8));
                        // 如果读取到了空的数组则表示读取到了文件的末尾位置
                        if (buffer.length == 0) {
                            snapshotMsg.setLastPart(true);
                        } else {
                            snapshotMsg.setLastPart(false);
                        }
                        // 记录当前读取的数据的长度
                        progress.setLastReadLength(buffer.length);
                        rpcServer.sendMsg(endpoint, snapshotMsg); // 发送给指定的节点数据
                        continue;
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
                    appendLogMsg.setTerm(raftMeta.getCurrentTerm());
                    appendLogMsg.setLastCommitted(raftMeta.getCommittedIndex()); // 当前可以提交的索引的位置

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
                    logger.debug("send append log to => {},{}", endpoint, appendLogMsg);
                }
            } catch (RaftCodecException e) {
                logger.debug("broadcast codec exception", e);
            }
            // 重复的发送数据
            registerLeaderTask();
        }, raftOptions.heartbeatInterval);
    }

    /**
     * 注册follower节点的超时任务
     * @return
     */
    public void registerSnapshotTask() {
        // 固定时间间隔创建状态机的快照数据
        snapshotTask = taskExecutor.replicateTask(() -> {
            try {
                // 当前只允许leader进行快照生成
                if (nodeRole != RaftRole.LEADER) {
                    return;
                }
                if (lastApplied <= raftMeta.getFirstLogIndex()) {
                    return; // 未提交任何日志则直接返回
                }
                // 获取当前状态机提交的日志
                LogEntry logEntry = logManager.getLogEntry(lastApplied);
                if (logEntry == null) {
                    return;
                }
                // 写入日志快照数据
                SnapshotWriter snapshotWriter = snapshot.getSnapshotWriter();
                stateMachine.writeSnapshot(snapshotWriter); // 状态机写入数据
                // 创建快照的元数据
                SnapshotMeta meta = SnapshotMeta.builder()
                        .lastLogIndex(logEntry.getIndex())
                        .lastLogTerm(logEntry.getTerm())
                        .build();
                // 写入元数据(只能在快照数据完全准备好了才可以删除原始的日志数据)
                // 并且更新raftMeta信息否则会造成数据的丢失
                snapshotWriter.writeSnapshotMeta(meta);
                // todo 暂时不删除原始的日志数据后续进行删除
                snapshotWriter.complete();
                snapshot.reloadSnapshot(); // 重新加载snapshot数据(按照时间戳命令文件夹倒叙排列则可找到最新的快照)
                // 更新raft的元数据信息
                raftMeta.setFirstLogIndex(lastApplied + 1); // 更新第一条日志的索引位置
                updateRaftMeta();
            } catch (Exception e) {
                logger.info("create state machine task occur exception", e);
            }
        }, raftOptions.snapshotInterval);
    }

    /**
     * 注册follower节点的超时任务
     * @return
     */
    public void registerLeadershipTask() {
        // 注意这里将异步的调用转为了同步的调用
        leadershipSchedule = taskExecutor.submit(() -> {
            // 在一个随机超时的时间内leader没有收到follower/candidate的心跳消息
            // 并且没有收到一个term比自己大的投票或者新leader的心跳
            // 则有可能当前出现了网络分区或者follower/candidate发生了宕机
            // 这个时候集群其实已经没有了过半的服务器需要停止写入支持
            // 退化成leader也可以
            if (!raftGroupTable.greatHalf(heartbeatBox.size())) {
                logger.warn("raft happen leader ship !!!");
                // 节点变成了follower节点停止写入支持
                // 重新注册follower节点的定时器
                nodeRole = RaftRole.FOLLOWER;
                cancelTimeoutTask();
                registerFollowerTimeoutTask();
            } else {
                heartbeatBox.clear(); // 清空并继续收集
                // 重复的注册
                registerLeadershipTask();
            }
        }, raftOptions.heartbeatInterval * 10); // 10个心跳周期内未收到heartbeat心跳回复则退化成follower
    }

    /**
     * 取消已经注册的定时任务
     */
    public void cancelTimeoutTask() {
        cancelTimeoutTask(followerSchedule);
        cancelTimeoutTask(candidateSchedule);
        cancelTimeoutTask(leaderSchedule);
        cancelTimeoutTask(leadershipSchedule);
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

    @Override
    public void onAppendLog(NodeId nodeId, AppendLogMsg appendLogMsg) {
        taskExecutor.submit(() -> {
            logger.debug("node receive append log {},{}", nodeId, appendLogMsg);
            if (appendLogMsg.getTerm() < raftMeta.getCurrentTerm()) {
                AppendLogRes response = new AppendLogRes();
                response.setTerm(raftMeta.getCurrentTerm());
                response.setSuccess(false);
                rpcServer.sendMsg(nodeId, response);
                return;
            }
            if (appendLogMsg.getTerm() > raftMeta.getCurrentTerm()) {
                nodeRole = RaftRole.FOLLOWER;
                raftMeta.setCurrentTerm(appendLogMsg.getTerm()); // 持久化meta消息
                updateRaftMeta();
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
                // 更新当前集群中可以提交的索引
                raftMeta.setCommittedIndex(appendLogMsg.getLastCommitted()); // 持久化committedIndex
                updateRaftMeta();

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
                        response.setTerm(raftMeta.getCurrentTerm());
                        response.setSuccess(true);
                        rpcServer.sendMsg(nodeId, response); // 回复leader节点的心跳消息
                        // 对于follower来说可引用到状态的日志为
                        // 实际写入的日志数量和committedIndex的最小值
                        applyStateMachine(Math.min(raftMeta.getLastLogIndex(), raftMeta.getCommittedIndex()));
                        cancelTimeoutTask(); // 取消定时任务
                        registerFollowerTimeoutTask(); // 重新注册follower超时任务
                        return;
                    } else {
                        // 这个时候有两种情况一种是节点完成了全部消息的同步
                        // 还有一种是节点刚当前leader还未进行同步
                        LogEntry logEntry = logManager.getLogEntry(appendLogMsg.getPreLogIndex());
                        AppendLogRes response = new AppendLogRes();
                        response.setNodeId(currentId); // leader需要根据这个标识来确定对应的节点从而更新复制进度
                        response.setTerm(raftMeta.getCurrentTerm());
                        if (logEntry == null || !compare(appendLogMsg.getPreLogTerm(), logEntry.getTerm())) {
                            response.setSuccess(false);
                        } else {
                            // 对于follower来说可引用到状态的日志为
                            // 实际写入的日志数量和committedIndex的最小值
                            applyStateMachine(Math.min(raftMeta.getLastLogIndex(), raftMeta.getCommittedIndex()));
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
                    response.setTerm(raftMeta.getCurrentTerm());

                    // 追加从leader节点接收到的消息并进行持久化操作
                    // 如果日志匹配则添加否则返回false表示进行日志的回退操作
                    if (!logManager.replicateLog(appendLogMsg.getPreLogTerm(), appendLogMsg.getPreLogIndex(), appendLogMsg.getLogEntry())) { // 写入日志失败可能需要回退操作
                        response.setSuccess(false);
                    } else {
                        // 对于follower来说可引用到状态的日志为
                        // 实际写入的日志数量和committedIndex的最小值
                        applyStateMachine(Math.min(raftMeta.getLastLogIndex(), raftMeta.getCommittedIndex()));
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
                raftMeta.setCurrentTerm(appendLogMsg.getTerm());
                updateRaftMeta();
                // 重新注册follower节点的定时器
                cancelTimeoutTask();
                registerFollowerTimeoutTask();
            }
        });
    }

    /**
     * 应用命令到状态机
     * @param committedIndex
     */
    public synchronized void applyStateMachine(long committedIndex) {
        if (lastApplied >= committedIndex) {
            return;
        }
        long applyIndex = lastApplied + 1;
        for (;applyIndex <= committedIndex; applyIndex++) {
            LogEntry logEntry = logManager.getLogEntry(applyIndex);
            if (logEntry != null) {
                stateMachine.apply(logEntry.getEntries(), applyIndex);
            }
        }
        // lastApplied不需要进行持久化
        // raft启动的时候会使用日志来初始化整个状态机
        lastApplied = committedIndex;
    }

    @Override
    public void onAppendLogCallback(AppendLogRes appendLogRes) {
        taskExecutor.submit(() -> {
            logger.debug("node receive append log callback [{}]", appendLogRes);
            if (appendLogRes.getTerm() > raftMeta.getCurrentTerm()) {
                // 当前的leader节点退化成follower节点并等待心跳
                nodeRole = RaftRole.FOLLOWER;
                raftMeta.setCurrentTerm(appendLogRes.getTerm());
                updateRaftMeta();
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
            progress.setNextIndex(logManager.getRaftMeta().getLastLogIndex() + 1); // lastLogIndex为实际写入的最后一条日志的索引
            // 日志被复制过半后
            // 触发对应的回调通知客户端对象
            if (appendLogRes.getSuccess() == Boolean.TRUE) {
                // 日志过半则可以进行提交
                // 进行对应matchIndex日志的提交
                if (raftGroupTable.replicateGreatHalf(progress.getMatchIndex())) {
                    long committed = progress.getMatchIndex();
                    if (committed == progress.getNextIndex()) { // 当集群稳定之后matchIndex=nextIndex并且这两个值都指向下一条需要复制的日志
                        committed--;
                    }
                    raftMeta.setCommittedIndex(committed); // 持久化committedIndex
                    updateRaftMeta();
                    applyStateMachine(committed); // 应用到状态机
                }
                progress.incrMatchIndex(); // leader节点推进日志的复制
            } else {
                progress.decrMatchIndex(); // leader节点开始回退操作
                // 如果回退的进度小于第一条日志则需要发送快照
                if (progress.getMatchIndex() < raftMeta.getFirstLogIndex()) {
                    progress.setSnapshotOffset(0L); // 从位置0开始进行日志复制
                }
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
                updateRaftMeta();
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
                    updateRaftMeta(); // 持久化raft元数据
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
                updateRaftMeta();
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
                updateRaftMeta();
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

    @Override
    public void onRequestVote(NodeId nodeId, RequestVoteMsg requestVoteMsg) {
        taskExecutor.submit(() -> {
            logger.debug("receive vote request {},{}", nodeId, requestVoteMsg);
            // 请求投票的节点的term小于当前的节点则返回当前节点的任期给对端节点
            if (requestVoteMsg.getTerm() < raftMeta.getCurrentTerm()) {
                RequestVoteRes response = new RequestVoteRes();
                response.setTerm(raftMeta.getCurrentTerm());
                response.setSuccess(false);
                rpcServer.sendMsg(nodeId, response);
                return;
            }
            // 如果当前的term小于对端节点则立即变为follower节点并进行投票
            // 例如一个节点超时较快变成了candidate但是当前节点超时较慢
            // 目前仍然是一个follower节点
            if (requestVoteMsg.getTerm() > raftMeta.getCurrentTerm()) {
                nodeRole = RaftRole.FOLLOWER;
                raftMeta.setCurrentTerm(requestVoteMsg.getTerm());
                updateRaftMeta();
                // 记录当前节点给某个任期投过票
                // 这个属性很关键raft中要保证每个任期中
                // 只会给节点投递一票的数据
                raftMeta.setVoteFor(requestVoteMsg.getTerm());
                updateRaftMeta();

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
            if (nodeRole == RaftRole.FOLLOWER && raftMeta.getVoteFor() < requestVoteMsg.getTerm()) {
                // 获取当前节点与对端节点日志的新旧
                RequestVoteRes response = new RequestVoteRes();
                LogEntry lastLog = logManager.getLastLog();
                // 节点自身的最后一条日志的term比对端大或者term相等但是logIndex大的获得投票
                // 首先比较term当term相等的时候比较index
                if (great(requestVoteMsg.getLastLogTerm(), lastLog.getTerm()) ||
                        (compare(requestVoteMsg.getLastLogTerm(), lastLog.getTerm()) && great(requestVoteMsg.getLastLogIndex(), lastLog.getIndex()))) {
                    response.setTerm(raftMeta.getCurrentTerm());
                    response.setSuccess(true);
                } else {
                    // 对端节点的日志没有自己的新则不进行投票
                    response.setTerm(raftMeta.getCurrentTerm());
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
                response.setTerm(raftMeta.getCurrentTerm());
                response.setSuccess(false);
                rpcServer.sendMsg(nodeId, response);
            }
        });
    }

    @Override
    public void onRequestVoteCallback(RequestVoteRes res) {
        logger.debug("receive vote callback {}", res);
        taskExecutor.submit(() -> {
            if (raftMeta.getCurrentTerm() < res.getTerm()) {
                raftMeta.setCurrentTerm(res.getTerm());
                updateRaftMeta();

                nodeRole = RaftRole.FOLLOWER;
                cancelTimeoutTask(); // 取消当前状态下关联的定时任务
                registerFollowerTimeoutTask(); // 注册follower节点的定时任务
                return;
            }
            // 当前如果仍然是候选者节点
            // 则判断票数是否过半了
            if (res.getSuccess() == Boolean.TRUE) {
                logger.debug("leader success election => {},{}", currentId, raftMeta.getCurrentTerm());
                voteCount++;
                // 如果超过了半数的选票(加1是因为加上candidate节点自身)
                if (raftGroupTable.electionSuccess(voteCount + 1)) {
                    nodeRole = RaftRole.LEADER; // 节点选举成功变成了leader
                    // 写入一条NoOp的日志
                    logManager.appendLog("LEADER_ELECTION_NOOP");
                    raftGroupTable.initReplicateProgress(logManager.getRaftMeta().getLastLogIndex() + 1); // 初始化节点的复制进度表(始终指向leader最后一条日志的下一个位置)
                    cancelTimeoutTask(); // 取消所有的定时任务
                    registerLeaderTask(); // 注册leader节点的心跳任务
                    heartbeatBox.clear(); // 清空列表
                    registerLeadershipTask(); // 注册leadership超时任务
                }
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
        result.setStatus(Boolean.TRUE);
        taskExecutor.submit(() -> {
            if (nodeRole != RaftRole.LEADER) {
                result.setReason("current node not leader");
            } else {
                logManager.appendLog(clientRequestMsg.getMsg());
            }
        });
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
