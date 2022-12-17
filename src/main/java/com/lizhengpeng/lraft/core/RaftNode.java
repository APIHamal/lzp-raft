package com.lizhengpeng.lraft.core;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import com.lizhengpeng.lraft.exception.RaftCodecException;
import com.lizhengpeng.lraft.exception.RaftException;
import com.lizhengpeng.lraft.request.AppendLogMsg;
import com.lizhengpeng.lraft.request.InstallSnapshotMsg;
import com.lizhengpeng.lraft.request.RefreshLeaderMsg;
import com.lizhengpeng.lraft.request.RequestVoteMsg;
import com.lizhengpeng.lraft.response.AppendLogRes;
import com.lizhengpeng.lraft.response.InstallSnapshotRes;
import com.lizhengpeng.lraft.response.RefreshLeaderRes;
import com.lizhengpeng.lraft.response.RequestVoteRes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

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

    private ScheduledFuture<?> cleanTaskSchedule; // leader节点一定时间内未收到follower节点的回复则可能follower宕机或者网络出现异常

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

    private ConcurrentHashMap<TaskHolder, RpcClient> taskPool = new ConcurrentHashMap<>();


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
        this.lastApplied = raftMeta.getSnapshotLastLogIndex(); // 如果加入了日志快照则lastApplied在启动时应该对应快照最后一条日志的索引
        rpcServer = new RpcServer(raftOptions);
        applySnapshot(); // 状态机应用快照数据
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
        // 执行固定的创建snapshot任务
        // 间隔指定的时间创建快照数据
        registerSnapshotTask();
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
        cleanTimeoutTask(true); // 可能是从leader角色切换到了follower角色需要通知客户端写入失败
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
                        logManager.appendLog(Task.payload("LEADER_ELECTION_NOOP"));
                        raftGroupTable.initReplicateProgress(reloadRaftMeta().getLastLogIndex() + 1); // 初始化节点的复制进度表(始终指向leader最后一条日志的下一个位置)

                        cancelTimeoutTask(); // 取消所有的定时任务
                        registerReplicateLogTask(); // 注册leader节点的心跳任务
                        registerLeaderDownSchedule(); // 注册leadership超时任务
                        registerCleanSchedule(); // 执行客户端清理任务
                    }
                }
            } catch (Exception e) {
                logger.error("request vote response handler exception", e);
            }
        });
    }

    /**
     * 清除所有已经超时的任务
     * 主节点接收到follower提交任务时
     * 在任务执行期间可能发生leadership或者集群写入超时
     */
    private void cleanTimeoutTask(boolean forceClean) {
        // 不需要在raft主线程中执行
        // async方法执行在另外一组线程池中
        taskExecutor.async(() -> {
            // 异步清理执行超时的客户端
            if (CollUtil.isNotEmpty(taskPool.keySet())) {
                taskPool.keySet().forEach(taskHolder -> {
                    // 当从leader切换到follower的时候需要强制清理数据
                    // 或者当前仍然是leader但是写入超时了也需要通知
                    if (forceClean || taskHolder.getStartTime() + raftOptions.getWriteTimeout() <= System.currentTimeMillis()) {
                        RpcClient rpcClient = taskPool.remove(taskHolder);
                        if (rpcClient != null) {
                            if (nodeRole == RaftRole.LEADER) { // 如果当前是leader则代表写入超时了
                                rpcClient.sendMessage(Status.failed("submit task timeout, please try again later"));
                            } else if (nodeRole != RaftRole.LEADER && StrUtil.isNotBlank(raftLeaderId)) { // 如果当前不是leader节点则可能发生了leadership通知客户端重定向
                                rpcClient.sendMessage(Status.redirect());
                            } else { // 集群中可能没有主节点则发生超时
                                rpcClient.sendMessage(Status.failed("there is currently no leader, please try again later"));
                            }
                        }
                    }
                });
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
     * 注册follower节点的超时任务
     * @return
     */
    public void registerCleanSchedule() {
        // 注意这里将异步的调用转为了同步的调用
        cleanTaskSchedule = taskExecutor.submit(() -> {
            try {
                // 当前不是leader则不需要执行
                if (nodeRole != RaftRole.LEADER) {
                    logger.warn("current node role => {} cancel clean task execute", nodeRole);
                    return;
                }
                cleanTimeoutTask(false); // 执行任务
                registerCleanSchedule(); // 重新注册任务
            } catch (Exception e) {
                logger.error("clean task exception", e);
            }
        }, raftOptions.getCleanTaskInterval()); // 10个心跳周期内未收到heartbeat心跳回复则退化成follower
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

                    // 当matchIndex小于firstLogIndex的时候
                    // 并且快照存在的时候则发送快照数据
                    // 否则正常同步日志即可
                    if (progress.getMatchIndex() == reloadRaftMeta().getSnapshotLastLogIndex() && reloadRaftMeta().getSnapshotLastLogIndex() != 0) { // 表示需要进行快照安装
                        // 根据snapshotOffset获取指定的数据
                        InstallSnapshotMsg snapshotMsg = new InstallSnapshotMsg();
                        snapshotMsg.setTerm(reloadRaftMeta().getCurrentTerm());
                        snapshotMsg.setNodeId(currentId);
                        snapshotMsg.setLastLogIndex(reloadRaftMeta().getSnapshotLastLogIndex());
                        snapshotMsg.setLastLogTerm(reloadRaftMeta().getSnapshotLastLogTerm());
                        snapshotMsg.setOffset(progress.getSnapshotOffset());
                        // 读取日志快照数据
                        SnapshotReader snapshotReader = new SnapshotReader(raftOptions.getLogDir() + File.separator + "snapshot" + File.separator + "raft.snapshot");
                        long hasRead = snapshotReader.reader(snapshotMsg);
                        // 更新下一次读取快照的位置
                        // 快照文件全部读取完成之后会设置lastPart的标识
                        // 标识快照读取完毕
                        progress.setSnapshotOffset(progress.getSnapshotOffset() + hasRead); // 这里是设置下一次读取快照的位置
                        // 将日志快照发送到指定的节点
                        logger.debug("send install snapshot to => {},{}", endpoint, snapshotMsg);
                        rpcServer.sendMsg(endpoint, snapshotMsg);
                    } else {
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
     * 状态机加载快照数据
     * 快照文件存在时并直接运用到状态机中
     */
    public synchronized void applySnapshot() {
        try {
            // 快照文件最后会被重命名为raft.snapshot
            File snapshotFile = new File(raftOptions.getLogDir() + File.separator + "snapshot" + File.separator + "raft.snapshot");
            if (!snapshotFile.exists() || !snapshotFile.isFile()) {
                logger.warn("snapshot file not found, skip apply snapshot");
                return;
            }
            // 读取快照内容应用到状态机中
            byte[] snapshotBuffer = IoUtil.readBytes(new FileInputStream(snapshotFile));
            stateMachine.applySnapshot(new String(snapshotBuffer, StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RaftException("apply snapshot exception", e);
        }
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
            // 如果当前是主节点则任务获取任务池里面的任务
            // 因为在主节点中任务可以有关联的回调函数
            Task task = logEntry.getTask();
            TaskHolder holder = TaskHolder.holder(task.getUuid());
            task.bind(taskPool.remove(holder)); // 移除相关的rpc对象
            stateMachine.apply(task, index); // 传入状态机数据和对应的索引
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
                    // 如果匹配的matchIndex小于firstLogIndex并且
                    // 日志快照存在的情况下则发送快照文件(注意:firstLogIndex的有效值>=1snapshotLastLogIndex的有效值>=1)
                    if (progress.getMatchIndex() == reloadRaftMeta().getSnapshotLastLogIndex() && reloadRaftMeta().getSnapshotLastLogIndex() != 0) {
                        progress.setSnapshotOffset(0L); // 从位置0开始进行日志复制
                    } else {
                        // leader节点开始回退matchIndex操作
                        progress.decrMatchIndex();
                    }
                }
            } catch (Exception e) {
                logger.error("append log callback exception", e);
            }
        });
    }

    @Override
    public void onInstallSnapshot(NodeId nodeId, InstallSnapshotMsg snapshotMsg) {
        taskExecutor.submit(() -> {
            try {
                logger.debug("node receive install snapshot {},{}", nodeId, snapshotMsg);
                if (snapshotMsg.getTerm() < reloadRaftMeta().getCurrentTerm()) {
                    InstallSnapshotRes response = new InstallSnapshotRes();
                    response.setTerm(raftMeta.getCurrentTerm());
                    response.setSuccess(false);
                    rpcServer.sendMsg(nodeId, response);
                    return;
                }
                if (snapshotMsg.getTerm() > reloadRaftMeta().getCurrentTerm()) {
                    raftMeta.setCurrentTerm(snapshotMsg.getTerm()); // 持久化meta消息
                    updateRaftMeta(raftMeta);
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
                    // 如果snapshotOffset为0则标识要新建一个临时的快照文件
                    // 用来存放leader发送的快照数据
                    File snapshotDir = new File(raftOptions.getLogDir() + File.separator + "snapshot");
                    if (!snapshotDir.exists() && !snapshotDir.mkdirs()) {
                        throw new RaftException("create snapshot dir failed");
                    }
                    // 创建本地的临时快照文件
                    // raft也可以接收来自leader节点的快照安装请求
                    File tempSnapshotFile = new File(snapshotDir, "raft.remote.snapshot");
                    if (snapshotMsg.getOffset() == 0 && tempSnapshotFile.exists()) {
                        FileUtil.del(tempSnapshotFile); // offset为0表示需要新建一个快照文件因为需要先删除
                    }
                    if (!tempSnapshotFile.exists() && !tempSnapshotFile.createNewFile()) {
                        logger.info("create snapshot file => {} failed", tempSnapshotFile.getCanonicalPath());
                        throw new RaftException("create snapshot file failed");
                    }
                    InstallSnapshotRes snapshotRes = new InstallSnapshotRes();
                    snapshotRes.setNodeId(currentId);
                    snapshotRes.setTerm(reloadRaftMeta().getCurrentTerm());
                    // 根据offset写入指定的数据
                    // 全部写入成功后则应用状态机数据
                    // 在复制正确的情况下tempFile的长度
                    // 应该与snapshotOffset相等这个时候直接追加内容即可
                    // 否则返沪false让leader重置snapshotOffset为0
                    // 重新开始复制数据
                    RandomAccessFile accessFile = AccessUtils.openRandomAccess(tempSnapshotFile);
                    if (accessFile.length() != snapshotMsg.getOffset()) {
                        snapshotRes.setFinished(Boolean.FALSE);
                        snapshotRes.setSuccess(Boolean.FALSE); // 表示复制偏移量不连续则返回false重新开始快照复制
                        AccessUtils.closeAccessFile(accessFile);
                    } else {
                        // 快照文件生成成功后更新当前的元数据信息
                        reloadRaftMeta();
                        raftMeta.setSnapshotLastLogTerm(snapshotMsg.getLastLogTerm());
                        raftMeta.setSnapshotLastLogIndex(snapshotMsg.getLastLogIndex());
                        // firstLogIndex为当前snapshotLastLogIndex+1
                        // 添加了snapshot后在同步日志时需要额外的判断
                        // 本地生成的快照文件applied永远小于等于lastLogIndex
                        raftMeta.setFirstLogIndex(snapshotMsg.getLastLogIndex() + 1);
                        raftMeta.setLastLogIndex(snapshotMsg.getLastLogIndex());
                        updateRaftMeta(raftMeta);
                        // 跳转到指定的offset开始写入数据即可
                        // 写入成功后直接应用状态机即可
                        if (snapshotMsg.getLastPart() == Boolean.TRUE) {
                            AccessUtils.closeAccessFile(accessFile);
                            FileUtil.rename(tempSnapshotFile, "raft.snapshot", true);
                            // 删除旧的历史日志记录
                            logManager.cleanPrefix(raftMeta.getFirstLogIndex()); // 删除本次快照生成涉及的快照索引
                            snapshotRes.setFinished(Boolean.TRUE); // 整个快照复制完成
                            snapshotRes.setSuccess(Boolean.TRUE);
                            applySnapshot(); // 应用状态机
                            lastApplied = snapshotMsg.getLastLogIndex(); // 状态机最后应用的日志的位置
                        } else {
                            // 跳转指定的offset写入数据
                            accessFile.seek(snapshotMsg.getOffset());
                            accessFile.write(snapshotMsg.getData());
                            // 现在部分写入完成
                            snapshotRes.setSuccess(Boolean.TRUE);
                            snapshotRes.setFinished(Boolean.FALSE);
                            AccessUtils.closeAccessFile(accessFile);
                        }
                    }
                    rpcServer.sendMsg(nodeId, snapshotRes); // 回复leader消息
                } else {
                    // candidate节点需要退化成follower节点
                    // candidate节点目前不需要返回数据
                    // 等待下次收到心跳时在follower节点时候再进行处理
                    raftMeta.setCurrentTerm(snapshotMsg.getTerm());
                    updateRaftMeta(raftMeta);
                    // 重新注册follower节点的定时器
                    cancelTimeoutTask();
                    registerFollowerTimeoutTask();
                }
            } catch (Exception e) {
                logger.info("install snapshot file exception", e);
            }
        });
    }

    @Override
    public void onInstallSnapshotCallback(InstallSnapshotRes snapshotRes) {
        taskExecutor.submit(() -> {
            logger.debug("node receive install snapshot response {}", snapshotRes);
            if (nodeRole != RaftRole.LEADER) {
                logger.warn("receive install snapshot callback => {} response but node non leader role, skip snapshot response handler", snapshotRes);
                return;
            }
            if (snapshotRes.getTerm() > reloadRaftMeta().getCurrentTerm()) {
                // 当前的leader节点退化成follower节点并等待心跳
                raftMeta.setCurrentTerm(snapshotRes.getTerm());
                updateRaftMeta(raftMeta);
                // 重新注册follower节点的定时器
                // 重新注册follower节点的定时器
                cancelTimeoutTask();
                registerFollowerTimeoutTask();
                return;
            }
            // 获取节点的复制进度表
            // 根据节点的复制进度来推进commit的提交或者nextLogIndex的回退操作
            ReplicateProgress progress = raftGroupTable.getReplicate(snapshotRes.getNodeId());
            if (progress == null) {
                logger.warn("append log callback get node => {} progress not found", snapshotRes.getNodeId());
                return;
            }
            // 如果完成了日志快照的复制则更新复制的进度为当前最新的提交记录
            if (snapshotRes.getFinished() == Boolean.TRUE) {
                raftGroupTable.initReplicateProgress(reloadRaftMeta().getLastLogIndex() + 1); // 重新设置节点的复制进度表(始终指向leader最后一条日志的下一个位置)
            } else {
                // 如果follower等接收快照数据失败则重新设置偏移量
                // success为true的时候不需要做处理
                // snapshotOffset的增加已经在replicateLogTask中设置过了
                // 这里不需要做处理
                if (snapshotRes.getSuccess() == Boolean.FALSE) {
                    progress.setSnapshotOffset(0l); // 从头开始复制
                }
            }
        });
    }

    /**
     * 客户段获取当前raft集群leader节点的信息
     * @param refreshLeaderMsg
     * @param rpcClient
     */
    @Override
    public void onRefreshLeader(RefreshLeaderMsg refreshLeaderMsg, RpcClient rpcClient) {
        RefreshLeaderRes refreshResponse = new RefreshLeaderRes();
        refreshResponse.setRefreshed(Boolean.FALSE);
        refreshResponse.setErrorMsg("refresh leader failed");
        try {
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
        } catch (Exception e) {
            logger.info("refresh leader ");
        }
    }

    /**
     * 接收到客户端的提交的任务
     * @param task
     * @param rpcClient
     */
    @Override
    public void onSubmitTask(Task task, RpcClient rpcClient) {
        // 首先将任务提交到池中等待写入到raft节点同步
        // 如果任务被成功的指定可以进行回调
        taskExecutor.submit(() -> { // raft主线程执行的时候会开始向其他的节点同步任务
            try {
                if (nodeRole != RaftRole.LEADER && StrUtil.isNotBlank(raftLeaderId)) { // 当前集群存在leader节点但是当前节点不是leader
                    rpcClient.sendMessage(Status.redirect());
                } else if (nodeRole != RaftRole.LEADER && StrUtil.isBlank(raftLeaderId)) { // 当前集群不存在leader节点
                    rpcClient.sendMessage(Status.failed("there is currently no leader, please try again later"));
                } else {
                    // 写入相关数据到raft集群并返回该日志条目的索引
                    // 如果被提交到大部分的节点则committedIndex会大于当前的appendedIndex
                    taskPool.putIfAbsent(TaskHolder.holder(task.getUuid()), rpcClient);
                    logManager.appendLog(task);
                }
            } catch (Exception e) {
                logger.info("task submit failed", e);
            }
        });
    }

    /**
     * 注册快照生成任务
     * 间隔指定的时间后便会生成一次快照
     */
    public void registerSnapshotTask() {
        snapshotTask = taskExecutor.fixedDelayTask(() -> {
            try {
                // 当前只允许leader节点生成快照数据
                if (nodeRole != RaftRole.LEADER) {
                    logger.debug("current role not leader, skip snapshot task");
                    return;
                }
                // lastLogIndex小于firstLogIndex表示当前没有任何的日志数据
                // lastApplied小于firstLogIndex表示当前没有应用任何的日志数据
                if (reloadRaftMeta().getSnapshotLastLogIndex() >= reloadRaftMeta().getCommittedIndex()) {
                    logger.debug("snapshot last log index great than or equals last committed index, skip snapshot task");
                    return;
                }
                // 快照文件最后会被重命名为raft.snapshot
                // 但是快照文件在最终生成前会生成临时快照文件
                File snapshotDir = new File(raftOptions.getLogDir() + File.separator + "snapshot");
                if (!snapshotDir.exists() && !snapshotDir.mkdirs()) {
                    throw new RaftException("create snapshot dir failed");
                }
                // 创建本地的临时快照文件
                // raft也可以接收来自leader节点的快照安装请求
                File tempSnapshotFile = new File(snapshotDir, "raft.local." + reloadRaftMeta().getLastLogIndex() +".snapshot");
                FileUtil.del(tempSnapshotFile); // 快照临时文件如果存在则进行删除(之前可能存在这个时候需要进行删除)
                if (!tempSnapshotFile.createNewFile()) {
                    logger.info("create snapshot file => {} failed", tempSnapshotFile.getCanonicalPath());
                    throw new RaftException("create snapshot file failed");
                }
                // 创建文件流准备写入数据
                RandomAccessFile accessFile = AccessUtils.openRandomAccess(tempSnapshotFile);
                long applied = lastApplied; // 当前状态机应用的位置
                // 状态机写入当前的指令数据
                String snapshot = stateMachine.writeSnapshot();
                // 得到最后一条日志的term和index
                long lastLogTerm = logManager.getLogEntry(applied).getTerm();
                long lastLogIndex = logManager.getLogEntry(applied).getIndex();
                // 将状态机写入到临时文件中
                accessFile.write(snapshot.getBytes(StandardCharsets.UTF_8));
                AccessUtils.closeAccessFile(accessFile);
                // 更新快照文件的数据
                // 其实这里应该要保持原子性的更新操作
                // 真实生成环境应该借助rocksDb之类的本地kv存储保持更新的原子性
                // 更新临时文件的文件名为新的快照文件
                FileUtil.rename(tempSnapshotFile, "raft.snapshot", true);
                // 快照文件生成成功后更新当前的元数据信息
                reloadRaftMeta();
                raftMeta.setSnapshotLastLogTerm(lastLogTerm);
                raftMeta.setSnapshotLastLogIndex(lastLogIndex);
                // firstLogIndex为当前snapshotLastLogIndex+1
                // 添加了snapshot后在同步日志时需要额外的判断
                // 本地生成的快照文件applied永远小于等于lastLogIndex
                raftMeta.setFirstLogIndex(lastLogIndex + 1);
                updateRaftMeta(raftMeta);
                // 删除旧的历史日志记录
                logManager.cleanPrefix(applied); // 删除本次快照生成涉及的快照索引
            } catch (Exception e) {
                logger.info("generate snapshot file exception", e);
            }
        }, raftOptions.getSnapshotTaskInterval());
    }

}
