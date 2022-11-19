package com.lizhengpeng.lraft.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于内存实现的日志存储管理
 * @author lzp
 */
public class MemoryLogManager implements LogManager {

    private static final Logger logger = LoggerFactory.getLogger(MemoryLogManager.class);
    
    private static final Long FIRST_LOG_INDEX = 1L;

    private static final AtomicLong nextLogIndex = new AtomicLong(FIRST_LOG_INDEX); // nextLogIndex主要是用来给日志设置索引编号

    private volatile Long lastCommitted = -1L;

    private CopyOnWriteArrayList<RaftLog> logList = new CopyOnWriteArrayList<>();

    /**
     * 保存当前的日志消息
     * 该方法由leader节点直接调用
     * 注意:raft是强领导者模型任何的写入都必须先
     * 写入到leader节点中再由leader节点通过日志
     * 同步到follower节点
     * @param term
     * @param entries
     */
    @Override
    public void appendLog(Long term, String entries) {
        RaftLog raftLog = RaftLog.builder()
                .term(term)
                .index(nextLogIndex.getAndAdd(1)) // 日志的索引始终是自增的
                .entries(entries)
                .build();
        logList.add(raftLog);
    }

    /**
     * 获取当前的最后一条日志消息
     * 该方法在发起投票时调用candidate需要
     * 告知其他的节点自己当前的最后一条日志
     * 的任期号和索引来判断是否发起投票
     * @return
     */
    @Override
    public RaftLog getLastLog() {
        if (logList.isEmpty()) { // 心跳日志的term均为-1
            return RaftLog.builder().term(-1L).index(-1L).build();
        }
        return logList.get(logList.size() - 1);
    }

    /**
     * 回去指定索引的日志
     * @param logIndex
     * @return
     */
    @Override
    public RaftLog getRaftLog(long logIndex) {
        RaftLog raftLog = getRaftLogByIndex(logIndex);
        if (raftLog != null) {
            RaftLog preLog = getRaftLogByIndex(logIndex - 1);
            if (preLog != null) {
                raftLog.setPreLogTerm(preLog.getPreLogTerm());
                raftLog.setPreLogIndex(preLog.getPreLogIndex());
            } else {
                raftLog.setPreLogTerm(0L);
                raftLog.setPreLogIndex(0L);
            }
        }
        return raftLog;
    }

    /**
     * 获取指定索引的日志
     * @param logIndex
     * @return
     */
    private RaftLog getRaftLogByIndex(long logIndex) {
        for (RaftLog raftLog : logList) {
            if (raftLog.getIndex() == logIndex) {
                return raftLog;
            }
        }
        return null;
    }

    /**
     * 获取下一条日志的索引数据
     * @return
     */
    @Override
    public Long getNextLogIndex() {
        return nextLogIndex.get();
    }

    /**
     * 追加来自leader节点发送的数据
     * 注意:如果发生了重新选举等操作新选举出来的leader节点可能较之间的leader
     * 节点日志落后此时可能会发生日志覆盖raft规定follower的节点必须和leader同步
     * 因此可能发生日志覆盖(但是注意raft选举安全性保证已经提交的日志一定不会被覆盖)
     * @param raftLog
     */
    @Override
    public boolean appendLogFromLeader(RaftLog raftLog) {
        // 我们规定了raft集群中第一条日志的index为1
        // appendLog由leader节点直接调用
        // 因此第一条日志的索引一定是1
        // 约定第一条日志的preLogIndex|preLogTerm均为0
        // 最极端的情况下所有的数据都不匹配需要回退到最开始的位置
        if (raftLog.getPreLogTerm() == 0 && raftLog.getPreLogIndex() == 0) {
            logList.clear(); // 清空当前的列表
            nextLogIndex.set(0); // 下一条日志的索引为2
            logList.add(raftLog);
            nextLogIndex.addAndGet(1);
            return true;
        } else {
            // 判断上一条日志是否存在
            RaftLog log = getRaftLogByIndex(raftLog.getPreLogIndex());
            // 上一条日志不存在的时候则要求leader节点进行回退nextIndex操作
            if (log == null) {
                return false;
            } else {
                // 对应日志的term必须相同
                if (raftLog.getPreLogTerm().compareTo(log.getTerm()) != 0) {
                    return false; // 相同索引位置的日志条目的term不同也视为不同的日志项
                } else {
                    addRaftLog(log, raftLog); // 在指定的位置后面加入
                    return true;
                }
            }
        }
    }

    /**
     * 添加指定的日志消息
     * @param baseLog
     * @param raftLog
     */
    private void addRaftLog(RaftLog baseLog, RaftLog raftLog) {
        // 可能存在日志覆盖的场景此处需要进行删除数据
        // 获取log的位置信息
        int index = getLogIndex(baseLog);
        if (index != -1) {
            while (logList.size() > index) {
                logList.remove(logList.size() - 1); // 移除末尾的元素
            }
            logList.add(raftLog); // 添加当前的log数据
            nextLogIndex.set(logList.size()); // 始终为当前列表的下一个
        }
    }

    /**
     * 获取日志项在列表中的存储位置
     * @param raftLog
     * @return
     */
    private int getLogIndex(RaftLog raftLog) {
        for (int index = 0; index < logList.size(); index++) {
            if (logList.get(index).getIndex().compareTo(raftLog.getIndex()) == 0) {
                return index;
            }
        }
        return -1;
    }

}
