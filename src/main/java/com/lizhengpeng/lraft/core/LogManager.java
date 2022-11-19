package com.lizhengpeng.lraft.core;

/**
 * raft日志管理
 * @author lzp
 */
public interface LogManager {

    /**
     * 添加日志到节点(该方法在leader节点接收到日志时调用)
     * @param term
     * @param entries
     */
    void appendLog(Long term, String entries);

    /**
     * 获取当前的最后一条日志
     * @return
     */
    RaftLog getLastLog();

    /**
     * 获取指定所以的日志数据
     * @param logIndex
     * @return
     */
    RaftLog getRaftLog(long logIndex);

    /**
     * 获取下一个日志的索引
     * @return
     */
    Long getNextLogIndex();

    /**
     * 添加从leader节点接收到的日志
     * @param raftLog
     * @return 返回true或者false表示成功或失败来判断写入是否成功
     */
    boolean appendLogFromLeader(RaftLog raftLog);

}
