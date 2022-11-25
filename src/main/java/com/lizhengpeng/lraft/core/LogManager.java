package com.lizhengpeng.lraft.core;

/**
 * raft日志管理
 * @author lzp
 */
public interface LogManager {

    /**
     * 添加日志到节点
     * 该方法在leader节点直接调用
     * @param term
     * @param entries
     */
    LogEntry appendLog(Long term, String entries);

    /**
     * 复制从leader发送的日志数据
     * @param preLogTerm
     * @param preLogIndex
     * @param raftLog
     * @return 返回true或者false表示成功或失败来判断写入是否成功
     */
    boolean replicateLog(Long preLogTerm, Long preLogIndex, LogEntry raftLog);

    /**
     * 根据索引获取对应的日志
     * @param index
     * @return
     */
    LogEntry getLogEntry(Long index);

    /**
     * 获取当前的最后一条日志
     * 如果不存在则返回term|index都为0的数据
     * @return
     */
    LogEntry getLastLog();

    /**
     * 获取下一个日志的索引
     * @return
     */
    Long getNextLogIndex();

}
