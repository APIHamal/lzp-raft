package com.lizhengpeng.lraft.core;

/**
 * raft日志管理
 * @author lzp
 */
public interface LogManager {

    /**
     * 添加日志到节点
     * 该方法在leader节点直接调用
     * @param task
     * @return
     */
    long appendLog(Task task);

    /**
     * 添加日志到节点
     * 该方法在leader节点直接调用
     * @param term
     * @param task
     * @return
     */
    long appendLog(long term, Task task);

    /**
     * 复制从leader发送的日志数据
     * @param preLogTerm
     * @param preLogIndex
     * @param raftLog
     * @return 返回true或者false表示成功或失败来判断写入是否成功
     */
    boolean replicateLog(long preLogTerm, long preLogIndex, LogEntry raftLog);

    /**
     * 根据索引获取对应的日志
     * @param index
     * @return
     */
    LogEntry getLogEntry(long index);

    /**
     * 获取当前的最后一条日志
     * 如果不存在则返回term|index都为0的数据
     * @return
     */
    LogEntry getLastLog();

}
