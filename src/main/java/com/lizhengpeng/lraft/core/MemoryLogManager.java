package com.lizhengpeng.lraft.core;

import cn.hutool.core.collection.ListUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.lizhengpeng.lraft.core.RaftUtils.*;

/**
 * 基于内存实现的日志存储管理
 * @author lzp
 */
public class MemoryLogManager implements LogManager {

    private static final Logger logger = LoggerFactory.getLogger(MemoryLogManager.class);

    private static final AtomicLong nextLogIndex = new AtomicLong(1L);

    private List<LogEntry> logEntries = new ArrayList<>();

    private RaftNode raftNode;

    public MemoryLogManager(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    /**
     * 添加日志到列表中
     * @param entries
     * @return
     */
    @Override
    public long appendLog(String entries) {
        return this.appendLog(raftNode.getCurrentTerm(), entries);
    }

    @Override
    public long appendLog(Long term, String entries) {
        LogEntry logEntry = LogEntry.builder()
                .term(raftNode.getCurrentTerm())
                .index(nextLogIndex.getAndAdd(1))
                .entries(entries)
                .build();
        logEntries.add(logEntry);
        return logEntry.getIndex();
    }

    /**
     * 复制从leader节点接收到的日志
     * @param raftLog
     * @return
     */
    @Override
    public boolean replicateLog(Long preLogTerm, Long preLogIndex, LogEntry raftLog) {
        logger.warn("receive log => {}", raftLog);
        // 设置日志所属的Index因为log的preLogIndex已经确定
        // 所以这条log自身的index也就确定了
        if (compare(preLogTerm, 0L) && compare(preLogIndex, 0L)) {
            logEntries.clear();
            logEntries.add(raftLog);
            nextLogIndex.set(raftLog.getIndex() + 1); // 下一个日志索引的值
            return true;
        } else {
            // 判断索引是否匹配
            LogEntry temp = LogEntry.builder()
                    .term(preLogTerm)
                    .index(preLogIndex)
                    .build();
            int index = logEntries.indexOf(temp);
            if (index == -1) { // 未找到指定的日志项则返回false
                return false;
            } else {
                nextLogIndex.set(raftLog.getIndex() + 1); // 下一条日志的索引是在当前日志基础上加一(自增)
                // 如果preLogIndex为末尾的数据则直接添加
                if (index == logEntries.size() - 1) {
                    logEntries.add(raftLog);
                } else {
                    // 清楚列表数据直到index的位置
                    List<LogEntry> newLogEntries = ListUtil.toList(logEntries.subList(0, index + 1));
                    newLogEntries.add(raftLog);
                    logEntries = newLogEntries; // 更新日志列表
                }
                return true;
            }
        }
    }

    /**
     * 根据索引获取日志
     * @param index
     * @return
     */
    @Override
    public LogEntry getLogEntry(Long index) {
        if (index == null || index <= 0) {
            return null;
        }
        if (!logEntries.isEmpty()) {
            for (LogEntry entry : logEntries) {
                if (compare(entry.getIndex(), index)) {
                    return entry;
                }
            }
        }
        return null;
    }

    /**
     * 获取列表中的最后一条日志
     * @return
     */
    @Override
    public LogEntry getLastLog() {
        if (logEntries.isEmpty()) { // 无任何日志时返回一个term|index为0的数据
            return LogEntry.builder() // 这样可以保证选举的正常运行
                    .term(0L)         // 由于term|index在raft中都是从1递增的
                    .index(0L)        // 因此一旦有日志记录term|index就不可能为0
                    .build();
        }
        return logEntries.get(logEntries.size() - 1);
    }

    /**
     * 获取下一条日志的索引数据
     * @return
     */
    @Override
    public Long getNextLogIndex() {
        return nextLogIndex.get();
    }

}
