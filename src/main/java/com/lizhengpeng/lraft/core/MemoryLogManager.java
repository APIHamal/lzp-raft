package com.lizhengpeng.lraft.core;

import cn.hutool.core.collection.ListUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于内存实现的日志存储管理
 * @author lzp
 */
public class MemoryLogManager implements LogManager {

    private static final AtomicLong nextLogIndex = new AtomicLong(1L);

    private List<LogEntry> logEntries = new ArrayList<>();

    /**
     * 判断两个值是否相等
     * @param source
     * @param dest
     * @return
     */
    public static boolean compare(Long source, Long dest) {
        return compare(source, dest, 0);
    }

    /**
     * 判断两个值是否相等
     * @param source
     * @param dest
     * @return
     */
    public static boolean great(Long source, Long dest) {
        return  source != null && dest != null && source.compareTo(dest) > 0;
    }

    /**
     * 判断两个值是否相等
     * @param source
     * @param dest
     * @return
     */
    public static boolean greatOrEquals(Long source, Long dest) {
        return  source != null && dest != null && source.compareTo(dest) >= 0;
    }

    /**
     * 判断两个值是否相等
     * @param source
     * @param dest
     * @return
     */
    public static boolean compare(Long source, Long dest, int compareRes) {
        return source != null && dest != null && source.compareTo(dest) == compareRes;
    }

    /**
     * 添加日志到列表中
     * @param term
     * @param entries
     * @return
     */
    @Override
    public void appendLog(Long term, String entries) {
        LogEntry logEntry = LogEntry.builder()
                .term(term)
                .index(nextLogIndex.getAndAdd(1))
                .entries(entries)
                .build();
        logEntries.add(logEntry);
    }

    /**
     * 根据索引创建指定的日志
     * @param term
     * @param index 日志对应的索引该方法自动填充preLogIndex|preLogTerm
     * @param entries
     * @return
     */
    public LogEntry createLog(Long term, Long index, String entries) {
        LogEntry logEntry = LogEntry.builder()
                .term(term)
                .index(index)
                .entries(entries)
                .build();
        LogEntry preLog = this.getLogEntry(index - 1);
        if (preLog == null) { // 第一条日志的preLogIndex|preLogTerm都是0
            logEntry.setPreLogTerm(0L);
            logEntry.setPreLogIndex(0L);
        } else {
            logEntry.setPreLogTerm(preLog.getTerm());
            logEntry.setPreLogIndex(preLog.getPreLogIndex());
        }
        return logEntry;
    }

    /**
     * 复制从leader节点接收到的日志
     * @param raftLog
     * @return
     */
    @Override
    public boolean replicateLog(LogEntry raftLog) {
        // 整个日志中的第一条数据
        if (compare(raftLog.getPreLogTerm(), 0L) && compare(raftLog.getPreLogIndex(), 0L)) {
            logEntries.clear();
            logEntries.add(raftLog);
            nextLogIndex.set(2); // 下一个日志索引的值
            return true;
        } else {
            // 判断索引是否匹配
            LogEntry temp = LogEntry.builder()
                    .term(raftLog.getPreLogTerm())
                    .index(raftLog.getPreLogIndex())
                    .build();
            int index = logEntries.indexOf(temp);
            if (index == -1) { // 未找到指定的日志项则返回false
                return false;
            } else {
                // 清楚待保存日志的preLogTerm和preLogIndex
                raftLog.setPreLogIndex(null);
                raftLog.setPreLogTerm(null);
                // 如果preLogIndex为末尾的数据则直接添加
                if (index == logEntries.size() - 1) {
                    logEntries.add(raftLog);
                } else {
                    // 清楚列表数据直到index的位置
                    List<LogEntry> newLogEntries = ListUtil.toList(logEntries.subList(0, index + 1));
                    newLogEntries.add(raftLog);
                    logEntries = newLogEntries; // 更新日志列表
                }
                nextLogIndex.addAndGet(logEntries.size()); // 下一个日志的索引数据
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
