package com.lizhengpeng.lraft.core;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

/**
 * 日志抽象
 * @author lzp
 */
@Setter
@Getter
@Builder
public class LogEntry {

    private static final Integer HEART_BEAT_LOG = 1;

    private static final Integer GENERAL_LOG = 2;

    private Integer kind;

    private Long term;

    private Long index;

    private String entries;

    private Long preLogIndex;

    private Long preLogTerm;

    private boolean logEquals(Long source, Long dest) {
        return source != null && dest != null && source.compareTo(dest) == 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj != null && obj instanceof LogEntry) {
            LogEntry logEntry = (LogEntry) obj;
            return logEquals(term, logEntry.getTerm()) && logEquals(index, logEntry.getIndex());
        }
        return false;
    }

    @Override
    public int hashCode() {
            return Objects.hash(term, index);
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "kind=" + kind +
                ", term=" + term +
                ", index=" + index +
                ", entries='" + entries + '\'' +
                ", preLogIndex=" + preLogIndex +
                ", preLogTerm=" + preLogTerm +
                '}';
    }
}
