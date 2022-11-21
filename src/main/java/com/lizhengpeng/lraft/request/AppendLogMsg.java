package com.lizhengpeng.lraft.request;

import com.lizhengpeng.lraft.core.LogEntry;
import lombok.Getter;
import lombok.Setter;

/**
 * 追加日志的请求
 * @author lzp
 */
@Setter
@Getter
public class AppendLogMsg {

    private long term;

    private String leaderId;

    private long preLogTerm;

    private long preLogIndex;

    private LogEntry logEntry;

    private long lastCommitted;

    @Override
    public String toString() {
        return "AppendLogMsg{" +
                "term=" + term +
                ", leaderId='" + leaderId + '\'' +
                ", preLogTerm=" + preLogTerm +
                ", preLogIndex=" + preLogIndex +
                ", logEntry=" + logEntry +
                ", lastCommitted=" + lastCommitted +
                '}';
    }
}
