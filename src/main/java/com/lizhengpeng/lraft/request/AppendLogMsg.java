package com.lizhengpeng.lraft.request;

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

    private String entries;

    private long lastCommitted;

    @Override
    public String toString() {
        return "AppendLogMsg{" +
                "term=" + term +
                ", leaderId='" + leaderId + '\'' +
                ", preLogTerm=" + preLogTerm +
                ", preLogIndex=" + preLogIndex +
                ", entries='" + entries + '\'' +
                ", lastCommitted=" + lastCommitted +
                '}';
    }
}
