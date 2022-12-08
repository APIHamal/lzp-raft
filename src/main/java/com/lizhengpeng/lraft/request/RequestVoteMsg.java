package com.lizhengpeng.lraft.request;

import lombok.Getter;
import lombok.Setter;

/**
 * 请求投票的rpc
 * @author lzp
 */
@Setter
@Getter
public class RequestVoteMsg {

    private long term;

    private String nodeId;

    private long lastLogTerm;

    private long lastLogIndex;

    @Override
    public String toString() {
        return "RequestVoteMsg{" +
                "term=" + term +
                ", nodeId='" + nodeId + '\'' +
                ", lastLogTerm=" + lastLogTerm +
                ", lastLogIndex=" + lastLogIndex +
                '}';
    }
}
