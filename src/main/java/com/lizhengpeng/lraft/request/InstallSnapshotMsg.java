package com.lizhengpeng.lraft.request;

import lombok.Getter;
import lombok.Setter;

/**
 * raft快照安装
 * @author lzp
 */
@Setter
@Getter
public class InstallSnapshotMsg {

    private Long term;

    private String nodeId;

    private Long lastLogIndex;

    private Long lastLogTerm;

    private byte[] data;

    private Long offset;

    private Boolean lastPart; // 快照的最后部分

    @Override
    public String toString() {
        return "InstallSnapshotMsg{" +
                "term=" + term +
                ", nodeId='" + nodeId + '\'' +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                ", offset=" + offset +
                ", lastPart=" + lastPart +
                '}';
    }
}
