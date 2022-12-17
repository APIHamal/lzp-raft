package com.lizhengpeng.lraft.core;

import lombok.Getter;
import lombok.Setter;

/**
 * raft节点的复制进度
 * @author lzp
 */
@Setter
@Getter
public class ReplicateProgress {

    private long matchIndex;

    private long nextIndex;

    private long snapshotOffset;

    private long lastLog; // 记录上一次发送的日志

    public void decrMatchIndex() { // 第一条日志数据
        if (matchIndex > 0) {
            matchIndex--;
        }
    }

    public void incrMatchIndex() { // matchIndex永远<=nextIndex(集群稳定后并且没有写入新数据的情况下matchIndex=nextIndex)
        if (matchIndex < nextIndex) {
            matchIndex++;
        }
    }

    @Override
    public String toString() {
        return "ReplicateProgress{" +
                "matchIndex=" + matchIndex +
                ", nextIndex=" + nextIndex +
                '}';
    }
}
