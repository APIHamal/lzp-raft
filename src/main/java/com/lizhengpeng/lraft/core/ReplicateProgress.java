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

    private long lastReadLength; // 记录上一次读取的snapshot长度

    public void decrMatchIndex() {
        if (matchIndex > 0) {
            matchIndex--;
        }
    }

    public void incrMatchIndex() {
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
