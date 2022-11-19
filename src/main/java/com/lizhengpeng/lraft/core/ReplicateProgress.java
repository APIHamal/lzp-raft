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

    public void decrMatchIndex() {
        if (matchIndex > 0) {
            matchIndex--;
        }
    }

    public void incrMatchIndex() {
        matchIndex++;
    }

}
