package com.lizhengpeng.lraft.core;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * 日志快照元数据
 * @author lzp
 */
@Setter
@Getter
@Builder
public class SnapshotMeta {

    private long lastLogIndex;

    private long lastLogTerm;

}
