package com.lizhengpeng.lraft.core;

/**
 * 状态机接口
 * @author lzp
 */
public interface StateMachine {

    /**
     * 状态机应用命令
     * @param command
     * @param logIndex
     */
    void apply(String command, Long logIndex);

    /**
     * 生成日志快照
     * @param snapshotWriter
     */
    void writeSnapshot(SnapshotWriter snapshotWriter);

    /**
     * 读取日志快照
     * @param snapshot
     */
    void readSnapshot(Snapshot snapshot);

}
