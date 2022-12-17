package com.lizhengpeng.lraft.core;

/**
 * 状态机接口
 * @author lzp
 */
public interface StateMachine {

    /**
     * 状态机提交任务
     * @param task
     * @param logIndex
     */
    void apply(Task task, Long logIndex);

    /**
     * 状态机写入当前的快照数据
     * @return
     */
    String writeSnapshot();

    /**
     * 应用快照数据到状态机中
     * @param snapshotData
     */
    void applySnapshot(String snapshotData);

}
