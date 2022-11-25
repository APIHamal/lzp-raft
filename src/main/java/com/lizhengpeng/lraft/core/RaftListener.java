package com.lizhengpeng.lraft.core;

/**
 * 日志复制结果的回调接口
 * @author lzp
 */
public interface RaftListener {

    void trigger(Status status);

}
