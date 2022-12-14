package com.lizhengpeng.lraft.core;

import lombok.Getter;
import lombok.Setter;

/**
 * raft相关的配置
 * @author lzp
 */
@Setter
@Getter
public class RaftOptions {

    private String logDir = "./"; // 默认的文件存储位置

    private int minElectionTimeout = 1000; // 最小选举超时时间

    private int maxElectionTimeout = 2000; // 最大选举超时时间

    private int heartbeatInterval = 80; // 心跳时间间隔

    private int connectTimeout = 20; // 连接超时时间

    private int refreshLeaderTimeout = 1000 * 10; // 刷新leader节点的超时时间

    private long logFileMaxSize = 1024 * 64; // 1MB

    private long writeTimeout = 1000 * 5; // 5秒超时

    private int cleanTaskInterval = 100; // 清理任务的执行时间

    private int snapshotTaskInterval = 1000; // 创建快照的时间间隔

    @Override
    public String toString() {
        return "RaftOptions{" +
                "logDir='" + logDir + '\'' +
                ", minElectionTimeout=" + minElectionTimeout +
                ", maxElectionTimeout=" + maxElectionTimeout +
                ", heartbeatInterval=" + heartbeatInterval +
                ", connectTimeout=" + connectTimeout +
                ", refreshLeaderTimeout=" + refreshLeaderTimeout +
                ", logFileMaxSize=" + logFileMaxSize +
                ", writeTimeout=" + writeTimeout +
                '}';
    }

}
