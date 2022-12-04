package com.lizhengpeng.lraft.core;

import lombok.Getter;
import lombok.Setter;

/**
 * raft集群的元数据管理
 * @author lzp
 */
@Setter
@Getter
public class RaftMeta {

    private long currentTerm; // 当前集群的任期

    private long voteFor; // 当前节点给哪个节点进行过投票

    private long lastLogIndex; // 写入文件中的最后一条日志的索引

    private long committedIndex; // 集群的提交进度(raft规定只能提交自己term内的日志)

    private long firstLogIndex; // 加入了日志快照后需要裁剪日志数据

    @Override
    public String toString() {
        return "RaftMeta{" +
                "currentTerm=" + currentTerm +
                ", voteFor=" + voteFor +
                ", lastLogIndex=" + lastLogIndex +
                ", committedIndex=" + committedIndex +
                '}';
    }
}
