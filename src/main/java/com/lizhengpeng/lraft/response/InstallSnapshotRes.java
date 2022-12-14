package com.lizhengpeng.lraft.response;

import lombok.Getter;
import lombok.Setter;

/**
 * 快照安装的响应
 * @author lzp
 */
@Setter
@Getter
public class InstallSnapshotRes {

    private long term; // 任期号

    private String nodeId; // 节点的Id

    private Boolean success; // 是否安装成功

    private Boolean finished; // 整个快照复制完成

    @Override
    public String toString() {
        return "InstallSnapshotRes{" +
                "term=" + term +
                ", nodeId='" + nodeId + '\'' +
                ", success=" + success +
                ", finished=" + finished +
                '}';
    }
}
