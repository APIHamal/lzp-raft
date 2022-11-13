package com.lizhengpeng.lraft.core;

import lombok.Getter;
import lombok.Setter;

/**
 * raft节点的地址
 * @author lzp
 */
@Setter
@Getter
public class Endpoint {

    private String host;

    private Integer port;

    public Endpoint(String host, Integer port) {
        this.host = host;
        this.port = port;
    }

    public NodeId getNodeId() {
        return NodeId.of(host + ":" + port);
    }

    @Override
    public String toString() {
        return "Endpoint{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
