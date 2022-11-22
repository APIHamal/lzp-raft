package com.lizhengpeng.lraft.core;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Endpoint endpoint = (Endpoint) o;
        return Objects.equals(host, endpoint.host) && Objects.equals(port, endpoint.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public String toString() {
        return "Endpoint{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
