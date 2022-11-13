package com.lizhengpeng.lraft.core;

import cn.hutool.core.util.StrUtil;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

/**
 * raft节点的标识
 * @author lzp
 */
@Setter
@Getter
public class NodeId {

    public static final NodeId DEFAULT = new NodeId("-1");

    private String nodeId;

    public NodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public static NodeId of(String nodeId) {
        if (StrUtil.isBlank(nodeId)) {
            return DEFAULT;
        }
        return new NodeId(nodeId);
    }

    public String getText() {
        return nodeId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj != null && (obj instanceof NodeId)) {
            NodeId other = (NodeId) obj;
            return StrUtil.isNotBlank(nodeId) && StrUtil.equals(nodeId, other.getNodeId());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId);
    }

    @Override
    public String toString() {
        return "NodeId{" +
                "nodeId='" + nodeId + '\'' +
                '}';
    }
}
