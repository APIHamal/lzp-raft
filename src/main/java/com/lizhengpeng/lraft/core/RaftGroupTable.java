package com.lizhengpeng.lraft.core;

import cn.hutool.core.collection.ListUtil;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * raft集群成员信息
 * @author lzp
 */
@Setter
@Getter
public class RaftGroupTable {

    private static final Logger logger = LoggerFactory.getLogger(RaftGroupTable.class);

    private ConcurrentHashMap<NodeId, Endpoint> groupTable = new ConcurrentHashMap<>();

    /**
     * 添加节点到表中
     * @param endpoint
     * @return
     */
    public NodeId addEndpoint(Endpoint endpoint) {
        NodeId nodeId = endpoint.getNodeId();
        groupTable.put(nodeId, endpoint);
        return nodeId;
    }

    /**
     * 根据成员的Id获取成员的地址信息
     * @param nodeId
     * @return
     */
    public Endpoint getEndpoint(NodeId nodeId) {
        return groupTable.get(nodeId);
    }

    /**
     * 获取所有成员的地址信息
     * @return
     */
    public List<Endpoint> getAllEndpoint() {
        return ListUtil.toList(groupTable.values());
    }

    /**
     * 获取成员中出了指定节点外的其他节点
     * @param nodeId
     * @return
     */
    public List<Endpoint> getBroadcastList(NodeId nodeId) {
        List<Endpoint> endpoints = new ArrayList<>();
        groupTable.entrySet().forEach(entry -> {
            if (!Objects.equals(nodeId, entry.getKey())) {
                endpoints.add(entry.getValue());
            }
        });
        return endpoints;
    }

    /**
     * 选举成功
     * @param voteCount
     * @return
     */
    public boolean electionSuccess(long voteCount) {
        logger.info("vote count => {}", voteCount);
        int halfCount = groupTable.size();
        if (halfCount % 2 == 0) {
            // 偶数节点的集群
            halfCount = (halfCount / 2) + 1;
        } else {
            // 奇数节点的集群
            halfCount = (halfCount + 1) / 2;
        }
        logger.info("vote count half => {}", halfCount);
        return voteCount >= halfCount;
    }
}
