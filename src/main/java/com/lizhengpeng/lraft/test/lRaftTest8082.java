package com.lizhengpeng.lraft.test;

import com.lizhengpeng.lraft.core.Endpoint;
import com.lizhengpeng.lraft.core.RaftNode;
import com.lizhengpeng.lraft.core.RaftOptions;
import com.lizhengpeng.lraft.sample.kv.KvStoreStateMachine;

import java.util.ArrayList;
import java.util.List;

public class lRaftTest8082 {
    public static void main(String[] args) {
        Endpoint endpoint8080 = new Endpoint("127.0.0.1", 8080);
        Endpoint endpoint8081 = new Endpoint("127.0.0.1", 8081);
        Endpoint endpoint8082 = new Endpoint("127.0.0.1", 8082);
        List<Endpoint> endpoints = new ArrayList<>();
        endpoints.add(endpoint8080);
        endpoints.add(endpoint8081);
        endpoints.add(endpoint8082);
        // 8082端口启动raft节点
        RaftOptions raftOptions = new RaftOptions();
        raftOptions.setLogDir("C:\\raft_dir\\8082");
        new Thread(() -> {
            RaftNode raftNode = new RaftNode(raftOptions, new KvStoreStateMachine());
            raftNode.addGroupMember(endpoints);
            raftNode.startRaftServer(endpoint8082);
        }).start();
    }
}
