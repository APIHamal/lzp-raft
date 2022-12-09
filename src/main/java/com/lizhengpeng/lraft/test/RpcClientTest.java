package com.lizhengpeng.lraft.test;

import com.lizhengpeng.lraft.core.Endpoint;
import com.lizhengpeng.lraft.core.RaftClient;
import com.lizhengpeng.lraft.core.Task;

import java.util.ArrayList;
import java.util.List;

public class RpcClientTest {
    public static void main(String[] args) throws InterruptedException {
        RaftClient raftClient = new RaftClient();
        Endpoint endpoint8080 = new Endpoint("127.0.0.1", 8080);
        Endpoint endpoint8081 = new Endpoint("127.0.0.1", 8081);
        Endpoint endpoint8082 = new Endpoint("127.0.0.1", 8082);
        List<Endpoint> endpoints = new ArrayList<>();
        endpoints.add(endpoint8080);
        endpoints.add(endpoint8081);
        endpoints.add(endpoint8082);
        raftClient.addEndpoints(endpoints);
        Task task = Task.payload("kv-cmd-" + System.currentTimeMillis());
        System.out.println(raftClient.submitSync(task));

    }
}
