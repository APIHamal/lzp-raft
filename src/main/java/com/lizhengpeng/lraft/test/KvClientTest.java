package com.lizhengpeng.lraft.test;

import cn.hutool.core.util.StrUtil;
import com.lizhengpeng.lraft.core.Endpoint;
import com.lizhengpeng.lraft.core.RaftClient;
import com.lizhengpeng.lraft.core.Status;
import com.lizhengpeng.lraft.core.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class KvClientTest {
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
        // set key val或者get key的格式
        Scanner scanner = new Scanner(System.in);
        System.out.println("kv store >>>> ");
        String nextLine = scanner.nextLine();
        while (!StrUtil.equals(nextLine, "exit")) {
            Status status = raftClient.submitSync(Task.payload(nextLine));
            System.out.println("receive response => " + status);
            System.out.println("kv store >>>> ");
            nextLine = scanner.nextLine();
        }
        raftClient.shutdown();
    }
}
