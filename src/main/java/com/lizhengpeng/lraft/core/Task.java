package com.lizhengpeng.lraft.core;

import cn.hutool.core.util.StrUtil;
import lombok.Getter;

import java.util.Objects;
import java.util.UUID;

/**
 * raft中任务的抽象
 * 客户端提交到raft中的数据统一抽象成为一个任务
 * @author lzp
 */
@Getter
public class Task {

    /**
     * 任务唯一的Id标识
     * 服务端根据此Id来标识不同的任务
     */
    private final String uuid;

    /**
     * 客户端对象
     */
    private RpcClient rpcClient;

    /**
     * 任务的Id属性
     */
    private String data; // 任务携带的数据

    /**
     * 绑定客户端对象
     * @param rpcClient
     */
    void bind(RpcClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    /**
     * 私有的构造函数
     * 主要是为了创建任务的时候生成Id
     * @param data
     */
    private Task(String data) {
        uuid = UUID.randomUUID().toString().replace("-","");
        this.data = data;
    }

    /**
     * 返回持有的rpc客户端对象
     * @return
     */
    public RpcClient getRpcClient() {
        if (rpcClient != null) {
            return rpcClient;
        }
        return RpcClient.NO_OP;
    }

    /**
     * 生成一个新的任务并且携带数据
     * @param data
     * @return
     */
    public static Task payload(String data) {
        return new Task(data);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj != null && (obj instanceof Task)) {
            Task task = (Task) obj;
            return StrUtil.isNotBlank(uuid) && StrUtil.equals(uuid, task.getUuid());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid);
    }

    @Override
    public String toString() {
        return "Task{" +
                "uuid='" + uuid + '\'' +
                ", rpcClient=" + rpcClient +
                ", data='" + data + '\'' +
                '}';
    }
}
