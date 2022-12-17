package com.lizhengpeng.lraft.sample.kv;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSONObject;
import com.lizhengpeng.lraft.core.RpcClient;
import com.lizhengpeng.lraft.core.StateMachine;
import com.lizhengpeng.lraft.core.Status;
import com.lizhengpeng.lraft.core.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * kv键值存储的状态机
 */
public class KvStoreStateMachine implements StateMachine {

    private static final Logger logger = LoggerFactory.getLogger(KvStoreStateMachine.class);

    private static final ConcurrentHashMap<String, String> kvStore = new ConcurrentHashMap<>();

    /**
     * 应用状态机数据
     * @param task
     * @param logIndex
     */
    @Override
    public void apply(Task task, Long logIndex) {
        logger.info("KvStore 状态机执行  => {}  raftLog索引 => {}", task, logIndex);
        RpcClient rpcClient = task.getRpcClient(); // 注意只有leader节点才持有rpcClient对象
        String[] command = task.getData().split(" ");
        if (command.length != 3 && command.length != 2) {
            rpcClient.sendMessage(Status.failed("command error"));
            return;
        }
        if (command[0].equals("set")) {
            kvStore.put(command[1], command[2]);
            rpcClient.sendMessage(Status.success("set key success"));
        } else if (command[0].equals("get")) {
            String val = kvStore.get(command[1]);
            if (StrUtil.isBlank(val)) {
                rpcClient.sendMessage(Status.failed("key not exists"));
            } else {
                rpcClient.sendMessage(Status.success("get response => " + val));
            }
        } else {
            rpcClient.sendMessage("un support command!!!");
        }
    }

    /**
     * 写出快照数据
     * @return
     */
    @Override
    public String writeSnapshot() {
        return JSONObject.toJSONString(kvStore); // 将当前的内容转为字符串写出
    }
}
