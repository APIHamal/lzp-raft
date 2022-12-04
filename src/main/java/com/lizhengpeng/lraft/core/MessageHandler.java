package com.lizhengpeng.lraft.core;

import com.lizhengpeng.lraft.request.*;
import com.lizhengpeng.lraft.response.AppendLogRes;
import com.lizhengpeng.lraft.response.InstallSnapshotRes;
import com.lizhengpeng.lraft.response.RequestVoteRes;

/**
 * 用来处理接收到的rpc消息
 * 本质上就是一个回调的接口
 * @author lzp
 */
public interface MessageHandler {

    /**
     * 当前节点接收到心跳/日志同步
     * @param nodeId
     * @param appendLogMsg
     */
    void onAppendLog(NodeId nodeId, AppendLogMsg appendLogMsg);

    /**
     * 心跳/日志同步返回
     * @param appendLogRes
     */
    void onAppendLogCallback(AppendLogRes appendLogRes);

    /**
     * 节点接收到日志快照的复制请求
     * @param nodeId
     * @param installSnapshotMsg
     */
    void onInstallSnapshot(NodeId nodeId, InstallSnapshotMsg installSnapshotMsg);

    /**
     * 日志快照写入的回复请求
     * @param installSnapshotRes
     */
    void onInstallSnapshotCallback(InstallSnapshotRes installSnapshotRes);

    /**
     * 当前节点接收到投票请求
     * @param nodeId
     * @param requestVoteMsg
     */
    void onRequestVote(NodeId nodeId, RequestVoteMsg requestVoteMsg);

    /**
     * 接收到投票响应
     * @param requestVoteMsg
     */
    void onRequestVoteCallback(RequestVoteRes requestVoteMsg);

    /**
     * leader节点接收到日志时出发
     * @param appendLogEntry
     * @param rpcClient
     */
    void onLeaderAppendLog(ClientRequestMsg appendLogEntry, RpcClient rpcClient);

    /**
     * 客户端获取leader节点
     * @param refreshLeaderMsg
     * @param rpcClient
     */
    void onRefreshLeader(RefreshLeaderMsg refreshLeaderMsg, RpcClient rpcClient);

}
