package com.lizhengpeng.lraft.response;

import com.lizhengpeng.lraft.core.Endpoint;
import lombok.Getter;
import lombok.Setter;

/**
 * 返回获取leader的响应
 * @author lzp
 */
@Setter
@Getter
public class RefreshLeaderRes {

    private Boolean refreshed; // 是否刷新成功

    private Endpoint endpoint; // leader节点的地址

    private String errorMsg; // 获取失败的错误原因(当前没有leader节点[正在选举|或者发生了leaderShip])

    @Override
    public String toString() {
        return "RefreshLeaderRes{" +
                "refreshed=" + refreshed +
                ", endpoint=" + endpoint +
                ", errorMsg='" + errorMsg + '\'' +
                '}';
    }
}
