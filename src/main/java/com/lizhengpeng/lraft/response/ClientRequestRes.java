package com.lizhengpeng.lraft.response;

import lombok.Getter;
import lombok.Setter;

/**
 * 客户端请求raft的响应
 * @author lzp
 */
@Setter
@Getter
public class ClientRequestRes {

    private String reason;

    @Override
    public String toString() {
        return "ClientRequestRes{" +
                "reason='" + reason + '\'' +
                '}';
    }
}
