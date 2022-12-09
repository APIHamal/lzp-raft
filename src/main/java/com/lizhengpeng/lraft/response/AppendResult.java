package com.lizhengpeng.lraft.response;

import lombok.Getter;
import lombok.Setter;

/**
 * 客户端请求raft的响应
 * @author lzp
 */
@Setter
@Getter
public class AppendResult {

    private Boolean status;

    private String reason;

    private Boolean redirect;

    @Override
    public String toString() {
        return "AppendResult{" +
                "status=" + status +
                ", reason='" + reason + '\'' +
                '}';
    }
}
