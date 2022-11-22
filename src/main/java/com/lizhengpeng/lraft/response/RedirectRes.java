package com.lizhengpeng.lraft.response;

import com.lizhengpeng.lraft.core.Endpoint;
import lombok.Getter;
import lombok.Setter;

/**
 * 重定向的响应
 * 在刷新完leader的信息后集群可能发生了leadership
 * 这个时候重定向到新的leader节点
 * @author lzp
 */
@Setter
@Getter
public class RedirectRes {

    private Boolean redirect;

    private Endpoint endpoint;

    private String reason;

    @Override
    public String toString() {
        return "RedirectRes{" +
                "redirect=" + redirect +
                ", endpoint=" + endpoint +
                ", reason='" + reason + '\'' +
                '}';
    }
}
