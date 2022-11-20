package com.lizhengpeng.lraft.request;

import lombok.Getter;
import lombok.Setter;

/**
 * 追加日志的请求
 * @author lzp
 */
@Setter
@Getter
public class ClientRequestMsg {

    private String msg;

    @Override
    public String toString() {
        return "ClientRequestMsg{" +
                "msg='" + msg + '\'' +
                '}';
    }
}
