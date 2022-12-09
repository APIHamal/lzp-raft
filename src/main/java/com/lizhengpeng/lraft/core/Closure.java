package com.lizhengpeng.lraft.core;

import lombok.Getter;
import lombok.Setter;

/**
 * 任务执行的回调
 * @author lzp
 */
@Setter
@Getter
public class Closure {

    private Boolean status;

    private String data;

    private String reason;

    private Boolean redirect;

    @Override
    public String toString() {
        return "Closure{" +
                "status=" + status +
                ", data='" + data + '\'' +
                ", reason='" + reason + '\'' +
                ", redirect=" + redirect +
                '}';
    }
}
