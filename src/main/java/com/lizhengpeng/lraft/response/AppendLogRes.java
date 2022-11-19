package com.lizhengpeng.lraft.response;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class AppendLogRes {

    private long term;

    private Boolean success;

    private String nodeId;

    @Override
    public String toString() {
        return "AppendLogRes{" +
                "term=" + term +
                ", success=" + success +
                '}';
    }
}
