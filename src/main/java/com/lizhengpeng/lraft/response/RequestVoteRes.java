package com.lizhengpeng.lraft.response;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class RequestVoteRes {

    private long term;

    private Boolean success;

    @Override
    public String toString() {
        return "RequestVoteRes{" +
                "term=" + term +
                ", success=" + success +
                '}';
    }
}
