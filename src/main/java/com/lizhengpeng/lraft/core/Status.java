package com.lizhengpeng.lraft.core;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * 异步任务执行结果
 * @author lzp
 */
@Setter
@Getter
@Builder
public class Status {

    private Boolean ok; // 任务是否正常执行

    private String reason; // 原因

    private String data;

    private Boolean redirect;

    /**
     * 判断任务是否执行成功
     * @return
     */
    public boolean onOk() {
        return ok == Boolean.TRUE;
    }

    /**
     * 判断当前是否需要进行leader重定向操作
     * @return
     */
    public boolean onRedirect() {
        return redirect == Boolean.TRUE;
    }

    /**
     * 创建一个失败的状态
     * @param reason
     */
    public static Status failed(String reason) {
        return Status.builder()
                .ok(Boolean.FALSE)
                .reason(reason)
                .build();
    }

    /**
     * 创建一个执行成功的状态
     * @param data
     */
    public static Status success(String data) {
        return Status.builder()
                .ok(Boolean.TRUE)
                .data(data)
                .build();
    }

    /**
     * 创建一个重定向的状态
     * @return
     */
    public static Status redirect() {
        return Status.builder()
                .ok(Boolean.FALSE)
                .redirect(Boolean.TRUE)
                .build();
    }

    @Override
    public String toString() {
        return "Status{" +
                "ok=" + ok +
                ", reason='" + reason + '\'' +
                ", data='" + data + '\'' +
                ", redirect=" + redirect +
                '}';
    }
}
