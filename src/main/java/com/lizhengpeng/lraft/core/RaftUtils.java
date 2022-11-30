package com.lizhengpeng.lraft.core;

import com.alibaba.fastjson2.JSONObject;

import java.nio.charset.StandardCharsets;

/**
 * 相关的工具方法
 * @author lzp
 */
public class RaftUtils {

    /**
     * 判断两个值是否相等
     * @param source
     * @param dest
     * @return
     */
    public static boolean compare(Long source, Long dest) {
        return compare(source, dest, 0);
    }

    /**
     * 判断两个值是否相等
     * @param source
     * @param dest
     * @return
     */
    public static boolean great(Long source, Long dest) {
        return  source != null && dest != null && source.compareTo(dest) > 0;
    }

    /**
     * 判断两个值是否相等
     * @param source
     * @param dest
     * @return
     */
    public static boolean greatOrEquals(Long source, Long dest) {
        return  source != null && dest != null && source.compareTo(dest) >= 0;
    }

    /**
     * 判断两个值是否相等
     * @param source
     * @param dest
     * @return
     */
    public static boolean compare(Long source, Long dest, int compareRes) {
        return source != null && dest != null && source.compareTo(dest) == compareRes;
    }

    /**
     * 对象序列化成字节数组
     * @param obj
     * @return
     */
    public static byte[] writeObj(Object obj) {
        if (obj == null) {
            return new byte[0];
        }
        return JSONObject.toJSONString(obj).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 对象反序列化
     * @param buffer
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> T readObj(byte[] buffer, Class<T> clazz) {
        return JSONObject.parseObject(new String(buffer, StandardCharsets.UTF_8), clazz);
    }

}
