package com.lizhengpeng.lraft.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

/**
 * 文件流的工具类
 * @author lzp
 */
public class AccessUtils {

    /**
     * 关闭指定的输出流对象
     * @param accessFile
     */
    public static void closeAccessFile(RandomAccessFile accessFile) {
        try {
            if (accessFile != null) {
                accessFile.close();
            }
        } catch (Exception e) {
            // Ignore Exception
        }
    }

    /**
     * 获取指定的文件输入/输出流
     * @param file
     * @return
     */
    public static RandomAccessFile openRandomAccess(File file) throws FileNotFoundException {
        return new RandomAccessFile(file, "rws");
    }

}
