package org.catmq.entity;

import com.alibaba.fastjson2.JSON;

/**
 * @author BYL
 */
public interface ISerialization {

    /**
     * Serialize object to byte array
     *
     * @return byte array from object
     */
    byte[] toBytes();

    /**
     * Deserialize byte array to object
     *
     * @param bytes byte array
     * @param clazz class type
     * @return object
     */
    static <T> T fromBytes(byte[] bytes, Class<T> clazz) {
        return JSON.parseObject(bytes, clazz);
    }
}
