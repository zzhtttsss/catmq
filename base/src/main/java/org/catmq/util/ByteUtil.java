package org.catmq.util;

import java.nio.ByteBuffer;

public class ByteUtil {

    public static byte[] convLong2Bytes(long... longs) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(longs.length * Long.BYTES);
        for (long l: longs) {
            byteBuffer.putLong(l);
        }
        return byteBuffer.array();
    }

    public static long getLong(byte[] bytes, int index) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes, index, bytes.length);
        buffer.flip();
        return buffer.getLong();
    }
}
