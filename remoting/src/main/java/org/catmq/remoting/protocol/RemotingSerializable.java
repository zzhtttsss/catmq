package org.catmq.remoting.protocol;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public abstract class RemotingSerializable {
    private final static Charset CHARSET_UTF8 = StandardCharsets.UTF_8;

    public static byte[] encode(final Object obj) {
        final String json = toJson(obj, false);
        if (json != null) {
            return json.getBytes(CHARSET_UTF8);
        }
        return null;
    }

    public static String toJson(final Object obj, boolean prettyFormat) {
        return prettyFormat ? JSON.toJSONString(obj, JSONWriter.Feature.PrettyFormat) : JSON.toJSONString(obj);
    }

    public static <T> T decode(final byte[] data, Class<T> classOfT) {
        return fromJson(data, classOfT);
    }

    public static <T> T fromJson(String json, Class<T> classOfT) {
        return JSON.parseObject(json, classOfT);
    }

    private static <T> T fromJson(byte[] data, Class<T> classOfT) {
        return JSON.parseObject(data, classOfT);
    }

    public byte[] encode() {
        final String json = this.toJson();
        if (json != null) {
            return json.getBytes(CHARSET_UTF8);
        }
        return null;
    }

    /**
     * Allow call-site to apply specific features according to their requirements.
     *
     * @param features Features to apply
     * @return serialized data.
     */
    public byte[] encode(JSONWriter.Feature... features) {
        final String json = JSON.toJSONString(this, features);
        return json.getBytes(CHARSET_UTF8);
    }

    public String toJson() {
        return toJson(false);
    }

    public String toJson(final boolean prettyFormat) {
        return toJson(this, prettyFormat);
    }
}
