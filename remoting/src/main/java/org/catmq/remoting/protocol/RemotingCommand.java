package org.catmq.remoting.protocol;

import com.alibaba.fastjson2.annotation.JSONField;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import org.catmq.remoting.CommandCustomHeader;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

@Data
public class RemotingCommand {

    static final Logger log = Logger.getLogger("RemotingCommand");
    private static final int RPC_TYPE = 0; // 0, REQUEST_COMMAND 1, RESPONSE_COMMAND
    private static final int RPC_ONEWAY = 1; // 0, RPC 1, Oneway
    private static final Map<Class, String> CANONICAL_NAME_CACHE = new HashMap<>();
    private static final Map<Field, Boolean> NULLABLE_FIELD_CACHE = new HashMap<>();
    private static final String STRING_CANONICAL_NAME = String.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_1 = Boolean.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_2 = boolean.class.getCanonicalName();
    private static final Map<Class<? extends CommandCustomHeader>, Field[]> CLASS_HASH_MAP = new HashMap<>();
    private static AtomicInteger REQUEST_ID = new AtomicInteger(0);

    /**
     * Request: OperationCode
     * Response: 0 represents success
     * not-0 represents failure
     */
    private int code;
    /**
     * Request: requestId that is used to distinguish different request
     * on the same connection
     * Response: the same as Request
     */
    private int requestId = REQUEST_ID.getAndIncrement();
    /**
     * Request：Normal RPC or Oneway RPC
     * Response：the same as Request
     */
    private int flag = 0;
    /**
     * Request：Additional Text
     * Response：the same as Request
     */
    private String remark;
    /**
     * Request：Custom extended information
     * Response：the same as Request
     */
    private HashMap<String, String> extFields;
    private transient CommandCustomHeader customHeader;
    private transient byte[] body;

    protected RemotingCommand() {
    }

    public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.customHeader = customHeader;
        return cmd;
    }

    public static RemotingCommand createResponseCommandWithHeader(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.markResponseType();
        cmd.customHeader = customHeader;
        return cmd;
    }

    public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
        return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "not set any response code", classHeader);
    }

    public static RemotingCommand createErrorResponse(int code, String remark,
                                                      Class<? extends CommandCustomHeader> classHeader) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(classHeader);
        response.setCode(code);
        response.setRemark(remark);
        return response;
    }

    public static RemotingCommand createErrorResponse(int code, String remark) {
        return createErrorResponse(code, remark, null);
    }

    public static RemotingCommand createResponseCommand(int code, String remark,
                                                        Class<? extends CommandCustomHeader> classHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.markResponseType();
        cmd.setCode(code);
        cmd.setRemark(remark);

        if (classHeader != null) {
            try {
                cmd.customHeader = classHeader.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                     NoSuchMethodException e) {
                return null;
            }
        }

        return cmd;
    }

    public static RemotingCommand createResponseCommand(int code, String remark) {
        return createResponseCommand(code, remark, null);
    }

    public static RemotingCommand decode(final byte[] array) throws Exception {
        return decode(Unpooled.wrappedBuffer(array));
    }

    public static RemotingCommand decode(final ByteBuffer buffer) throws Exception {
        return decode(Unpooled.wrappedBuffer(buffer));
    }


    public static RemotingCommand decode(final ByteBuf byteBuffer) throws Exception {
        int length = byteBuffer.readableBytes();
        // 1. total length of this buffer
        int oriHeaderLen = byteBuffer.readInt();
        // 2. length of header in this buffer
        int headerLength = getHeaderLength(oriHeaderLen);
        if (headerLength > length - 4) {
            throw new Exception("decode error, bad header length: " + headerLength);
        }

        RemotingCommand cmd = headerDecode(byteBuffer, headerLength);

        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            byteBuffer.readBytes(bodyData);
        }
        cmd.body = bodyData;

        return cmd;
    }

    public static int getHeaderLength(int length) {
        return length & 0xFFFFFF;
    }

    private static RemotingCommand headerDecode(ByteBuf byteBuffer, int len) throws Exception {
        byte[] headerData = new byte[len];
        byteBuffer.readBytes(headerData);
        return RemotingSerializable.decode(headerData, RemotingCommand.class);
    }

    public void markResponseType() {
        int bits = 1 << RPC_TYPE;
        this.flag |= bits;
    }

    public CommandCustomHeader readCustomHeader() {
        return customHeader;
    }

    public void writeCustomHeader(CommandCustomHeader customHeader) {
        this.customHeader = customHeader;
    }

    public CommandCustomHeader decodeCommandCustomHeader(
            Class<? extends CommandCustomHeader> classHeader) throws Exception {
        return decodeCommandCustomHeader(classHeader, true);
    }

    public CommandCustomHeader decodeCommandCustomHeader(Class<? extends CommandCustomHeader> classHeader,
                                                         boolean useFastEncode) throws Exception {
        CommandCustomHeader objectHeader;
        try {
            objectHeader = classHeader.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException e) {
            return null;
        }

        if (this.extFields != null) {
            Field[] fields = getClazzFields(classHeader);
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String fieldName = field.getName();
                    if (!fieldName.startsWith("this")) {
                        try {
                            String value = this.extFields.get(fieldName);
                            if (null == value) {
                                log.warning("the custom field <" + fieldName + "> is null");
                                continue;
                            }

                            field.setAccessible(true);
                            String type = getCanonicalName(field.getType());
                            Object valueParsed;

                            if (type.equals(STRING_CANONICAL_NAME)) {
                                valueParsed = value;
                            } else if (type.equals(INTEGER_CANONICAL_NAME_1) || type.equals(INTEGER_CANONICAL_NAME_2)) {
                                valueParsed = Integer.parseInt(value);
                            } else if (type.equals(LONG_CANONICAL_NAME_1) || type.equals(LONG_CANONICAL_NAME_2)) {
                                valueParsed = Long.parseLong(value);
                            } else if (type.equals(BOOLEAN_CANONICAL_NAME_1) || type.equals(BOOLEAN_CANONICAL_NAME_2)) {
                                valueParsed = Boolean.parseBoolean(value);
                            } else if (type.equals(DOUBLE_CANONICAL_NAME_1) || type.equals(DOUBLE_CANONICAL_NAME_2)) {
                                valueParsed = Double.parseDouble(value);
                            } else {
                                throw new Exception("the custom field <" + fieldName + "> type is not supported");
                            }

                            field.set(objectHeader, valueParsed);

                        } catch (Throwable e) {
                            log.warning(String.format("Failed field [%s] decoding.%s", fieldName, e));
                        }
                    }
                }
            }

            objectHeader.checkFields();
        }

        return objectHeader;
    }

    //make it able to test
    Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader) {
        Field[] field = CLASS_HASH_MAP.get(classHeader);

        if (field == null) {
            Set<Field> fieldList = new HashSet<>();
            for (Class className = classHeader; className != Object.class; className = className.getSuperclass()) {
                Field[] fields = className.getDeclaredFields();
                fieldList.addAll(Arrays.asList(fields));
            }
            field = fieldList.toArray(new Field[0]);
            synchronized (CLASS_HASH_MAP) {
                CLASS_HASH_MAP.put(classHeader, field);
            }
        }
        return field;
    }

    private String getCanonicalName(Class clazz) {
        String name = CANONICAL_NAME_CACHE.get(clazz);

        if (name == null) {
            name = clazz.getCanonicalName();
            synchronized (CANONICAL_NAME_CACHE) {
                CANONICAL_NAME_CACHE.put(clazz, name);
            }
        }
        return name;
    }

    public ByteBuffer encode() {
        // 1 header length size
        int length = 4;

        // 2 header data length
        byte[] headerData = this.headerEncode();
        length += headerData.length;

        // 3 body data length
        if (this.body != null) {
            length += body.length;
        }

        ByteBuffer result = ByteBuffer.allocate(4 + length);

        // length
        result.putInt(length);

        // header length
        result.putInt(headerData.length);

        // header data
        result.put(headerData);

        // body data;
        if (this.body != null) {
            result.put(this.body);
        }

        result.flip();

        return result;
    }

    private byte[] headerEncode() {
        this.makeCustomHeaderToNet();
        return RemotingSerializable.encode(this);
    }

    public void makeCustomHeaderToNet() {
        if (this.customHeader != null) {
            Field[] fields = getClazzFields(customHeader.getClass());
            if (null == this.extFields) {
                this.extFields = new HashMap<>();
            }

            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String name = field.getName();
                    if (!name.startsWith("this")) {
                        Object value = null;
                        try {
                            field.setAccessible(true);
                            value = field.get(this.customHeader);
                        } catch (Exception e) {
                            log.warning(String.format("Failed to access field [{%s}],%s", name, e));
                        }

                        if (value != null) {
                            this.extFields.put(name, value.toString());
                        }
                    }
                }
            }
        }
    }

    public void fastEncodeHeader(ByteBuf out) {
        int bodySize = this.body != null ? this.body.length : 0;
        int beginIndex = out.writerIndex();
        // skip 8 bytes
        out.writeLong(0);
        int headerSize;
        this.makeCustomHeaderToNet();
        byte[] header = RemotingSerializable.encode(this);
        headerSize = header.length;
        out.writeBytes(header);
        out.setInt(beginIndex, 4 + headerSize + bodySize);
        out.setInt(beginIndex + 4, headerSize);
    }

    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body != null ? this.body.length : 0);
    }

    public ByteBuffer encodeHeader(final int bodyLength) {
        // 1 header length size
        int length = 4;
        // 2 header data length
        byte[] headerData;
        headerData = this.headerEncode();
        length += headerData.length;
        // 3 body data length
        length += bodyLength;
        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);
        // length
        result.putInt(length);
        // header length
        result.putInt(headerData.length);
        // header data
        result.put(headerData);
        result.flip();
        return result;
    }

    public void markOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        this.flag |= bits;
    }

    @JSONField(serialize = false)
    public boolean isOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        return (this.flag & bits) == bits;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    @JSONField(serialize = false)
    public RemotingCommandType getType() {
        if (this.isResponseType()) {
            return RemotingCommandType.RESPONSE_COMMAND;
        }

        return RemotingCommandType.REQUEST_COMMAND;
    }

    @JSONField(serialize = false)
    public boolean isResponseType() {
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }

    public void addExtField(String key, String value) {
        if (null == extFields) {
            extFields = new HashMap<>();
        }
        extFields.put(key, value);
    }
}