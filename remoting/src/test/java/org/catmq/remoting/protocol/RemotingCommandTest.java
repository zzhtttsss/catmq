package org.catmq.remoting.protocol;

import lombok.Data;
import org.catmq.remoting.CommandCustomHeader;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class RemotingCommandTest {
    @Test
    public void testCreateRequestCommand_RegisterBroker() {
        int code = 103;
        CommandCustomHeader header = new SampleCommandCustomHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);
        Assert.assertEquals(103, cmd.getCode());
        //flag bit 0: 0 presents request
        Assert.assertEquals(0, cmd.getFlag() & 0x01);
    }

    @Test
    public void testCreateResponseCommand_SuccessWithHeader() {
        int code = RemotingSysResponseCode.SUCCESS;
        String remark = "Sample remark";
        RemotingCommand cmd = RemotingCommand.createResponseCommand(code, remark, SampleCommandCustomHeader.class);
        Assert.assertNotNull(cmd);
        Assert.assertEquals(RemotingSysResponseCode.SUCCESS, cmd.getCode());
        Assert.assertEquals(remark, cmd.getRemark());
        //flag bit 0: 1 presents response
        Assert.assertEquals(1, cmd.getFlag() & 0x01);
    }

    @Test
    public void testCreateResponseCommand_SuccessWithoutHeader() {
        int code = RemotingSysResponseCode.SUCCESS;
        String remark = "Sample remark";
        RemotingCommand cmd = RemotingCommand.createResponseCommand(code, remark);
        Assert.assertEquals(RemotingSysResponseCode.SUCCESS, cmd.getCode());
        Assert.assertEquals(remark, cmd.getRemark());
        //flag bit 0: 1 presents response
        Assert.assertEquals(1, cmd.getFlag() & 0x01);
    }

    @Test
    public void testCreateResponseCommand_FailToCreateCommand() {
        int code = RemotingSysResponseCode.SUCCESS;
        String remark = "Sample remark";
        RemotingCommand cmd = RemotingCommand.createResponseCommand(code, remark, CommandCustomHeader.class);
        Assert.assertNull(cmd);
    }

    @Test
    public void testCreateResponseCommand_SystemError() {
        RemotingCommand cmd = RemotingCommand.createResponseCommand(SampleCommandCustomHeader.class);
        Assert.assertEquals(RemotingSysResponseCode.SYSTEM_ERROR, cmd.getCode());
        Assert.assertEquals("not set any response code", cmd.getRemark());
        Assert.assertEquals(1, cmd.getFlag() & 0x01);
    }

    @Test
    public void testEncodeAndDecode_EmptyBody() {
        int code = 88;
        CommandCustomHeader header = new SampleCommandCustomHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);

        ByteBuffer buffer = cmd.encode();

        //Simulate buffer being read in NettyDecoder
        buffer.getInt();
        byte[] bytes = new byte[buffer.limit() - 4];
        buffer.get(bytes, 0, buffer.limit() - 4);
        buffer = ByteBuffer.wrap(bytes);

        RemotingCommand decodedCommand = null;
        try {
            decodedCommand = RemotingCommand.decode(buffer);
            Assert.assertNull(decodedCommand.getBody());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Should not throw Exception");
        }
    }

    @Test
    public void testEncodeAndDecode_FilledBody() {
        int code = 88;
        CommandCustomHeader header = new SampleCommandCustomHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);
        cmd.setBody(new byte[]{0, 1, 2, 3, 4});

        ByteBuffer buffer = cmd.encode();

        //Simulate buffer being read in NettyDecoder
        buffer.getInt();
        byte[] bytes = new byte[buffer.limit() - 4];
        buffer.get(bytes, 0, buffer.limit() - 4);
        buffer = ByteBuffer.wrap(bytes);

        RemotingCommand decodedCommand = null;
        try {
            decodedCommand = RemotingCommand.decode(buffer);
            Assert.assertArrayEquals(new byte[]{0, 1, 2, 3, 4}, decodedCommand.getBody());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Should not throw IOException");
        }
    }

    @Test
    public void testEncodeAndDecode_FilledBodyWithExtFields() throws Exception {
        int code = 88;
        CommandCustomHeader header = new ExtFieldsHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);

        cmd.addExtField("key", "value");

        ByteBuffer buffer = cmd.encode();

        //Simulate buffer being read in NettyDecoder
        buffer.getInt();
        byte[] bytes = new byte[buffer.limit() - 4];
        buffer.get(bytes, 0, buffer.limit() - 4);
        buffer = ByteBuffer.wrap(bytes);

        RemotingCommand decodedCommand = null;
        try {
            decodedCommand = RemotingCommand.decode(buffer);
            Assert.assertEquals("bilibili", decodedCommand.getExtFields().get("stringValue"));
            Assert.assertEquals("2333", decodedCommand.getExtFields().get("intValue"));
            Assert.assertEquals("23333333", decodedCommand.getExtFields().get("longValue"));
            Assert.assertEquals("true", decodedCommand.getExtFields().get("booleanValue"));
            Assert.assertEquals("0.618", decodedCommand.getExtFields().get("doubleValue"));
            Assert.assertEquals("value", decodedCommand.getExtFields().get("key"));

            ExtFieldsHeader decodedHeader = (ExtFieldsHeader) decodedCommand.decodeCommandCustomHeader(ExtFieldsHeader.class);
            Assert.assertEquals("bilibili", decodedHeader.getStringValue());
            Assert.assertEquals(2333, decodedHeader.getIntValue());
            Assert.assertEquals(23333333L, decodedHeader.getLongValue());
            Assert.assertTrue(decodedHeader.isBooleanValue());
            Assert.assertTrue(Math.abs(decodedHeader.getDoubleValue() - 0.618) < 1e-5);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Should not throw Exception");
        }
    }


    @Test
    public void testParentField() throws Exception {
        SubExtFieldsHeader subExtFieldsHeader = new SubExtFieldsHeader();
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(1, subExtFieldsHeader);
        Field[] fields = remotingCommand.getClazzFields(subExtFieldsHeader.getClass());
        Set<String> fieldNames = new HashSet<>();
        for (Field field : fields) {
            fieldNames.add(field.getName());
        }
        Assert.assertTrue(fields.length >= 7);
        Set<String> names = new HashSet<>();
        names.add("stringValue");
        names.add("intValue");
        names.add("longValue");
        names.add("booleanValue");
        names.add("doubleValue");
        names.add("name");
        names.add("value");
        for (String name : names) {
            Assert.assertTrue(fieldNames.contains(name));
        }
        remotingCommand.makeCustomHeaderToNet();
        SubExtFieldsHeader other = (SubExtFieldsHeader) remotingCommand.decodeCommandCustomHeader(subExtFieldsHeader.getClass());
        Assert.assertEquals(other, subExtFieldsHeader);
    }
}

class SampleCommandCustomHeader implements CommandCustomHeader {
    @Override
    public void checkFields() throws Exception {
    }
}

@Data
class ExtFieldsHeader implements CommandCustomHeader {
    private String stringValue = "bilibili";
    private int intValue = 2333;
    private long longValue = 23333333L;
    private boolean booleanValue = true;
    private double doubleValue = 0.618;

    @Override
    public void checkFields() throws Exception {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExtFieldsHeader)) return false;

        ExtFieldsHeader that = (ExtFieldsHeader) o;

        if (intValue != that.intValue) return false;
        if (longValue != that.longValue) return false;
        if (booleanValue != that.booleanValue) return false;
        if (Double.compare(that.doubleValue, doubleValue) != 0) return false;
        return Objects.equals(stringValue, that.stringValue);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = stringValue != null ? stringValue.hashCode() : 0;
        result = 31 * result + intValue;
        result = 31 * result + (int) (longValue ^ (longValue >>> 32));
        result = 31 * result + (booleanValue ? 1 : 0);
        temp = Double.doubleToLongBits(doubleValue);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}

@Data
class SubExtFieldsHeader extends ExtFieldsHeader {
    private String name = "12321";
    private int value = 111;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SubExtFieldsHeader)) return false;
        if (!super.equals(o)) return false;

        SubExtFieldsHeader that = (SubExtFieldsHeader) o;

        if (value != that.value) return false;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + value;
        return result;
    }
}