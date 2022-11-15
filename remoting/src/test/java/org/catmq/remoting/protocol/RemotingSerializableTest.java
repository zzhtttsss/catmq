/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.catmq.remoting.protocol;

import lombok.Data;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class RemotingSerializableTest {
    @Test
    public void testEncodeAndDecode_HeterogeneousClass() {
        Sample sample = new Sample();

        byte[] bytes = RemotingSerializable.encode(sample);
        Sample decodedSample = RemotingSerializable.decode(bytes, Sample.class);

        Assert.assertEquals(sample, decodedSample);
    }

    @Test
    public void testEncodeAndDecode_HomogeneousClass() {
        Sample2 sample = new Sample2();

        byte[] bytes = sample.encode();
        Sample2 decodedSample = RemotingSerializable.decode(bytes, Sample2.class);

        Assert.assertEquals(sample, decodedSample);
    }

    @Test
    public void testToJson_normalString() {
        RemotingSerializable serializable = new RemotingSerializable() {
            private List<String> stringList = Arrays.asList("a", "o", "e", "i", "u", "v");

            public List<String> getStringList() {
                return stringList;
            }

            public void setStringList(List<String> stringList) {
                this.stringList = stringList;
            }
        };

        String string = serializable.toJson();
        Assert.assertEquals("{\"stringList\":[\"a\",\"o\",\"e\",\"i\",\"u\",\"v\"]}", string);
    }

    @Test
    public void testToJson_prettyString() {
        RemotingSerializable serializable = new RemotingSerializable() {
            private List<String> stringList = Arrays.asList("a", "o", "e", "i", "u", "v");

            public List<String> getStringList() {
                return stringList;
            }

            public void setStringList(List<String> stringList) {
                this.stringList = stringList;
            }
        };

        String prettyString = serializable.toJson(true);

        Assert.assertEquals("{\n" +
                "\t\"stringList\":[\n" +
                "\t\t\"a\",\n" +
                "\t\t\"o\",\n" +
                "\t\t\"e\",\n" +
                "\t\t\"i\",\n" +
                "\t\t\"u\",\n" +
                "\t\t\"v\"\n" +
                "\t]\n" +
                "}", prettyString);
    }
}

@Data
class Sample {
    private String stringValue = "string";
    private int intValue = 2333;
    private Integer integerValue = 666;
    private double[] doubleArray = new double[]{0.618, 1.618};
    private List<String> stringList = Arrays.asList("a", "o", "e", "i", "u", "v");
    private Map<String, String> map = Collections.singletonMap("Hello", "World");
}

@Data
class Sample2 extends RemotingSerializable {
    private String stringValue = "string";
    private int intValue = 2333;
    private Integer integerValue = 666;
    private double[] doubleArray = new double[]{0.618, 1.618};
    private List<String> stringList = Arrays.asList("a", "o", "e", "i", "u", "v");
    private Map<String, String> map = Collections.singletonMap("Hello", "World");

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Sample2 sample = (Sample2) o;

        if (intValue != sample.intValue)
            return false;
        if (!Objects.equals(stringValue, sample.stringValue))
            return false;
        if (!Objects.equals(integerValue, sample.integerValue))
            return false;
        if (!Arrays.equals(doubleArray, sample.doubleArray))
            return false;
        if (!Objects.equals(stringList, sample.stringList))
            return false;
        return Objects.equals(map, sample.map);

    }

    @Override
    public int hashCode() {
        int result = stringValue != null ? stringValue.hashCode() : 0;
        result = 31 * result + intValue;
        result = 31 * result + (integerValue != null ? integerValue.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(doubleArray);
        result = 31 * result + (stringList != null ? stringList.hashCode() : 0);
        result = 31 * result + (map != null ? map.hashCode() : 0);
        return result;
    }
}