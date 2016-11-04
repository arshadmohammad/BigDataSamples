/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.inspector.encryption;

import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.util.Random;

/**
 *
 */
public class HbaseDataEncryptionManager implements DataEncryptionManager {
    /**
     * Size of boolean in bytes
     */
    public static final int SIZEOF_BOOLEAN = Byte.SIZE / Byte.SIZE;

    /**
     * Size of byte in bytes
     */
    public static final int SIZEOF_BYTE = SIZEOF_BOOLEAN;

    public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;
    // the magic number is to be backward compatible
    private static final byte MAGIC = (byte) 0XFF;
    private static final int MAGIC_SIZE = SIZEOF_BYTE;
    private static final int ID_LENGTH_OFFSET = MAGIC_SIZE;
    private static final int ID_LENGTH_SIZE = SIZEOF_INT;

    private final byte[] id;

    {
        String identifier = ManagementFactory.getRuntimeMXBean().getName();
        ;
        this.id = toBytes(identifier);
    }

    private final Random salter = new Random();

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.zookeeper.inspector.encryption.DataEncryptionManager#
     * decryptData (byte[])
     */
    public String decryptData(byte[] encrypted) throws Exception {
        return new String(removeMetaData(encrypted));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.zookeeper.inspector.encryption.DataEncryptionManager#
     * encryptData (java.lang.String)
     */
    public byte[] encryptData(String data) throws Exception {
        
        if (data == null) {
            return appendMetaData(new byte[0]);
        }
        return appendMetaData(data.getBytes());
    }

    public byte[] removeMetaData(byte[] data) {
        if (data == null || data.length == 0) {
            return data;
        }
        // check the magic data; to be backward compatible
        byte magic = data[0];
        if (magic != MAGIC) {
            return data;
        }

        int idLength = toInt(data, ID_LENGTH_OFFSET);
        int dataLength = data.length - MAGIC_SIZE - ID_LENGTH_SIZE - idLength;
        int dataOffset = MAGIC_SIZE + ID_LENGTH_SIZE + idLength;

        byte[] newData = new byte[dataLength];
        System.arraycopy(data, dataOffset, newData, 0, dataLength);
        return newData;
    }

    /**
     * Converts a byte array to an int value
     * 
     * @param bytes
     *            byte array
     * @param offset
     *            offset into array
     * @return the int value
     */
    public static int toInt(byte[] bytes, int offset) {
        return toInt(bytes, offset, SIZEOF_INT);
    }

    /**
     * Converts a byte array to an int value
     * 
     * @param bytes
     *            byte array
     * @param offset
     *            offset into array
     * @param length
     *            length of int (has to be {@link #SIZEOF_INT})
     * @return the int value
     * @throws IllegalArgumentException
     *             if length is not {@link #SIZEOF_INT} or if there's not enough
     *             room in the array at the offset indicated.
     */
    public static int toInt(byte[] bytes, int offset, final int length) {
        if (length != SIZEOF_INT || offset + length > bytes.length) {
            throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_INT);
        }
        int n = 0;
        for (int i = offset; i < (offset + length); i++) {
            n <<= 8;
            n ^= bytes[i] & 0xFF;
        }
        return n;
    }

    private static IllegalArgumentException explainWrongLengthOrOffset(final byte[] bytes, final int offset,
            final int length, final int expectedLength) {
        String reason;
        if (length != expectedLength) {
            reason = "Wrong length: " + length + ", expected " + expectedLength;
        } else {
            reason = "offset (" + offset + ") + length (" + length + ") exceed the" + " capacity of the array: "
                    + bytes.length;
        }
        return new IllegalArgumentException(reason);
    }

    private byte[] appendMetaData(byte[] data) {
        if (data == null || data.length == 0) {
            return data;
        }
        byte[] salt = toBytes(salter.nextLong());
        int idLength = id.length + salt.length;
        byte[] newData = new byte[MAGIC_SIZE + ID_LENGTH_SIZE + idLength + data.length];
        int pos = 0;
        pos = putByte(newData, pos, MAGIC);
        pos = putInt(newData, pos, idLength);
        pos = putBytes(newData, pos, id, 0, id.length);
        pos = putBytes(newData, pos, salt, 0, salt.length);
        pos = putBytes(newData, pos, data, 0, data.length);
        return newData;
    }

    /**
     * Write a single byte out to the specified byte array position.
     * 
     * @param bytes
     *            the byte array
     * @param offset
     *            position in the array
     * @param b
     *            byte to write out
     * @return incremented offset
     */
    public static int putByte(byte[] bytes, int offset, byte b) {
        bytes[offset] = b;
        return offset + 1;
    }

    /**
     * Put bytes at the specified byte array position.
     * 
     * @param tgtBytes
     *            the byte array
     * @param tgtOffset
     *            position in the array
     * @param srcBytes
     *            array to write out
     * @param srcOffset
     *            source offset
     * @param srcLength
     *            source length
     * @return incremented offset
     */
    public static int putBytes(byte[] tgtBytes, int tgtOffset, byte[] srcBytes, int srcOffset, int srcLength) {
        System.arraycopy(srcBytes, srcOffset, tgtBytes, tgtOffset, srcLength);
        return tgtOffset + srcLength;
    }

    /**
     * Convert a long value to a byte array using big-endian.
     *
     * @param val
     *            value to convert
     * @return the byte array
     */
    public static byte[] toBytes(long val) {
        byte[] b = new byte[8];
        for (int i = 7; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    /** When we encode strings, we always specify UTF8 encoding */
    private static final String UTF8_ENCODING = "UTF-8";
    /** When we encode strings, we always specify UTF8 encoding */
    private static final Charset UTF8_CHARSET = Charset.forName(UTF8_ENCODING);

    /**
     * Converts a string to a UTF-8 byte array.
     * 
     * @param s
     *            string
     * @return the byte array
     */
    public static byte[] toBytes(String s) {
        return s.getBytes(UTF8_CHARSET);
    }

    /**
     * Put an int value out to the specified byte array position.
     * 
     * @param bytes
     *            the byte array
     * @param offset
     *            position in the array
     * @param val
     *            int to write out
     * @return incremented offset
     * @throws IllegalArgumentException
     *             if the byte array given doesn't have enough room at the
     *             offset specified.
     */
    public static int putInt(byte[] bytes, int offset, int val) {
        if (bytes.length - offset < SIZEOF_INT) {
            throw new IllegalArgumentException(
                    "Not enough room to put an int at" + " offset " + offset + " in a " + bytes.length + " byte array");
        }
        for (int i = offset + 3; i > offset; i--) {
            bytes[i] = (byte) val;
            val >>>= 8;
        }
        bytes[offset] = (byte) val;
        return offset + SIZEOF_INT;
    }

}
