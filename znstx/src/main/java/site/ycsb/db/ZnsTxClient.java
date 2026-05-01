/**
 * Copyright (c) 2026 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package site.ycsb.db;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for the ZNS transactional engine via JNI.
 */
public class ZnsTxClient extends DB {
  private static final String PROP_ISOLATION = "znstx.isolation";
  private static final String PROP_DURABILITY = "znstx.durability";
  private static final String PROP_RECOVERY = "znstx.recovery";
  private static final String PROP_READ_BUFFER_BYTES = "znstx.readbuffer.bytes";

  private static final int DEFAULT_READ_BUFFER_BYTES = 64 * 1024;

  private static final int ISOLATION_SI = 0;
  private static final int ISOLATION_SS = 1;
  private static final int ISOLATION_CC = 2;

  private static final int DURABILITY_DEFAULT = 0;
  private static final int DURABILITY_REPLICA = 1;

  private static final Object INIT_LOCK = new Object();
  private static boolean nativeInitialized = false;
  private static int clientRefCount = 0;

  private int readBufferBytes = DEFAULT_READ_BUFFER_BYTES;

  static {
    System.loadLibrary("zns_tx_jni");
  }

  private static native int nativeInitTx(int isolationLevel, int durabilityLevel, boolean enableRecovery);
  private static native void nativeCleanTx();
  private static native int nativeBeginTx();
  private static native int nativeEndTx();
  private static native int nativeAbortTx();
  private static native int nativeCreateObj(String objKey);
  private static native int nativeWriteObj(String objKey, long offset, byte[] data);
  private static native byte[] nativeReadObj(String objKey, long offset, int length);

  private static native int nativePutObj(String objKey, byte[] data);
  private static native byte[] nativeGetObj(String objKey, int maxLength);

  @Override
  public void start() throws DBException {
    int rc = nativeBeginTx();
    if (rc != 0) {
      throw new DBException("Failed to begin transaction, rc=" + rc);
    }
  }

  @Override
  public void commit() throws DBException {
    int rc = nativeEndTx();
    if (rc != 0) {
      throw new DBException("Failed to commit transaction, rc=" + rc);
    }
  }

  @Override
  public void abort() throws DBException {
    int rc = nativeAbortTx();
    if (rc != 0) {
      throw new DBException("Failed to abort transaction, rc=" + rc);
    }
  }

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    int isolationLevel = parseIsolation(props.getProperty(PROP_ISOLATION, "si"));
    int durabilityLevel = parseDurability(props.getProperty(PROP_DURABILITY, "default"));
    boolean enableRecovery = Boolean.parseBoolean(props.getProperty(PROP_RECOVERY, "true"));
    readBufferBytes = parsePositiveInt(props.getProperty(PROP_READ_BUFFER_BYTES), DEFAULT_READ_BUFFER_BYTES);

    synchronized (INIT_LOCK) {
      if (!nativeInitialized) {
        int rc = nativeInitTx(isolationLevel, durabilityLevel, enableRecovery);
        if (rc != 0) {
          throw new DBException("nativeInitTx failed with rc=" + rc);
        }
        nativeInitialized = true;
      }
      clientRefCount++;
    }
  }

  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_LOCK) {
      if (clientRefCount > 0) {
        clientRefCount--;
      }
      if (nativeInitialized && clientRefCount == 0) {
        nativeCleanTx();
        nativeInitialized = false;
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String objKey = objectKey(table, key);
    try {
      byte[] raw = nativeGetObj(objKey, readBufferBytes);
      nativeAbortTx(); // End the read-only transaction after the read
      if (raw == null || raw.length == 0) {
        return Status.NOT_FOUND;
      }

      final ByteBuffer buf = ByteBuffer.allocate(4);
      int offset = 0;
      boolean foundAny = false;

      while (offset + 4 <= raw.length) {
        buf.put(raw, offset, 4);
        buf.flip();
        final int keyLen = buf.getInt();
        buf.clear();
        offset += 4;

        if (keyLen <= 0 || offset + keyLen + 4 > raw.length) {
          break;
        }

        final String fieldName = new String(raw, offset, keyLen, StandardCharsets.UTF_8);
        offset += keyLen;

        buf.put(raw, offset, 4);
        buf.flip();
        final int valueLen = buf.getInt();
        buf.clear();
        offset += 4;

        if (valueLen < 0 || offset + valueLen > raw.length) {
          break;
        }

        foundAny = true;
        if (fields == null || fields.contains(fieldName)) {
          result.put(fieldName, new ByteArrayByteIterator(raw, offset, valueLen));
        }

        offset += valueLen;
      }

      if (!foundAny) {
        return Status.NOT_FOUND;
      }

      return Status.OK;
    } catch (RuntimeException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String objKey = objectKey(table, key);
    try {
      byte[] existingData = nativeGetObj(objKey, readBufferBytes);
      Map<String, ByteIterator> mergedValues = new HashMap<>();

      if (existingData != null && existingData.length > 0) {
        final ByteBuffer buf = ByteBuffer.allocate(4);
        int offset = 0;

        while (offset + 4 <= existingData.length) {
          buf.put(existingData, offset, 4);
          buf.flip();
          final int keyLen = buf.getInt();
          buf.clear();
          offset += 4;

          if (keyLen <= 0 || offset + keyLen + 4 > existingData.length) {
            break;
          }

          final String fieldName = new String(existingData, offset, keyLen, StandardCharsets.UTF_8);
          offset += keyLen;

          buf.put(existingData, offset, 4);
          buf.flip();
          final int valueLen = buf.getInt();
          buf.clear();
          offset += 4;

          if (valueLen < 0 || offset + valueLen > existingData.length) {
            break;
          }

          mergedValues.put(fieldName, new ByteArrayByteIterator(existingData, offset, valueLen));
          offset += valueLen;
        }
      }

      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        mergedValues.put(entry.getKey(), entry.getValue());
      }

      byte[] payload = serializeFields(mergedValues);
      if (nativePutObj(objKey, payload) == 0) {
        return Status.OK;
      } else {
        nativeAbortTx(); // Abort the transaction if write failed
        return Status.ERROR;
      }
    } catch (RuntimeException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    String objKey = objectKey(table, key);
    byte[] payload = serializeFields(values);
    try {
      if (nativePutObj(objKey, payload) == 0) {
        return Status.OK;
      } else {
        nativeAbortTx(); // Abort the transaction if write failed
        return Status.ERROR;
      }
    } catch (RuntimeException e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  private static String objectKey(String table, String key) {
    return table + ":" + key;
  }

  private static int parseIsolation(String value) throws DBException {
    if (value == null) {
      return ISOLATION_SI;
    }
    String normalized = value.trim().toLowerCase();
    if ("si".equals(normalized) || "snapshot".equals(normalized) || "snapshotisolation".equals(normalized)) {
      return ISOLATION_SI;
    }
    if ("ss".equals(normalized) || "strict".equals(normalized) || "strictserializability".equals(normalized)) {
      return ISOLATION_SS;
    }
    if ("cc".equals(normalized) || "causal".equals(normalized) || "causalconsistency".equals(normalized)) {
      return ISOLATION_CC;
    }
    throw new DBException("Unknown isolation level: " + value);
  }

  private static int parseDurability(String value) throws DBException {
    if (value == null) {
      return DURABILITY_DEFAULT;
    }
    String normalized = value.trim().toLowerCase();
    if ("default".equals(normalized) || "defaultstriping".equals(normalized)) {
      return DURABILITY_DEFAULT;
    }
    if ("replica".equals(normalized) || "selectivereplica".equals(normalized)) {
      return DURABILITY_REPLICA;
    }
    throw new DBException("Unknown durability level: " + value);
  }

  private static int parsePositiveInt(String value, int defaultValue) {
    if (value == null || value.trim().isEmpty()) {
      return defaultValue;
    }
    try {
      int parsed = Integer.parseInt(value.trim());
      return parsed > 0 ? parsed : defaultValue;
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private static byte[] serializeFields(Map<String, ByteIterator> values) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      byte[] fieldName = entry.getKey().getBytes(StandardCharsets.UTF_8);
      byte[] fieldValue = entry.getValue().toArray();

      out.write((fieldName.length >>> 24) & 0xFF);
      out.write((fieldName.length >>> 16) & 0xFF);
      out.write((fieldName.length >>> 8) & 0xFF);
      out.write(fieldName.length & 0xFF);
      out.write(fieldName, 0, fieldName.length);

      out.write((fieldValue.length >>> 24) & 0xFF);
      out.write((fieldValue.length >>> 16) & 0xFF);
      out.write((fieldValue.length >>> 8) & 0xFF);
      out.write(fieldValue.length & 0xFF);
      out.write(fieldValue, 0, fieldValue.length);
    }
    return out.toByteArray();
  }

}
