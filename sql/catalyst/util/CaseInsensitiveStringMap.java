/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.util;

import org.apache.spark.annotation.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 类型敏感的的map类型,kv都是string类型的.
 * 这个用于传递参数给v2实现,用于保证一致性
 * 其中返回key的方法,例如@entrySet 和@keySet 返回的key会转换为小写字符.这个map不会允许空值
 */
@Experimental
public class CaseInsensitiveStringMap implements Map<String, String> {
  private final Logger logger = LoggerFactory.getLogger(CaseInsensitiveStringMap.class);

  // 不支持操作信息
  private String unsupportedOperationMsg = "CaseInsensitiveStringMap is read-only.";

  public static CaseInsensitiveStringMap empty() {
    return new CaseInsensitiveStringMap(new HashMap<>(0));
  }

  // 原始map
  private final Map<String, String> original;

  // 授权map
  private final Map<String, String> delegate;

  public CaseInsensitiveStringMap(Map<String, String> originalMap) {
    original = new HashMap<>(originalMap);
    delegate = new HashMap<>(originalMap.size());
    for (Map.Entry<String, String> entry : originalMap.entrySet()) {
      String key = toLowerCase(entry.getKey());
      if (delegate.containsKey(key)) {
        logger.warn("Converting duplicated key " + entry.getKey() +
                " into CaseInsensitiveStringMap.");
      }
      delegate.put(key, entry.getValue());
    }
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  private String toLowerCase(Object key) {
    return key.toString().toLowerCase(Locale.ROOT);
  }

  @Override
  public boolean containsKey(Object key) {
    return delegate.containsKey(toLowerCase(key));
  }

  @Override
  public boolean containsValue(Object value) {
    return delegate.containsValue(value);
  }

  // 获取指定key的value
  @Override
  public String get(Object key) {
    return delegate.get(toLowerCase(key));
  }

  // 存储kv(不支持)
  @Override
  public String put(String key, String value) {
    throw new UnsupportedOperationException(unsupportedOperationMsg);
  }

  // 移除kv(不支持)
  @Override
  public String remove(Object key) {
    throw new UnsupportedOperationException(unsupportedOperationMsg);
  }

  // 不支持一个集合的数据
  @Override
  public void putAll(Map<? extends String, ? extends String> m) {
    throw new UnsupportedOperationException(unsupportedOperationMsg);
  }

  // 情况map,不支持
  @Override
  public void clear() {
    throw new UnsupportedOperationException(unsupportedOperationMsg);
  }

  // 获取key集合
  @Override
  public Set<String> keySet() {
    return delegate.keySet();
  }

  // 获取value集合
  @Override
  public Collection<String> values() {
    return delegate.values();
  }

  // 获取entry集合
  @Override
  public Set<Map.Entry<String, String>> entrySet() {
    return delegate.entrySet();
  }

  /**
   * 返回是否指定的value被映射
   * Returns the boolean value to which the specified key is mapped,
   * or defaultValue if there is no mapping for the key. The key match is case-insensitive.
   */
  public boolean getBoolean(String key, boolean defaultValue) {
    String value = get(key);
    // We can't use `Boolean.parseBoolean` here, as it returns false for invalid strings.
    if (value == null) {
      return defaultValue;
    } else if (value.equalsIgnoreCase("true")) {
      return true;
    } else if (value.equalsIgnoreCase("false")) {
      return false;
    } else {
      throw new IllegalArgumentException(value + " is not a boolean string.");
    }
  }

  /**
   * Returns the integer value to which the specified key is mapped,
   * or defaultValue if there is no mapping for the key. The key match is case-insensitive.
   */
  public int getInt(String key, int defaultValue) {
    String value = get(key);
    return value == null ? defaultValue : Integer.parseInt(value);
  }

  /**
   * Returns the long value to which the specified key is mapped,
   * or defaultValue if there is no mapping for the key. The key match is case-insensitive.
   */
  public long getLong(String key, long defaultValue) {
    String value = get(key);
    return value == null ? defaultValue : Long.parseLong(value);
  }

  /**
   * Returns the double value to which the specified key is mapped,
   * or defaultValue if there is no mapping for the key. The key match is case-insensitive.
   */
  public double getDouble(String key, double defaultValue) {
    String value = get(key);
    return value == null ? defaultValue : Double.parseDouble(value);
  }

  /**
   * Returns the original case-sensitive map.
   */
  public Map<String, String> asCaseSensitiveMap() {
    return Collections.unmodifiableMap(original);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CaseInsensitiveStringMap that = (CaseInsensitiveStringMap) o;
    return delegate.equals(that.delegate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(delegate);
  }
}
