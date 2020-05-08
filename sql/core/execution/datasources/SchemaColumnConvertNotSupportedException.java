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

package org.apache.spark.sql.execution.datasources;

import org.apache.spark.annotation.Unstable;

/**
 * 当parquet类型的读取器,寻找列失败的时候抛出的异常
 */
@Unstable
public class SchemaColumnConvertNotSupportedException extends RuntimeException {

  /**
   * 列名称,这个列不能正常转换
   */
  private String column;
  /**
   * 实际parquet文件中的物理类型
   */
  private String physicalType;
  /**
   * 逻辑列类型(parquet schema中的类型,parquet读取器使用这个来转换文件)
   * Logical column type in the parquet schema the parquet reader use to parse all files.
   */
  private String logicalType;

  public String getColumn() {
    return column;
  }

  public String getPhysicalType() {
    return physicalType;
  }

  public String getLogicalType() {
    return logicalType;
  }

  public SchemaColumnConvertNotSupportedException(
      String column,
      String physicalType,
      String logicalType) {
    super();
    this.column = column;
    this.physicalType = physicalType;
    this.logicalType = logicalType;
  }
}
