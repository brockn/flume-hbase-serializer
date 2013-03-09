/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.cloudera.flume.sink.hbase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;


public class RegexHbaseEventRowKeySerializer implements HbaseEventSerializer {
  private static final Logger logger = LoggerFactory.getLogger(RegexHbaseEventRowKeySerializer.class);

  // Config vars
  /** Regular expression used to parse groups from event data. */
  public static final String REGEX_CONFIG = "regex";

  /** Whether to ignore case when performing regex matches. */
  public static final String IGNORE_CASE_CONFIG = "regexIgnoreCase";
  public static final boolean INGORE_CASE_DEFAULT = false;

  /** Comma separated list of column names to place matching groups in. */
  public static final String COL_NAME_CONFIG = "colNames";
  /** Index row key in event body */
  public static final String ROW_KEY_INDEX_CONFIG = "rowKeyIndex";
  /** Placeholder in colNames for row key */
  public static final String ROW_KEY_NAME = "ROW_KEY";
  
  protected byte[] columnFamily;
  private byte[] payload;
  private List<byte[]> colNames = Lists.newArrayList();
  private boolean regexIgnoreCase;
  private Pattern inputPattern;
  private int rowKeyIndex;
  private boolean isTraceLogging;

  @Override
  public void configure(Context context) {
    String regex = context.getString(REGEX_CONFIG, "").trim();
    logger.info("regex " + regex);
    if(regex.isEmpty()) {
      throw new IllegalArgumentException(REGEX_CONFIG + " cannot be empty");
    }
    regexIgnoreCase = context.getBoolean(IGNORE_CASE_CONFIG,
        INGORE_CASE_DEFAULT);
    logger.info("regexIgnoreCase " + regexIgnoreCase);
    inputPattern = Pattern.compile(regex, Pattern.DOTALL
        + (regexIgnoreCase ? Pattern.CASE_INSENSITIVE : 0));
    rowKeyIndex = context.getInteger(ROW_KEY_INDEX_CONFIG, -1);
    logger.info("rowKeyIndex " + rowKeyIndex);
    if(rowKeyIndex < 0) {
      throw new IllegalArgumentException(ROW_KEY_INDEX_CONFIG + " must be >= 1");
    }    
    String colNameStr = context.getString(COL_NAME_CONFIG, "").trim();
    if(colNameStr.isEmpty()) {
      throw new IllegalArgumentException(COL_NAME_CONFIG + " cannot be empty");
    }
    String[] columnNames = colNameStr.split(",");
    logger.info("columnNames " + Arrays.toString(columnNames));
    if(rowKeyIndex >= columnNames.length) {
      throw new IllegalArgumentException(ROW_KEY_INDEX_CONFIG + " must be " +
          "less than num columns " + columnNames.length);
    }
    if(!ROW_KEY_NAME.equalsIgnoreCase(columnNames[rowKeyIndex])) {
      throw new IllegalArgumentException("Column at " + rowKeyIndex + " must be "
          + ROW_KEY_NAME + " and is " + columnNames[rowKeyIndex]);
    }
    for (String s : columnNames) {
      colNames.add(s.getBytes(Charsets.UTF_8));
    }
    isTraceLogging = logger.isTraceEnabled();
  }

  @Override
  public void configure(ComponentConfiguration conf) {
    throw new UnsupportedOperationException("ComponentConfiguration is not supported");
  }

  @Override
  public void initialize(Event event, byte[] columnFamily) {
    this.payload = event.getBody();
    this.columnFamily = columnFamily;
  }
 
  @Override
  public List<Row> getActions() throws FlumeException {
    List<Row> actions = Lists.newArrayList();
    Matcher matcher = inputPattern.matcher(new String(payload, Charsets.UTF_8));
    if (!matcher.matches()) {
      if(isTraceLogging) {
        logger.trace("Payload does not match regex");
      }
      return actions;
    }
    if (matcher.groupCount() != colNames.size()) {
      if(isTraceLogging) {
        logger.trace("matcher.groupCount() != colNames.size(), " 
            + matcher.groupCount() + " != " + colNames.size());
      }
      return actions;
    }
    Put put = new Put(matcher.group(rowKeyIndex + 1).getBytes(Charsets.UTF_8));
    for (int i = 0; i < colNames.size(); i++) {
      if(i != rowKeyIndex) {
        put.add(columnFamily, colNames.get(i), matcher.group(i + 1).getBytes(Charsets.UTF_8));          
      }
    }
    actions.add(put);
    return actions;
  }

  @Override
  public List<Increment> getIncrements() {
    return Lists.newArrayListWithCapacity(0);
  }

  @Override
  public void close() {
  }
}