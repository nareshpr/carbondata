/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.query.carbon.executor.impl;

import java.util.concurrent.Callable;

import org.carbondata.common.logging.impl.StandardLogService;
import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.scanner.BlockProcessor;
import org.carbondata.query.carbon.scanner.impl.AggregateQueryBlockProcessor;
import org.carbondata.query.carbon.scanner.impl.DetailQueryBlockProcessor;

/**
 * Class which will execute the query
 */
public class QueryRunner implements Callable<Void> {

  /**
   * block processor
   */
  private BlockProcessor dataBlockProcessor;

  /**
   * file reader which will be used to execute the query
   */
  private FileHolder fileReader;

  /**
   * block execution info which is required to run the query
   */
  private BlockExecutionInfo blockExecutionInfo;

  public QueryRunner(BlockExecutionInfo executionInfos) {
    this.blockExecutionInfo = executionInfos;
    // if detail query detail query processor will be used to process the
    // block
    if (executionInfos.isDetailQuery()) {
      dataBlockProcessor = new DetailQueryBlockProcessor(executionInfos, fileReader);
    } else {
      dataBlockProcessor = new AggregateQueryBlockProcessor(executionInfos, fileReader);
    }
  }

  @Override public Void call() throws Exception {
    StandardLogService
        .setThreadName(blockExecutionInfo.getPartitionId(), blockExecutionInfo.getQueryId());
    try {
      this.dataBlockProcessor.processBlock();
    } finally {
      this.fileReader.finish();
    }
    return null;
  }

}
