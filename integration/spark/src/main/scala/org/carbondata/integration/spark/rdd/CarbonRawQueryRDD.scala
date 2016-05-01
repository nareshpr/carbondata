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


package org.carbondata.integration.spark.rdd

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.carbon.datastore.block.TableBlockInfo
import org.carbondata.core.iterator.CarbonIterator
import org.carbondata.integration.spark.RawKeyVal
import org.carbondata.integration.spark.util.CarbonSparkInterFaceLogEvent
import org.carbondata.query.carbon.executor.QueryExecutorFactory
import org.carbondata.query.carbon.model.QueryModel
import org.carbondata.query.expression.Expression


/**
  * This RDD is used to perform query.
  */
class CarbonRawQueryRDD[K, V](
                               sc: SparkContext,
                               queryModel: QueryModel,
                               filterExpression: Expression,
                               keyClass: RawKeyVal[K, V],
                               @transient conf: Configuration,
                               cubeCreationTime: Long,
                               schemaLastUpdatedTime: Long,
                               baseStoreLocation: String)
  extends  CarbonQueryRDD[K, V](sc,
                                queryModel,
                                filterExpression,
                                null,
                                conf,
                                cubeCreationTime,
                                schemaLastUpdatedTime,
                                baseStoreLocation) with Logging {

  private val jobtrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  override def compute(thepartition: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass().getName());
    val iter = new Iterator[(K, V)] {
      var rowIterator: CarbonIterator[Array[Object]] = _
      var queryStartTime: Long = 0
      try { {
        val carbonSparkPartition = thepartition.asInstanceOf[CarbonSparkPartition]
        val carbonInputSplit = carbonSparkPartition.serializableHadoopSplit.value

        // fill table block info
        val tableBlockInfoList = new util.ArrayList[TableBlockInfo]();
        tableBlockInfoList.add(new TableBlockInfo(carbonInputSplit.getPath.toString,
          carbonInputSplit.getStart,
          carbonInputSplit.getSegmentId,
          carbonInputSplit.getLocations,
          carbonInputSplit.getLength
        )
        )
        queryModel.setTableBlockInfos(tableBlockInfoList)
        queryStartTime = System.currentTimeMillis

        val carbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null)
        logInfo("*************************" + carbonPropertiesFilePath)
        if (null == carbonPropertiesFilePath) {
          System.setProperty("carbon.properties.filepath", System.getProperty("user.dir")
                                                           + '/' + "conf" + '/' +
                                                           "carbon.properties"
          );
        }
        // execute query
        rowIterator = QueryExecutorFactory.getQueryExecutor(queryModel).execute(queryModel)
                      .asInstanceOf[CarbonIterator[Array[Object]]]
      }
        // TODO: CarbonQueryUtil.isQuickFilter quick filter from dictionary needs to support
      } catch {
        case e: Exception =>
          LOGGER.error(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, e)
          // updateCubeAndLevelCacheStatus(levelCacheKeys)
          if (null != e.getMessage) {
            sys.error("Exception occurred in query execution :: " + e.getMessage)
          } else {
            sys.error("Exception occurred in query execution.Please check logs.")
          }
      }

      var havePair = false
      var finished = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = !rowIterator.hasNext()
          havePair = !finished
        }
        !finished
      }

      override def next(): (K, V) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        val row = rowIterator.next()
        keyClass.getKey(row, null)
      }

      logInfo("*************************** Total Time Taken to execute the query in Carbon Side: " +
              (System.currentTimeMillis - queryStartTime)
      )
    }
    iter
  }
}