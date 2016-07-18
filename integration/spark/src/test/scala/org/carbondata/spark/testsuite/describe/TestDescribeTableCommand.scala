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

package org.carbondata.spark.testsuite.describe

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest

import org.carbondata.spark.exception.MalformedCarbonCommandException

import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for validating describe command
 *
 */
class TestDescribeTableCommand extends QueryTest with BeforeAndAfterAll {
  
  override def beforeAll {
    sql("drop table if exists carbontable")
    sql("drop table if exists hivetable")
  }

  test("Create carbon and hive table and compare describe") {
    sql("create table carbontable(id int, username struct<sur_name:string," +
        "actual_name:struct<first_name:string,last_name:string>>, country string, salary double)" +
        "STORED BY 'org.apache.carbondata.format'")
    sql("create table hivetable(id int, username struct<sur_name:string," +
        "actual_name:struct<first_name:string,last_name:string>>, country string, salary double)")
    checkAnswer(sql("describe carbontable"), sql("describe hivetable"))
    checkAnswer(sql("desc carbontable"), sql("desc hivetable"))
    sql("drop table if exists carbontable")
    sql("drop table if exists hivetable")
  }
  
  override def afterAll {
  }
}