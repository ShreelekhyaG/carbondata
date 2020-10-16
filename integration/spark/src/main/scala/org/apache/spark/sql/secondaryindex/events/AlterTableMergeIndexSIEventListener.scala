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

package org.apache.spark.sql.secondaryindex.events

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.CarbonMergeFilesRDD
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.execution.command.Auditable
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.secondaryindex.command.IndexModel
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.index.Segment
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.index.IndexType
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events._
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionType}

class AlterTableMergeIndexSIEventListener
  extends OperationEventListener with Logging with Auditable {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val exceptionEvent = event.asInstanceOf[AlterTableMergeIndexEvent]
    val alterTableModel = exceptionEvent.alterTableModel
    val carbonMainTable = exceptionEvent.carbonTable
    val compactionType = alterTableModel.compactionType
    val sparkSession = exceptionEvent.sparkSession
    if (compactionType.equalsIgnoreCase(CompactionType.SEGMENT_INDEX.toString)) {
      LOGGER.info( s"Compaction request received for table " +
                   s"${ carbonMainTable.getDatabaseName}.${carbonMainTable.getTableName}")
      val lock = CarbonLockFactory.getCarbonLockObj(
        carbonMainTable.getAbsoluteTableIdentifier,
        LockUsage.COMPACTION_LOCK)

      try {
        if (lock.lockWithRetries()) {
          LOGGER.info("Acquired the compaction lock for table" +
                      s" ${carbonMainTable.getDatabaseName}.${carbonMainTable.getTableName}")
          val indexProviderMap = carbonMainTable.getIndexesMap
          if (!indexProviderMap.isEmpty) {
            if (null != indexProviderMap.get(IndexType.SI.getIndexProviderName)) {
              val secondaryIndexIterator = indexProviderMap
                .get(IndexType.SI.getIndexProviderName).entrySet().iterator()
              while (secondaryIndexIterator.hasNext) {
                val index = secondaryIndexIterator.next()
                val secondaryIndex = IndexModel(Some(carbonMainTable.getDatabaseName),
                  carbonMainTable.getTableName,
                  index.getValue.get(CarbonCommonConstants.INDEX_COLUMNS).split(",").toList,
                  index.getKey)
                val metastore = CarbonEnv.getInstance(sparkSession)
                  .carbonMetaStore
                val indexCarbonTable = metastore
                  .lookupRelation(Some(carbonMainTable.getDatabaseName),
                    secondaryIndex.indexName)(sparkSession).asInstanceOf[CarbonRelation]
                  .carbonTable
                setAuditTable(indexCarbonTable)
                setAuditInfo(Map("compactionType" -> compactionType))
                val validSegments: mutable.Buffer[Segment] = CarbonDataMergerUtil
                  .getValidSegmentList(
                    carbonMainTable)
                  .asScala
                val validSegmentIds: mutable.Buffer[String] = mutable.Buffer[String]()
                validSegments.foreach { segment =>
                  val segmentFile = segment.getSegmentFileName
                  val sfs = new SegmentFileStore(indexCarbonTable.getTablePath, segmentFile)
                  if (sfs.getSegmentFile != null) {
                    val indexFiles = sfs.getIndexCarbonFiles
                    val segmentPath = CarbonTablePath
                      .getSegmentPath(indexCarbonTable.getTablePath, segment.getSegmentNo)
                    if (indexFiles.size() == 0) {
                      LOGGER.warn("No index files present in path: " + segmentPath + " to merge")
                    } else {
                      // call merge only if segments having index files
                      validSegmentIds += segment.getSegmentNo
                    }
                  } else {
                    validSegmentIds += segment.getSegmentNo
                  }
                }
                // Just launch job to merge index for all index tables
                CarbonMergeFilesRDD.mergeIndexFiles(
                  sparkSession,
                  validSegmentIds,
                  SegmentStatusManager.mapSegmentToStartTime(carbonMainTable),
                  indexCarbonTable.getTablePath,
                  indexCarbonTable,
                  mergeIndexProperty = true,
                  readFileFooterFromCarbonDataFile = true)
              }
            }
          }
          LOGGER.info(s"Compaction request completed for table " +
                      s"${carbonMainTable.getDatabaseName}.${carbonMainTable.getTableName}")
        } else {
          LOGGER.error(s"Not able to acquire the compaction lock for table" +
                       s" ${carbonMainTable.getDatabaseName}.${carbonMainTable.getTableName}")
          CarbonException.analysisException(
            "Table is already locked for compaction. Please try after some time.")
        }
      } finally {
        lock.unlock()
      }
      operationContext.setProperty("compactionException", "false")
    }
  }

  override protected def opName: String = "MergeIndex SI EventListener"

}
