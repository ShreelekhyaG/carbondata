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

package org.apache.carbondata.hive;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable;
import org.apache.carbondata.hive.util.HiveCarbonUtil;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.TableOptionConstant;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.Progressable;

public class MapredCarbonOutputFormat<T> extends CarbonTableOutputFormat
    implements HiveOutputFormat<Void, T>, OutputFormat<Void, T> {

  static {
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, "hive");
  }

  @Override
  public RecordWriter<Void, T> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s,
      Progressable progressable) throws IOException {
    throw new RuntimeException("Should never be used");
  }

  @Override
  public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
  }

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
      Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties,
      Progressable progress) throws IOException {
    ThreadLocalSessionInfo.setConfigurationToCurrentThread(jc);
    CarbonLoadModel carbonLoadModel = null;
    // Try to get loadmodel from JobConf.
    String encodedString = jc.get(LOAD_MODEL);
    System.out.println(jc.get(HiveConf.ConfVars.DEFAULTPARTITIONNAME.varname));
    System.out.println("=======");
    System.out.println(tableProperties.get(HiveConf.ConfVars.DEFAULTPARTITIONNAME.varname));
    if (encodedString != null) {
      carbonLoadModel =
          (CarbonLoadModel) ObjectSerializationUtil.convertStringToObject(encodedString);
    } else {
      // Try to get loadmodel from Container environment.
      encodedString = System.getenv("carbon");
      if (encodedString != null) {
        carbonLoadModel =
            (CarbonLoadModel) ObjectSerializationUtil.convertStringToObject(encodedString);
      } else {
        carbonLoadModel = HiveCarbonUtil.getCarbonLoadModel(tableProperties, jc);
      }
    }
    for (Map.Entry<Object, Object> entry : tableProperties.entrySet()) {
      carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().getTableInfo().getFactTable()
          .getTableProperties()
          .put(entry.getKey().toString().toLowerCase(), entry.getValue().toString().toLowerCase());
    }
    String tablePath = FileFactory.getCarbonFile(carbonLoadModel.getTablePath()).getAbsolutePath();
    TaskAttemptID taskAttemptID = TaskAttemptID.forName(jc.get("mapred.task.id"));
    // taskAttemptID will be null when the insert job is fired from presto. Presto send the JobConf
    // and since presto does not use the MR framework for execution, the mapred.task.id will be
    // null, so prepare a new ID.
    if (taskAttemptID == null) {
      SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
      String jobTrackerId = formatter.format(new Date());
      taskAttemptID = new TaskAttemptID(jobTrackerId, 0, TaskType.MAP, 0, 0);
      // update the app name here, as in this class by default it will written by Hive
      CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, "presto");
    } else {
      carbonLoadModel.setTaskNo("" + taskAttemptID.getTaskID().getId());
    }
    TaskAttemptContextImpl context = new TaskAttemptContextImpl(jc, taskAttemptID);
    final boolean isHivePartitionedTable =
        carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().isHivePartitionTable();
    PartitionInfo partitionInfo =
        carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().getPartitionInfo();
    final int partitionColumn =
        partitionInfo != null ? partitionInfo.getColumnSchemaList().size() : 0;
    List<String> measureColumns = new ArrayList<>();
    List<String> dateOrTimestampPartitionColumns = new ArrayList<>();
    boolean hasTimeStampAsPartition = false;
    if (carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().isHivePartitionTable()) {
      // in case of static partition, partition path will have hive stage dir also after partition
      // names. So, check and remove hive stage dir from the path. eg., tablepath/a=2/.hive_xxxx
      StringBuilder finalOutputPath = new StringBuilder(finalOutPath.toString());
      String partitionLoc = finalOutputPath.substring(
          carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().getTablePath().length());
      String[] partitionSplits =
          partitionLoc.substring(partitionLoc.indexOf(CarbonCommonConstants.FILE_SEPARATOR) + 1)
              .split(CarbonCommonConstants.FILE_SEPARATOR);
      if (partitionSplits.length != partitionColumn && null != partitionInfo) {
        finalOutputPath = new StringBuilder(carbonLoadModel.getTablePath());
        for (int i = 0; i < partitionColumn; i++) {
          finalOutputPath.append(CarbonCommonConstants.FILE_SEPARATOR).append(partitionSplits[i]);
        }
      }
      carbonLoadModel.getMetrics().addToPartitionPath(finalOutputPath.toString());
      context.getConfiguration().set("carbon.outputformat.writepath", finalOutputPath.toString());
      // for partition table, directly write data to store path
      carbonLoadModel.setDirectWriteToStorePath(true);

      // get measure column names list
      Objects.requireNonNull(partitionInfo);
      for (ColumnSchema columnSchema : partitionInfo.getColumnSchemaList()) {
        if (!columnSchema.isDimensionColumn()) {
          measureColumns.add(columnSchema.getColumnName());
        }
        if (columnSchema.getDataType().equals(DataTypes.TIMESTAMP) || columnSchema.getDataType()
            .equals(DataTypes.DATE)) {
          if (columnSchema.getDataType().equals(DataTypes.TIMESTAMP)) {
            hasTimeStampAsPartition = true;
          }
          dateOrTimestampPartitionColumns.add(columnSchema.getColumnName());
        }
      }
    }
    String serializationNullFormat =
        carbonLoadModel.getSerializationNullFormat().split(CarbonCommonConstants.COMMA, 2)[1];
    CarbonTableOutputFormat.setLoadModel(jc, carbonLoadModel);
    org.apache.hadoop.mapreduce.RecordWriter<NullWritable, ObjectArrayWritable> re =
        super.getRecordWriter(context);
    boolean hasTimeStampAsPartitionCol = hasTimeStampAsPartition;
    return new FileSinkOperator.RecordWriter() {
      @Override
      public void write(Writable writable) throws IOException {
        try {
          ObjectArrayWritable objectArrayWritable = new ObjectArrayWritable();
          if (isHivePartitionedTable) {
            Object[] actualRow = ((CarbonHiveRow) writable).getData();
            Object[] newData = Arrays.copyOf(actualRow, actualRow.length + partitionColumn);
            String partitionPath = finalOutPath.toString().substring(tablePath.length());
            if (hasTimeStampAsPartitionCol) {
              // since, timestamp data has minutes & seconds separated by ":", partition path will
              // be formatted by hive. eg., 2019-09-09 00:03:06 will be formatted like
              // 2019-09-09 00%3A03%3A06. Hence revert back to its original form.
              partitionPath = FileUtils.unescapePathName(partitionPath);
            }
            String[] partitionValues = partitionPath.split("/");
            for (int j = 0, i = actualRow.length; j < partitionValues.length; j++) {
              if (partitionValues[j].contains("=")) {
                String[] partitionSplit = partitionValues[j].split("=");
                String data = partitionSplit[1];
                // If data is empty, then hive will insert default partition name as data. Keep same
                // for dimension types and insert data as null for measure types.
                String defaultPartition = jc.get(HiveConf.ConfVars.DEFAULTPARTITIONNAME.varname);
                if (null != defaultPartition && defaultPartition.equalsIgnoreCase(data)) {
                  if (measureColumns.contains(partitionSplit[0])) {
                    data = null;
                  } else if (dateOrTimestampPartitionColumns.contains(partitionSplit[0])) {
                    // use serializationNullFormat for data and timestamp, as storing it as null,
                    // will throw bad record exception
                    data = serializationNullFormat;
                  }
                }
                newData[i++] = data;
              }
            }
            objectArrayWritable.set(newData);
          } else {
            objectArrayWritable.set(((CarbonHiveRow) writable).getData());
          }
          re.write(NullWritable.get(), objectArrayWritable);
        } catch (InterruptedException e) {
          throw new IOException(e.getCause());
        }
      }

      @Override
      public void close(boolean b) throws IOException {
        try {
          re.close(context);
          ThreadLocalSessionInfo.setConfigurationToCurrentThread(context.getConfiguration());
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    };
  }

  public static String unescapePathName(String path) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (c == '%' && i + 2 < path.length()) {
        int code = -1;
        try {
          code = Integer.parseInt(path.substring(i + 1, i + 3), 16);
        } catch (Exception e) {
          code = -1;
        }
        if (code >= 0) {
          sb.append((char) code);
          i += 2;
          continue;
        }
      }
      sb.append(c);
    }
    return sb.toString();
  }

}