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
package org.carbondata.hadoop.test.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.CarbonDataLoadSchema;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.metadata.CarbonMetadata;
import org.carbondata.core.carbon.metadata.converter.SchemaConverter;
import org.carbondata.core.carbon.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.schema.SchemaEvolution;
import org.carbondata.core.carbon.metadata.schema.SchemaEvolutionEntry;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.TableInfo;
import org.carbondata.core.carbon.metadata.schema.table.TableSchema;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.carbondata.core.carbon.path.CarbonStorePath;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.load.BlockDetails;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.writer.CarbonDictionaryWriter;
import org.carbondata.core.writer.CarbonDictionaryWriterImpl;
import org.carbondata.core.writer.ThriftWriter;
import org.carbondata.processing.api.dataloader.DataLoadModel;
import org.carbondata.processing.api.dataloader.SchemaInfo;
import org.carbondata.processing.csvload.DataGraphExecuter;
import org.carbondata.processing.dataprocessor.DataProcessTaskStatus;
import org.carbondata.processing.dataprocessor.IDataProcessStatus;
import org.carbondata.processing.graphgenerator.GraphGenerator;
import org.carbondata.processing.graphgenerator.GraphGeneratorException;
import org.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * This class will create store file based on provided schema
 *
 * @author Administrator
 */
public class StoreCreator {

  private static AbsoluteTableIdentifier absoluteTableIdentifier;

  static {
    try {
      String storePath = new File("src/test/resources/store").getCanonicalPath();
      String dbName = "testdb";
      String tableName = "testtable";
      absoluteTableIdentifier =
          new AbsoluteTableIdentifier(storePath, new CarbonTableIdentifier(dbName, tableName));
    } catch (IOException ex) {

    }
  }

  /**
   * Create store without any restructure
   */
  public static void createCarbonStore() {

    try {

      String factFilePath = new File("src/test/resources/data.csv").getCanonicalPath();
      File storeDir = new File(absoluteTableIdentifier.getStorePath());
      CarbonUtil.deleteFoldersAndFiles(storeDir);

      String kettleHomePath = "../processing/carbonplugins";
      int currentRestructureNumber = 0;
      CarbonTable table = createTable();
      writeDictionary(factFilePath, table);
      CarbonDataLoadSchema schema = new CarbonDataLoadSchema(table);
      LoadModel loadModel = new LoadModel();
      String partitionId = "0";
      loadModel.setSchema(schema);
      loadModel.setSchemaName(absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName());
      loadModel.setCubeName(absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
      loadModel.setTableName(absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
      loadModel.setFactFilePath(factFilePath);
      loadModel.setLoadMetadataDetails(new ArrayList<LoadMetadataDetails>());

      executeGraph(loadModel, absoluteTableIdentifier.getStorePath(), kettleHomePath,
          currentRestructureNumber);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  private static CarbonTable createTable() throws IOException {
    TableInfo tableInfo = new TableInfo();
    tableInfo.setDatabaseName(absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName());
    TableSchema tableSchema = new TableSchema();
    tableSchema.setTableName(absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
    List<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
    ArrayList<Encoding> encodings = new ArrayList<>();
    encodings.add(Encoding.DIRECT_DICTIONARY);
    ColumnSchema id = new ColumnSchema();
    id.setColumnName("ID");
    id.setColumnar(true);
    id.setDataType(DataType.INT);
    id.setEncodingList(encodings);
    id.setColumnUniqueId(UUID.randomUUID().toString());
    id.setDimensionColumn(true);
    columnSchemas.add(id);

    ColumnSchema date = new ColumnSchema();
    date.setColumnName("date");
    date.setColumnar(true);
    date.setDataType(DataType.STRING);
    date.setEncodingList(encodings);
    date.setColumnUniqueId(UUID.randomUUID().toString());
    date.setDimensionColumn(true);
    columnSchemas.add(date);

    ColumnSchema country = new ColumnSchema();
    country.setColumnName("country");
    country.setColumnar(true);
    country.setDataType(DataType.STRING);
    country.setEncodingList(encodings);
    country.setColumnUniqueId(UUID.randomUUID().toString());
    country.setDimensionColumn(true);
    columnSchemas.add(country);

    ColumnSchema name = new ColumnSchema();
    name.setColumnName("name");
    name.setColumnar(true);
    name.setDataType(DataType.STRING);
    name.setEncodingList(encodings);
    name.setColumnUniqueId(UUID.randomUUID().toString());
    name.setDimensionColumn(true);
    columnSchemas.add(name);

    ColumnSchema phonetype = new ColumnSchema();
    phonetype.setColumnName("phonetype");
    phonetype.setColumnar(true);
    phonetype.setDataType(DataType.STRING);
    phonetype.setEncodingList(encodings);
    phonetype.setColumnUniqueId(UUID.randomUUID().toString());
    phonetype.setDimensionColumn(true);
    columnSchemas.add(phonetype);

    ColumnSchema serialname = new ColumnSchema();
    serialname.setColumnName("serialname");
    serialname.setColumnar(true);
    serialname.setDataType(DataType.STRING);
    serialname.setEncodingList(encodings);
    serialname.setColumnUniqueId(UUID.randomUUID().toString());
    serialname.setDimensionColumn(true);
    columnSchemas.add(serialname);

    ColumnSchema salary = new ColumnSchema();
    salary.setColumnName("salary");
    salary.setColumnar(true);
    salary.setDataType(DataType.INT);
    salary.setEncodingList(new ArrayList<Encoding>());
    salary.setColumnUniqueId(UUID.randomUUID().toString());
    salary.setDimensionColumn(false);
    columnSchemas.add(salary);

    tableSchema.setListOfColumns(columnSchemas);
    SchemaEvolution schemaEvol = new SchemaEvolution();
    schemaEvol.setSchemaEvolutionEntryList(new ArrayList<SchemaEvolutionEntry>());
    tableSchema.setSchemaEvalution(schemaEvol);
    tableSchema.setTableId(1);
    tableInfo.setTableUniqueName(
        absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName() + "_"
            + absoluteTableIdentifier.getCarbonTableIdentifier().getTableName());
    tableInfo.setLastUpdatedTime(System.currentTimeMillis());
    tableInfo.setFactTable(tableSchema);
    tableInfo.setAggregateTableList(new ArrayList<TableSchema>());

    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
            absoluteTableIdentifier.getCarbonTableIdentifier());
    String schemaFilePath = carbonTablePath.getSchemaFilePath();
    String schemaMetadataPath = CarbonTablePath.getFolderContainingFile(schemaFilePath);
    tableInfo.setMetaDataFilepath(schemaMetadataPath);
    CarbonMetadata.getInstance().loadTableMetadata(tableInfo);

    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    org.carbondata.format.TableInfo thriftTableInfo = schemaConverter
        .fromWrapperToExternalTableInfo(tableInfo, tableInfo.getDatabaseName(),
            tableInfo.getFactTable().getTableName());
    org.carbondata.format.SchemaEvolutionEntry schemaEvolutionEntry =
        new org.carbondata.format.SchemaEvolutionEntry(tableInfo.getLastUpdatedTime());
    thriftTableInfo.getFact_table().getSchema_evolution().getSchema_evolution_history()
        .add(schemaEvolutionEntry);

    FileFactory.FileType fileType = FileFactory.getFileType(schemaMetadataPath);
    if (!FileFactory.isFileExist(schemaMetadataPath, fileType)) {
      FileFactory.mkdirs(schemaMetadataPath, fileType);
    }

    ThriftWriter thriftWriter = new ThriftWriter(schemaFilePath, false);
    thriftWriter.open();
    thriftWriter.write(thriftTableInfo);
    thriftWriter.close();
    return CarbonMetadata.getInstance().getCarbonTable(tableInfo.getTableUniqueName());
  }

  private static void writeDictionary(String factFilePath, CarbonTable table) throws Exception {
    BufferedReader reader = new BufferedReader(new FileReader(factFilePath));
    String header = reader.readLine();
    String[] split = header.split(",");
    List<CarbonColumn> allCols = new ArrayList<CarbonColumn>();
    List<CarbonDimension> dims = table.getDimensionByTableName(table.getFactTableName());
    allCols.addAll(dims);
    List<CarbonMeasure> msrs = table.getMeasureByTableName(table.getFactTableName());
    allCols.addAll(msrs);
    Set<String>[] set = new HashSet[dims.size()];
    for (int i = 0; i < set.length; i++) {
      set[i] = new HashSet<String>();
    }
    String line = reader.readLine();
    while (line != null) {
      String[] data = line.split(",");
      for (int i = 0; i < set.length; i++) {
        set[i].add(data[i]);
      }
      line = reader.readLine();
    }

    for (int i = 0; i < set.length; i++) {
      CarbonDictionaryWriter writer =
          new CarbonDictionaryWriterImpl(absoluteTableIdentifier.getStorePath(),
              absoluteTableIdentifier.getCarbonTableIdentifier(), dims.get(i).getColumnId());
      for (String value : set[i]) {
        writer.write(value);
      }
      writer.close();
    }
    reader.close();
  }

  /**
   * Execute graph which will further load data
   *
   * @param loadModel
   * @param storeLocation
   * @param kettleHomePath
   * @param currentRestructNumber
   * @throws Exception
   */
  public static void executeGraph(LoadModel loadModel, String storeLocation, String kettleHomePath,
      int currentRestructNumber) throws Exception {
    System.setProperty("KETTLE_HOME", kettleHomePath);
    new File(storeLocation).mkdirs();
    String outPutLoc = storeLocation + "/etl";
    String schemaName = loadModel.getSchemaName();
    String cubeName = loadModel.getCubeName();
    String tempLocationKey = schemaName + '_' + cubeName;
    CarbonProperties.getInstance().addProperty(tempLocationKey, storeLocation);
    CarbonProperties.getInstance().addProperty("store_output_location", outPutLoc);
    CarbonProperties.getInstance().addProperty("send.signal.load", "false");

    String tableName = loadModel.getTableName();
    String fileNamePrefix = "";

    String graphPath =
        outPutLoc + File.separator + loadModel.getSchemaName() + File.separator + tableName
            + File.separator + 0 + File.separator + 1 + File.separator + tableName + ".ktr";
    File path = new File(graphPath);
    if (path.exists()) {
      path.delete();
    }

    DataProcessTaskStatus schmaModel = new DataProcessTaskStatus(schemaName, cubeName, tableName);
    schmaModel.setCsvFilePath(loadModel.getFactFilePath());
    SchemaInfo info = new SchemaInfo();
    BlockDetails blockDetails = new BlockDetails();
    blockDetails.setFilePath(loadModel.getFactFilePath());
    blockDetails.setBlockOffset(0);
    blockDetails.setBlockLength(new File(loadModel.getFactFilePath()).length());
    GraphGenerator.blockInfo.put("qwqwq", new BlockDetails[]{blockDetails});
    schmaModel.setBlocksID("qwqwq");

    info.setSchemaName(schemaName);
    info.setCubeName(cubeName);

    generateGraph(schmaModel, info, loadModel.getTableName(), "0", loadModel.getSchema(), null,
        currentRestructNumber, loadModel.getLoadMetadataDetails());

    DataGraphExecuter graphExecuter = new DataGraphExecuter(schmaModel);
    graphExecuter
        .executeGraph(graphPath, new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN),
            info, "0", loadModel.getSchema());
  }

  /**
   * generate graph
   *
   * @param schmaModel
   * @param info
   * @param tableName
   * @param partitionID
   * @param schema
   * @param factStoreLocation
   * @param currentRestructNumber
   * @param loadMetadataDetails
   * @throws GraphGeneratorException
   */
  private static void generateGraph(IDataProcessStatus schmaModel, SchemaInfo info,
      String tableName, String partitionID, CarbonDataLoadSchema schema, String factStoreLocation,
      int currentRestructNumber, List<LoadMetadataDetails> loadMetadataDetails)
      throws GraphGeneratorException {
    DataLoadModel model = new DataLoadModel();
    model.setCsvLoad(null != schmaModel.getCsvFilePath() || null != schmaModel.getFilesToProcess());
    model.setSchemaInfo(info);
    model.setTableName(schmaModel.getTableName());
    model.setTaskNo("1");
    model.setBlocksID(schmaModel.getBlocksID());
    if (null != loadMetadataDetails && !loadMetadataDetails.isEmpty()) {
      model.setLoadNames(
          CarbonDataProcessorUtil.getLoadNameFromLoadMetaDataDetails(loadMetadataDetails));
      model.setModificationOrDeletionTime(CarbonDataProcessorUtil
          .getModificationOrDeletionTimesFromLoadMetadataDetails(loadMetadataDetails));
    }
    boolean hdfsReadMode =
        schmaModel.getCsvFilePath() != null && schmaModel.getCsvFilePath().startsWith("hdfs:");
    int allocate = null != schmaModel.getCsvFilePath() ? 1 : schmaModel.getFilesToProcess().size();
    GraphGenerator generator =
        new GraphGenerator(model, hdfsReadMode, partitionID, factStoreLocation,
            currentRestructNumber, allocate, schema, "0");
    generator.generateGraph();
  }

   /**
   * This is local model object used inside this class to store information related to data loading
   *
   * @author Administrator
   */
  static class LoadModel {

    private CarbonDataLoadSchema schema;
    private String tableName;
    private String cubeName;
    private String schemaName;
    private List<LoadMetadataDetails> loadMetaDetail;
    private String factFilePath;

    public void setSchema(CarbonDataLoadSchema schema) {
      this.schema = schema;
    }

    public List<LoadMetadataDetails> getLoadMetadataDetails() {
      return loadMetaDetail;
    }

    public CarbonDataLoadSchema getSchema() {
      return schema;
    }

    public String getFactFilePath() {
      return factFilePath;
    }

    public String getTableName() {
      return tableName;
    }

    public String getCubeName() {
      return cubeName;
    }

    public String getSchemaName() {
      return schemaName;
    }

    public void setLoadMetadataDetails(List<LoadMetadataDetails> loadMetaDetail) {
      this.loadMetaDetail = loadMetaDetail;
    }

    public void setFactFilePath(String factFilePath) {
      this.factFilePath = factFilePath;
    }

    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    public void setCubeName(String cubeName) {
      this.cubeName = cubeName;
    }

    public void setSchemaName(String schemaName) {
      this.schemaName = schemaName;
    }

  }

  public static void main(String[] args) {
    StoreCreator.createCarbonStore();
  }

}