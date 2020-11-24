package com.github.airblader.imap.catalog;

import static com.github.airblader.ConfigOptionsLibrary.FOLDER;
import static com.github.airblader.imap.ImapUtils.getImapProperties;

import com.github.airblader.imap.table.ImapTableSourceFactory;
import jakarta.mail.Folder;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.Store;
import java.util.*;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.BigIntType;

@RequiredArgsConstructor
public class ImapCatalog implements Catalog {
  private final String name;
  private final ImapCatalogOptions catalogOptions;

  private Store store;

  @Override
  public void open() throws CatalogException {
    if (store != null && store.isConnected()) {
      return;
    }

    var session = Session.getInstance(getImapProperties(catalogOptions), null);
    try {
      store = session.getStore();
      store.connect(catalogOptions.getEffectiveUser(), catalogOptions.getEffectivePassword());
    } catch (MessagingException e) {
      throw new CatalogException("Failed to connect to the IMAP server: " + e.toString(), e);
    }
  }

  @Override
  public void close() throws CatalogException {
    if (store != null) {
      try {
        store.close();
      } catch (MessagingException e) {
        throw new CatalogException(e);
      }
    }
  }

  @Override
  public Optional<Factory> getFactory() {
    return Optional.of(new ImapTableSourceFactory());
  }

  @Override
  public String getDefaultDatabase() throws CatalogException {
    return "default";
  }

  @Override
  public List<String> listDatabases() throws CatalogException {
    var databases = new ArrayList<String>();
    databases.add(getDefaultDatabase());
    return databases;
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    if (!databaseExists(databaseName)) {
      throw new DatabaseNotExistException(name, databaseName);
    }

    return new CatalogDatabaseImpl(new HashMap<>(), null);
  }

  @Override
  public boolean databaseExists(String databaseName) throws CatalogException {
    return getDefaultDatabase().equals(databaseName);
  }

  @Override
  public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
      throws DatabaseAlreadyExistException, CatalogException {
    throw new CatalogException("Unsupported operation");
  }

  @Override
  public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
      throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
    if (databaseExists(name) && !ignoreIfNotExists) {
      throw new DatabaseNotExistException(name, name);
    }

    throw new CatalogException("Unsupported operation");
  }

  @Override
  public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
      throws DatabaseNotExistException, CatalogException {
    throw new CatalogException("Unsupported operation");
  }

  @Override
  public List<String> listTables(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    try {
      var defaultFolder = store.getDefaultFolder();

      return Arrays.stream(defaultFolder.list("*"))
          .filter(
              folder -> {
                try {
                  return (folder.getType() & Folder.HOLDS_MESSAGES) != 0;
                } catch (MessagingException e) {
                  throw new CatalogException(e);
                }
              })
          .map(Folder::getFullName)
          .collect(Collectors.toList());
    } catch (MessagingException e) {
      throw new CatalogException(e);
    }
  }

  @Override
  public List<String> listViews(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    return new ArrayList<>();
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    if (!tableExists(tablePath)) {
      throw new TableNotExistException(name, tablePath);
    }

    var schema =
        TableSchema.builder()
            .field("uid", new AtomicDataType(new BigIntType(false)))
            .field("subject", DataTypes.STRING())
            .field("sent", DataTypes.TIMESTAMP())
            .field("received", DataTypes.TIMESTAMP())
            .field("to", DataTypes.ARRAY(DataTypes.STRING()))
            .field("cc", DataTypes.ARRAY(DataTypes.STRING()))
            .field("bcc", DataTypes.ARRAY(DataTypes.STRING()))
            .field("recipients", DataTypes.ARRAY(DataTypes.STRING()))
            .field("replyTo", DataTypes.ARRAY(DataTypes.STRING()))
            .field(
                "headers",
                DataTypes.ARRAY(
                    DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("value", DataTypes.STRING()))))
            .field("from", DataTypes.ARRAY(DataTypes.STRING()))
            .field("bytes", DataTypes.INT())
            .field("contentType", DataTypes.STRING())
            .field("content", DataTypes.STRING())
            .field("seen", DataTypes.BOOLEAN())
            .field("draft", DataTypes.BOOLEAN())
            .field("answered", DataTypes.BOOLEAN())
            .primaryKey("uid")
            .watermark(new WatermarkSpec("received", "received", DataTypes.TIMESTAMP()))
            .build();

    var properties = catalogOptions.toProperties();
    properties.put(FOLDER.key(), tablePath.getObjectName());

    return new CatalogTableImpl(schema, properties, null);
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) throws CatalogException {
    try {
      return store.getFolder(tablePath.getObjectName()).exists();
    } catch (MessagingException e) {
      return false;
    }
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    throw new CatalogException("Unsupported operation");
  }

  @Override
  public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
      throws TableNotExistException, TableAlreadyExistException, CatalogException {
    throw new CatalogException("Unsupported operation");
  }

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
      throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    throw new CatalogException("Unsupported operation");
  }

  @Override
  public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    throw new CatalogException("Unsupported operation");
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    if (!tableExists(tablePath)) {
      throw new TableNotExistException(name, tablePath);
    }

    throw new TableNotPartitionedException(name, tablePath);
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    if (!tableExists(tablePath)) {
      throw new TableNotExistException(name, tablePath);
    }

    throw new TableNotPartitionedException(name, tablePath);
  }

  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(
      ObjectPath tablePath, List<Expression> filters)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    if (!tableExists(tablePath)) {
      throw new TableNotExistException(name, tablePath);
    }

    throw new TableNotPartitionedException(name, tablePath);
  }

  @Override
  public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    throw new PartitionNotExistException(name, tablePath, partitionSpec);
  }

  @Override
  public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws CatalogException {
    throw new CatalogException("Unsupported operation");
  }

  @Override
  public void createPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition partition,
      boolean ignoreIfExists)
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
          PartitionAlreadyExistsException, CatalogException {
    if (!tableExists(tablePath)) {
      throw new TableNotExistException(name, tablePath);
    }

    throw new TableNotPartitionedException(name, tablePath);
  }

  @Override
  public void dropPartition(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new PartitionNotExistException(name, tablePath, partitionSpec);
  }

  @Override
  public void alterPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition newPartition,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new PartitionNotExistException(name, tablePath, partitionSpec);
  }

  @Override
  public List<String> listFunctions(String dbName)
      throws DatabaseNotExistException, CatalogException {
    return new ArrayList<>();
  }

  @Override
  public CatalogFunction getFunction(ObjectPath functionPath)
      throws FunctionNotExistException, CatalogException {
    throw new FunctionNotExistException(name, functionPath);
  }

  @Override
  public boolean functionExists(ObjectPath functionPath) throws CatalogException {
    return false;
  }

  @Override
  public void createFunction(
      ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
      throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
    throw new CatalogException("Unsupported operation");
  }

  @Override
  public void alterFunction(
      ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
      throws FunctionNotExistException, CatalogException {
    throw new CatalogException("Unsupported operation");
  }

  @Override
  public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
      throws FunctionNotExistException, CatalogException {
    throw new CatalogException("Unsupported operation");
  }

  @Override
  public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    return CatalogTableStatistics.UNKNOWN;
  }

  @Override
  public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    return CatalogColumnStatistics.UNKNOWN;
  }

  @Override
  public CatalogTableStatistics getPartitionStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    return CatalogTableStatistics.UNKNOWN;
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException, CatalogException {
    return CatalogColumnStatistics.UNKNOWN;
  }

  @Override
  public void alterTableStatistics(
      ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    throw new CatalogException("Unsupported operation");
  }

  @Override
  public void alterTableColumnStatistics(
      ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException, TablePartitionedException {
    throw new CatalogException("Unsupported operation");
  }

  @Override
  public void alterPartitionStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogTableStatistics partitionStatistics,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new CatalogException("Unsupported operation");
  }

  @Override
  public void alterPartitionColumnStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogColumnStatistics columnStatistics,
      boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new CatalogException("Unsupported operation");
  }
}
