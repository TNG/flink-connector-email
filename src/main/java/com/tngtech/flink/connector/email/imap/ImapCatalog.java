package com.tngtech.flink.connector.email.imap;

import com.tngtech.flink.connector.email.common.SessionProperties;
import jakarta.mail.Folder;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.Store;
import lombok.RequiredArgsConstructor;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;

import java.util.*;
import java.util.stream.Collectors;

import static com.tngtech.flink.connector.email.imap.ImapConfigOptions.FOLDER;

@PublicEvolving
@RequiredArgsConstructor
public class ImapCatalog implements Catalog {

    private static final String DEFAULT_DATABASE = "folders";
    private static final EnumSet<ReadableMetadata> DEFAULT_METADATA = EnumSet.of(
        ReadableMetadata.UID,
        ReadableMetadata.SUBJECT,
        ReadableMetadata.SENT,
        ReadableMetadata.RECEIVED,
        ReadableMetadata.FROM,
        ReadableMetadata.TO,
        ReadableMetadata.CC,
        ReadableMetadata.BCC
    );

    private final String name;
    private final ImapCatalogOptions options;

    private Store store;

    @Override
    public void open() throws CatalogException {
        reconnect();
    }

    @Override
    public void close() throws CatalogException {
        if (store != null) {
            try {
                store.close();
            } catch (MessagingException e) {
                throw new CatalogException(e.getMessage(), e);
            }
        }
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new ImapSourceFactory());
    }

    // ---------------------------------------------------------------------------------------------

    private void reconnect() throws CatalogException {
        if (store != null && store.isConnected()) {
            return;
        }

        if (store == null) {
            final SessionProperties sessionProperties = new SessionProperties(options);
            final Session session = Session.getInstance(sessionProperties.getProperties());

            try {
                store = session.getStore();
            } catch (MessagingException e) {
                throw new CatalogException(e.getMessage(), e);
            }
        }

        try {
            if (options.usesAuthentication()) {
                store.connect(options.getUser(), options.getPassword());
            } else {
                store.connect("", "");
            }
        } catch (MessagingException e) {
            throw new CatalogException(e.getMessage(), e);
        }
    }

    // ---------------------------------------------------------------------------------------------

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return DEFAULT_DATABASE;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return Collections.singletonList(DEFAULT_DATABASE);
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
        throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(name, databaseName);
        }

        return new CatalogDatabaseImpl(new HashMap<>(), "Lists all folders as source tables");
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return getDefaultDatabase().equals(databaseName);
    }

    @Override
    public void createDatabase(
        String databaseName,
        CatalogDatabase database,
        boolean ignoreIfExists
    ) throws DatabaseAlreadyExistException, CatalogException {
        if (databaseExists(databaseName)) {
            if (ignoreIfExists) {
                return;
            }

            throw new DatabaseAlreadyExistException(name, databaseName);
        }

        throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
    }

    @Override
    public void dropDatabase(
        String databaseName,
        boolean ignoreIfNotExists,
        boolean cascade
    ) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new DatabaseNotExistException(name, databaseName);
        }

        throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
    }

    @Override
    public void alterDatabase(
        String databaseName,
        CatalogDatabase newDatabase,
        boolean ignoreIfNotExists
    ) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new DatabaseNotExistException(name, databaseName);
        }

        throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
    }

    @Override
    public List<String> listTables(
        String databaseName
    ) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(name, databaseName);
        }

        reconnect();
        try {
            return Arrays.stream(store.getDefaultFolder().list("*"))
                .filter(
                    folder -> {
                        try {
                            return (folder.getType() & Folder.HOLDS_MESSAGES) != 0;
                        } catch (MessagingException e) {
                            throw new CatalogException(e.getMessage(), e);
                        }
                    })
                .map(Folder::getFullName)
                .collect(Collectors.toList());
        } catch (MessagingException e) {
            throw new CatalogException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> listViews(
        String databaseName
    ) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(
        ObjectPath tablePath
    ) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(name, tablePath);
        }

        Schema.Builder schemaBuilder = Schema.newBuilder();
        DEFAULT_METADATA.forEach(
            metadata -> schemaBuilder.columnByMetadata(metadata.getKey(), metadata.getType()));

        final Map<String, String> sourceOptions = new HashMap<>(options.toOptions());
        sourceOptions.put(FOLDER.key(), tablePath.getObjectName());

        return CatalogTable.of(schemaBuilder.build(), null, Collections.emptyList(), sourceOptions);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        reconnect();
        try {
            return store.getFolder(tablePath.getObjectName()).exists();
        } catch (MessagingException e) {
            return false;
        }
    }

    @Override
    public void dropTable(
        ObjectPath tablePath,
        boolean ignoreIfNotExists
    ) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new TableNotExistException(name, tablePath);
        }

        throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
    }

    @Override
    public void renameTable(
        ObjectPath tablePath,
        String newTableName,
        boolean ignoreIfNotExists
    ) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new TableNotExistException(name, tablePath);
        }

        final ObjectPath newTablePath = new ObjectPath(tablePath.getDatabaseName(), newTableName);
        if (tableExists(newTablePath)) {
            throw new TableNotExistException(name, newTablePath);
        }

        throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
    }

    @Override
    public void createTable(
        ObjectPath tablePath,
        CatalogBaseTable table,
        boolean ignoreIfExists
    ) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(name, tablePath.getDatabaseName());
        }

        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                return;
            }

            throw new TableAlreadyExistException(name, tablePath);
        }

        throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
    }

    @Override
    public void alterTable(
        ObjectPath tablePath,
        CatalogBaseTable newTable,
        boolean ignoreIfNotExists
    ) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new TableNotExistException(name, tablePath);
        }

        throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
        ObjectPath tablePath
    ) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
        ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec
    ) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
        ObjectPath tablePath,
        List<Expression> filters
    ) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
        throws PartitionNotExistException, CatalogException {

        throw new PartitionNotExistException(name, tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(
        ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec
    ) throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(
        ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec,
        CatalogPartition partition,
        boolean ignoreIfExists
    ) throws PartitionAlreadyExistsException, CatalogException {
        if (partitionExists(tablePath, partitionSpec)) {
            if (ignoreIfExists) {
                return;
            }

            throw new PartitionAlreadyExistsException(name, tablePath, partitionSpec);
        }

        throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
    }

    @Override
    public void dropPartition(
        ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec,
        boolean ignoreIfNotExists
    ) throws PartitionNotExistException, CatalogException {
        if (!partitionExists(tablePath, partitionSpec)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new PartitionNotExistException(name, tablePath, partitionSpec);
        }

        throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
    }

    @Override
    public void alterPartition(
        ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec,
        CatalogPartition newPartition,
        boolean ignoreIfNotExists
    ) throws PartitionNotExistException, CatalogException {
        if (!partitionExists(tablePath, partitionSpec)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new PartitionNotExistException(name, tablePath, partitionSpec);
        }

        throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
    }

    @Override
    public List<String> listFunctions(
        String dbName
    ) throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(
        ObjectPath functionPath
    ) throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(name, functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(
        ObjectPath functionPath,
        CatalogFunction function,
        boolean ignoreIfExists
    ) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (functionExists(functionPath)) {
            if (ignoreIfExists) {
                return;
            }

            throw new FunctionAlreadyExistException(name, functionPath);
        }

        throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
    }

    @Override
    public void alterFunction(
        ObjectPath functionPath,
        CatalogFunction newFunction,
        boolean ignoreIfNotExists
    ) throws FunctionNotExistException, CatalogException {
        if (!functionExists(functionPath)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new FunctionNotExistException(name, functionPath);
        }

        throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
    }

    @Override
    public void dropFunction(
        ObjectPath functionPath,
        boolean ignoreIfNotExists
    ) throws FunctionNotExistException, CatalogException {
        if (!functionExists(functionPath)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new FunctionNotExistException(name, functionPath);
        }

        throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
    }

    @Override
    public CatalogTableStatistics getTableStatistics(
        ObjectPath tablePath
    ) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(name, tablePath);
        }

        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(
        ObjectPath tablePath
    ) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(name, tablePath);
        }

        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
        ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec
    ) throws PartitionNotExistException, CatalogException {
        if (!partitionExists(tablePath, partitionSpec)) {
            throw new PartitionNotExistException(name, tablePath, partitionSpec);
        }

        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
        ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec
    ) throws PartitionNotExistException, CatalogException {
        if (!partitionExists(tablePath, partitionSpec)) {
            throw new PartitionNotExistException(name, tablePath, partitionSpec);
        }

        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
        ObjectPath tablePath,
        CatalogTableStatistics tableStatistics,
        boolean ignoreIfNotExists
    ) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new TableNotExistException(name, tablePath);
        }

        throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
    }

    @Override
    public void alterTableColumnStatistics(
        ObjectPath tablePath,
        CatalogColumnStatistics columnStatistics,
        boolean ignoreIfNotExists
    ) throws TableNotExistException, CatalogException, TablePartitionedException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new TableNotExistException(name, tablePath);
        }

        throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
    }

    @Override
    public void alterPartitionStatistics(
        ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec,
        CatalogTableStatistics partitionStatistics,
        boolean ignoreIfNotExists
    ) throws PartitionNotExistException, CatalogException {
        if (!partitionExists(tablePath, partitionSpec)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new PartitionNotExistException(name, tablePath, partitionSpec);
        }

        throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
    }

    @Override
    public void alterPartitionColumnStatistics(
        ObjectPath tablePath,
        CatalogPartitionSpec partitionSpec,
        CatalogColumnStatistics columnStatistics,
        boolean ignoreIfNotExists
    ) throws PartitionNotExistException, CatalogException {
        if (!partitionExists(tablePath, partitionSpec)) {
            if (ignoreIfNotExists) {
                return;
            }

            throw new PartitionNotExistException(name, tablePath, partitionSpec);
        }

        throw new UnsupportedOperationException("Not supported by the IMAP catalog.");
    }
}
