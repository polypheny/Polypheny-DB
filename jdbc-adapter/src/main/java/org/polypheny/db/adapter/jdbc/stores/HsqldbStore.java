package org.polypheny.db.adapter.jdbc.stores;


import com.google.common.collect.ImmutableList;
import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.adapter.Adapter.AdapterSettingInteger;
import org.polypheny.db.adapter.Adapter.AdapterSettingList;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.adapter.jdbc.connection.TransactionalConnectionFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.sql.dialect.HsqldbSqlDialect;
import org.polypheny.db.transaction.PUID;
import org.polypheny.db.transaction.PUID.Type;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.util.PolyphenyHomeDirManager;


@Slf4j
@AdapterProperties(
        name = "HSQLDB",
        description = "Java-based relational database system. It supports an in-memory and a persistent file based mode. Deploying a HSQLDB instance requires no additional dependencies to be installed or servers to be set up.",
        usedModes = DeployMode.EMBEDDED)
@AdapterSettingList(name = "tableType", options = { "Memory", "Cached" }, position = 1)
@AdapterSettingInteger(name = "maxConnections", defaultValue = 25)
@AdapterSettingList(name = "trxControlMode", options = { "locks", "mvlocks", "mvcc" })
@AdapterSettingList(name = "trxIsolationLevel", options = { "read_committed", "serializable" })
@AdapterSettingList(name = "type", options = { "Memory", "File" })
public class HsqldbStore extends AbstractJdbcStore {

    public HsqldbStore( final int storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, HsqldbSqlDialect.DEFAULT, settings.get( "type" ).equals( "File" ) );
    }


    @Override
    protected ConnectionFactory deployEmbedded() {
        if ( RuntimeConfig.TWO_PC_MODE.getBoolean() ) {
            // TODO MV: implement
            throw new RuntimeException( "2PC Mode is not implemented" );
        } else {
            BasicDataSource dataSource = new BasicDataSource();
            dataSource.setDriverClassName( "org.hsqldb.jdbcDriver" );
            String trxSettings = ";hsqldb.tx=" + settings.get( "trxControlMode" ) + ";hsqldb.tx_level=" + settings.get( "trxIsolationLevel" );
            if ( settings.get( "tableType" ).equals( "Memory" ) ) {
                trxSettings += ";hsqldb.default_table_type=memory";
            } else {
                trxSettings += ";hsqldb.default_table_type=cached";
            }
            if ( settings.get( "type" ).equals( "Memory" ) ) {
                dataSource.setUrl( "jdbc:hsqldb:mem:" + getUniqueName() + trxSettings );
            } else {
                File path = PolyphenyHomeDirManager.getInstance().registerNewFolder( "data/hsqldb/" + getAdapterId() );
                dataSource.setUrl( "jdbc:hsqldb:file:" + path + trxSettings );
            }
            dataSource.setUsername( "sa" );
            dataSource.setPassword( "" );
            dataSource.setMaxTotal( -1 ); // No limit for number of connections (limited by connection handler; see settings maxConnections)
            dataSource.setDefaultAutoCommit( false );
            return new TransactionalConnectionFactory( dataSource, Integer.parseInt( settings.get( "maxConnections" ) ), dialect );
        }
    }


    @Override
    public Table createTableSchema( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CatalogPartitionPlacement partitionPlacement ) {
        return currentJdbcSchema.createJdbcTable( catalogTable, columnPlacementsOnStore, partitionPlacement );
    }


    @Override
    public Schema getCurrentSchema() {
        return currentJdbcSchema;
    }


    @Override
    public void addIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds ) {
        List<CatalogColumnPlacement> ccps = Catalog.getInstance().getColumnPlacementsOnAdapterPerTable( getAdapterId(), catalogIndex.key.tableId );
        List<CatalogPartitionPlacement> partitionPlacements = new ArrayList<>();
        partitionIds.forEach( id -> partitionPlacements.add( catalog.getPartitionPlacement( getAdapterId(), id ) ) );

        String physicalIndexName = getPhysicalIndexName( catalogIndex.key.tableId, catalogIndex.id );
        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {

            StringBuilder builder = new StringBuilder();
            builder.append( "CREATE " );
            if ( catalogIndex.unique ) {
                builder.append( "UNIQUE INDEX " );
            } else {
                builder.append( "INDEX " );
            }

            builder.append( dialect.quoteIdentifier( physicalIndexName + "_" + partitionPlacement.partitionId ) );
            builder.append( " ON " )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalSchemaName ) )
                    .append( "." )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalTableName ) );

            builder.append( "(" );
            boolean first = true;
            for ( long columnId : catalogIndex.key.columnIds ) {
                if ( !first ) {
                    builder.append( ", " );
                }
                first = false;
                builder.append( dialect.quoteIdentifier( getPhysicalColumnName( columnId ) ) ).append( " " );
            }
            builder.append( ")" );
            executeUpdate( builder, context );
        }
        Catalog.getInstance().setIndexPhysicalName( catalogIndex.id, physicalIndexName );
    }


    @Override
    public void dropIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds ) {
        List<CatalogPartitionPlacement> partitionPlacements = new ArrayList<>();
        partitionIds.forEach( id -> partitionPlacements.add( catalog.getPartitionPlacement( getAdapterId(), id ) ) );

        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
            StringBuilder builder = new StringBuilder();
            builder.append( "DROP INDEX " );
            builder.append( dialect.quoteIdentifier( catalogIndex.physicalName + "_" + partitionPlacement.partitionId ) );
            executeUpdate( builder, context );
        }
    }


    @Override
    public List<AvailableIndexMethod> getAvailableIndexMethods() {
        return ImmutableList.of(
                new AvailableIndexMethod( "default", "Default" )
        );
    }


    @Override
    public AvailableIndexMethod getDefaultIndexMethod() {
        return getAvailableIndexMethods().get( 0 );
    }


    @Override
    public List<FunctionalIndexInfo> getFunctionalIndexes( CatalogTable catalogTable ) {
        return ImmutableList.of();
    }


    @Override
    public void shutdown() {
        try {
            // TODO MV: Find better solution then generating random XID
            connectionFactory.getOrCreateConnectionHandler( PolyXid.generateLocalTransactionIdentifier( PUID.randomPUID( Type.NODE ), PUID.randomPUID( Type.TRANSACTION ) ) ).execute( "SHUTDOWN" );
        } catch ( SQLException | ConnectionHandlerException e ) {
            log.warn( "Exception while shutting down {}", getUniqueName(), e );
        }
        super.shutdown();
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        // There is no modifiable setting for this store
    }


    @Override
    protected String getTypeString( PolyType type ) {
        if ( type.getFamily() == PolyTypeFamily.MULTIMEDIA ) {
            return "BLOB(" + RuntimeConfig.UI_UPLOAD_SIZE_MB.getInteger() + "M)";
        }
        switch ( type ) {
            case BOOLEAN:
                return "BOOLEAN";
            case VARBINARY:
                return "VARBINARY";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case REAL:
                return "REAL";
            case DOUBLE:
                return "FLOAT";
            case DECIMAL:
                return "DECIMAL";
            case VARCHAR:
            case JSON:
                return "VARCHAR";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
            case ARRAY:
                return "LONGVARCHAR";
        }
        throw new RuntimeException( "Unknown type: " + type.name() );
    }


    @Override
    protected String getDefaultPhysicalSchemaName() {
        return "PUBLIC";
    }

}
