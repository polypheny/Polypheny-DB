package org.polypheny.db.adapter.jdbc.stores;


import com.google.common.collect.ImmutableList;
import java.io.File;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.adapter.jdbc.connection.TransactionalConnectionFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.SqlDialect;
import org.polypheny.db.sql.dialect.HsqldbSqlDialect;
import org.polypheny.db.transaction.PUID;
import org.polypheny.db.transaction.PUID.Type;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.util.FileSystemManager;


@Slf4j
public class HsqldbStore extends AbstractJdbcStore {

    @SuppressWarnings("WeakerAccess")
    public static final String ADAPTER_NAME = "HSQLDB";
    @SuppressWarnings("WeakerAccess")
    public static final String DESCRIPTION = "Java-based relational database system. It supports an in-memory and a persistent file based mode. Deploying a HSQLDB instance requires no additional dependencies to be installed or servers to be set up.";
    @SuppressWarnings("WeakerAccess")
    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingList( "type", false, true, false, ImmutableList.of( "Memory", "File" ) ),
            new AdapterSettingList( "tableType", false, true, false, ImmutableList.of( "Memory", "Cached" ) ),
            new AdapterSettingInteger( "maxConnections", false, true, false, 25 ),
            new AdapterSettingList( "trxControlMode", false, true, false, Arrays.asList( "locks", "mvlocks", "mvcc" ) ),
            new AdapterSettingList( "trxIsolationLevel", false, true, false, Arrays.asList( "read_committed", "serializable" ) )
    );


    public HsqldbStore( final int storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, createConnectionFactory( storeId, uniqueName, settings, HsqldbSqlDialect.DEFAULT ), HsqldbSqlDialect.DEFAULT, settings.get( "type" ).equals( "File" ) );
    }


    public static ConnectionFactory createConnectionFactory( final int storeId, final String uniqueName, final Map<String, String> settings, SqlDialect dialect ) {
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
                dataSource.setUrl( "jdbc:hsqldb:mem:" + uniqueName + trxSettings );
            } else {
                File path = FileSystemManager.getInstance().registerNewFolder( "data/hsqldb/" + storeId );
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
    public Table createTableSchema( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore ) {
        return currentJdbcSchema.createJdbcTable( catalogTable, columnPlacementsOnStore );
    }


    @Override
    public Schema getCurrentSchema() {
        return currentJdbcSchema;
    }


    @Override
    public void addIndex( Context context, CatalogIndex catalogIndex ) {
        List<CatalogColumnPlacement> ccps = Catalog.getInstance().getColumnPlacementsOnAdapter( getAdapterId(), catalogIndex.key.tableId );
        StringBuilder builder = new StringBuilder();
        builder.append( "CREATE " );
        if ( catalogIndex.unique ) {
            builder.append( "UNIQUE INDEX " );
        } else {
            builder.append( "INDEX " );
        }
        String physicalIndexName = getPhysicalIndexName( catalogIndex.key.tableId, catalogIndex.id );
        builder.append( dialect.quoteIdentifier( physicalIndexName ) );
        builder.append( " ON " )
                .append( dialect.quoteIdentifier( ccps.get( 0 ).physicalSchemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( ccps.get( 0 ).physicalTableName ) );

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

        Catalog.getInstance().setIndexPhysicalName( catalogIndex.id, physicalIndexName );
    }


    @Override
    public void dropIndex( Context context, CatalogIndex catalogIndex ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "DROP INDEX " );
        builder.append( dialect.quoteIdentifier( catalogIndex.physicalName ) );
        executeUpdate( builder, context );
    }


    @Override
    public String getAdapterName() {
        return ADAPTER_NAME;
    }


    @Override
    public List<AdapterSetting> getAvailableSettings() {
        return AVAILABLE_SETTINGS;
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
            removeInformationPage();
            // TODO MV: Find better solution then generating random XID
            connectionFactory.getOrCreateConnectionHandler( PolyXid.generateLocalTransactionIdentifier( PUID.randomPUID( Type.NODE ), PUID.randomPUID( Type.TRANSACTION ) ) ).execute( "SHUTDOWN" );
            connectionFactory.close();
        } catch ( SQLException | ConnectionHandlerException e ) {
            log.warn( "Exception while shutting down {}", getUniqueName(), e );
        }
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
