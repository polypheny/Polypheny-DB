package org.polypheny.db.adapter.jdbc.stores;


import com.google.common.collect.ImmutableList;
import java.io.File;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.polypheny.db.PUID;
import org.polypheny.db.PUID.Type;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.adapter.jdbc.connection.TransactionalConnectionFactory;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedTable;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.dialect.HsqldbSqlDialect;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;


@Slf4j
public class HsqldbStore extends AbstractJdbcStore {

    @SuppressWarnings("WeakerAccess")
    public static final String ADAPTER_NAME = "HSQLDB";
    @SuppressWarnings("WeakerAccess")
    public static final String DESCRIPTION = "Java-based relational database system. It supports an in-memory and a persistent file based mode. Deploying a HSQLDB instance requires no additional dependencies to be installed or servers to be set up.";
    @SuppressWarnings("WeakerAccess")
    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingList( "type", false, true, false, ImmutableList.of( "Memory", "File" ) ),
            new AdapterSettingString( "path", false, true, false, "." + File.separator ),
            new AdapterSettingInteger( "maxConnections", false, true, false, 25 ),
            new AdapterSettingList( "trxControlMode", false, true, false, Arrays.asList( "locks", "mvlocks", "mvcc" ) ),
            new AdapterSettingList( "trxIsolationLevel", false, true, false, Arrays.asList( "read_committed", "serializable" ) )
    );


    public HsqldbStore( final int storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, createConnectionFactory( uniqueName, settings ), HsqldbSqlDialect.DEFAULT );
    }


    public static ConnectionFactory createConnectionFactory( final String uniqueName, final Map<String, String> settings ) {
        if ( RuntimeConfig.TWO_PC_MODE.getBoolean() ) {
            // TODO MV: implement
            throw new RuntimeException( "TWO PC Mode is not implemented" );
        } else {
            BasicDataSource dataSource = new BasicDataSource();
            dataSource.setDriverClassName( "org.hsqldb.jdbcDriver" );
            String trxSettings = ";hsqldb.tx=" + settings.get( "trxControlMode" ) + ";hsqldb.tx_level=" + settings.get( "trxIsolationLevel" );
            if ( settings.get( "type" ).equals( "Memory" ) ) {
                dataSource.setUrl( "jdbc:hsqldb:mem:" + uniqueName + trxSettings );
            } else {
                String path = settings.get( "path" );
                dataSource.setUrl( "jdbc:hsqldb:file:" + path + uniqueName + trxSettings );
            }
            dataSource.setUsername( "sa" );
            dataSource.setPassword( "" );
            dataSource.setMaxTotal( -1 ); // No limit for number of connections (limited by connection handler; see settings maxConnections)
            dataSource.setDefaultAutoCommit( false );
            return new TransactionalConnectionFactory( dataSource, Integer.parseInt( settings.get( "maxConnections" ) ) );
        }

    }


    @Override
    public Table createTableSchema( CatalogCombinedTable combinedTable ) {
        return currentJdbcSchema.createJdbcTable( combinedTable );
    }


    @Override
    public Schema getCurrentSchema() {
        return currentJdbcSchema;
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
        switch ( type ) {
            case BOOLEAN:
                return "BOOLEAN";
            case VARBINARY:
                return "VARBINARY";
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
        }
        throw new RuntimeException( "Unknown type: " + type.name() );
    }

}
