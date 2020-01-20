package ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.stores;


import ch.unibas.dmi.dbis.polyphenydb.PUID;
import ch.unibas.dmi.dbis.polyphenydb.PUID.Type;
import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.PolyXid;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.connection.ConnectionFactory;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.connection.ConnectionHandlerException;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.connection.TransactionalConnectionFactory;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.HsqldbSqlDialect;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;


@Slf4j
public class HsqldbStore extends AbstractJdbcStore {

    @SuppressWarnings("WeakerAccess")
    public static final String ADAPTER_NAME = "HSQLDB";
    @SuppressWarnings("WeakerAccess")
    public static final String DESCRIPTION = "Java-based relational database system.";
    @SuppressWarnings("WeakerAccess")
    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingList( "type", false, true, false, ImmutableList.of( "Memory", "File" ) ),
            new AdapterSettingString( "path", false, true, false, "." + File.separator ),
            new AdapterSettingInteger( "maxConnections", false, true, false, 25 )
    );


    public HsqldbStore( final int storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, createConnectionFactory( uniqueName, settings ), HsqldbSqlDialect.DEFAULT );
    }


    public static ConnectionFactory createConnectionFactory( final String uniqueName, final Map<String, String> settings ) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName( "org.hsqldb.jdbcDriver" );
        if ( settings.get( "type" ).equals( "Memory" ) ) {
            dataSource.setUrl( "jdbc:hsqldb:mem:" + uniqueName + ";hsqldb.tx=mvcc" );
        } else {
            String path = settings.get( "path" );
            dataSource.setUrl( "jdbc:hsqldb:file:" + path + uniqueName + ";hsqldb.tx=mvcc" );
        }
        dataSource.setUsername( "sa" );
        dataSource.setPassword( "" );
        dataSource.setDefaultAutoCommit( false );
        return new TransactionalConnectionFactory( dataSource, Integer.parseInt( settings.get( "maxConnections" ) ) );
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
    protected String getTypeString( PolySqlType polySqlType ) {
        switch ( polySqlType ) {
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
            case TEXT:
                throw new RuntimeException( "Unsupported datatype: " + polySqlType.name() );
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
        }
        throw new RuntimeException( "Unknown type: " + polySqlType.name() );
    }

}
