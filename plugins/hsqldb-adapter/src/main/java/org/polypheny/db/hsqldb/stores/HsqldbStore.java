/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.hsqldb.stores;


import com.google.common.collect.ImmutableList;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingList;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.adapter.jdbc.connection.TransactionalConnectionFactory;
import org.polypheny.db.adapter.jdbc.stores.AbstractJdbcStore;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.plugins.PolyPluginManager;
import org.polypheny.db.prepare.Context;
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
        usedModes = DeployMode.EMBEDDED,
        defaultMode = DeployMode.EMBEDDED)
@AdapterSettingList(name = "tableType", options = { "Memory", "Cached" }, position = 1, defaultValue = "Memory")
@AdapterSettingInteger(name = "maxConnections", defaultValue = 25)
@AdapterSettingList(name = "trxControlMode", options = { "locks", "mvlocks", "mvcc" }, defaultValue = "mvcc")
@AdapterSettingList(name = "trxIsolationLevel", options = { "read_committed", "serializable" }, defaultValue = "read_committed")
@AdapterSettingList(name = "type", options = { "Memory", "File" }, defaultValue = "Memory")
public class HsqldbStore extends AbstractJdbcStore {


    public HsqldbStore( final long storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, HsqldbSqlDialect.DEFAULT, settings.get( "type" ).equals( "File" ) );
    }


    @Override
    protected ConnectionFactory deployEmbedded() {
        if ( RuntimeConfig.TWO_PC_MODE.getBoolean() ) {
            // TODO MV: implement
            throw new GenericRuntimeException( "2PC Mode is not implemented" );
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
            dataSource.setDefaultTransactionIsolation( Connection.TRANSACTION_READ_COMMITTED );
            dataSource.setDriverClassLoader( PolyPluginManager.getMainClassLoader() );
            return new TransactionalConnectionFactory( dataSource, Integer.parseInt( settings.get( "maxConnections" ) ), dialect );
        }
    }


    @Override
    public String addIndex( Context context, LogicalIndex index, AllocationTable allocation ) {
        PhysicalTable physical = adapterCatalog.fromAllocation( allocation.id );

        String physicalIndexName = getPhysicalIndexName( physical.id, index.id );

        StringBuilder builder = new StringBuilder();
        builder.append( "CREATE " );
        if ( index.unique ) {
            builder.append( "UNIQUE INDEX " );
        } else {
            builder.append( "INDEX " );
        }

        builder.append( dialect.quoteIdentifier( physicalIndexName ) );
        builder.append( " ON " )
                .append( dialect.quoteIdentifier( physical.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( physical.name ) );

        builder.append( "(" );
        boolean first = true;
        for ( long columnId : index.key.fieldIds ) {
            if ( !first ) {
                builder.append( ", " );
            }
            first = false;
            builder.append( dialect.quoteIdentifier( getPhysicalColumnName( columnId ) ) ).append( " " );
        }
        builder.append( ")" );
        executeUpdate( builder, context );
        return physicalIndexName;
    }


    @Override
    public void dropIndex( Context context, LogicalIndex index, long allocId ) {

        PhysicalTable physical = adapterCatalog.fromAllocation( allocId );

        String physicalIndexName = getPhysicalIndexName( physical.id, index.id );

        StringBuilder builder = new StringBuilder();
        builder.append( "DROP INDEX " );
        builder.append( dialect.quoteIdentifier( physicalIndexName ) );
        executeUpdate( builder, context );

    }


    @Override
    public List<IndexMethodModel> getAvailableIndexMethods() {
        return ImmutableList.of(
                new IndexMethodModel( "default", "Default" )
        );
    }


    @Override
    public IndexMethodModel getDefaultIndexMethod() {
        return getAvailableIndexMethods().get( 0 );
    }


    @Override
    public List<FunctionalIndexInfo> getFunctionalIndexes( LogicalTable catalogTable ) {
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
        return switch ( type ) {
            case BOOLEAN -> "BOOLEAN";
            case VARBINARY -> "VARBINARY";
            case TINYINT -> "TINYINT";
            case SMALLINT -> "SMALLINT";
            case INTEGER -> "INT";
            case BIGINT -> "BIGINT";
            case REAL -> "REAL";
            case DOUBLE -> "FLOAT";
            case DECIMAL -> "DECIMAL";
            case VARCHAR -> "VARCHAR";
            case DATE -> "DATE";
            case TIME -> "TIME";
            case TIMESTAMP -> "TIMESTAMP";
            case ARRAY -> "LONGVARCHAR";
            case TEXT -> "VARCHAR(200000)"; // clob can sadly not be used as pk which puts arbitrary limit on the value
            case JSON, NODE, EDGE, DOCUMENT -> "LONGVARCHAR";
            default -> throw new GenericRuntimeException( "Unknown type: " + type.name() );
        };
    }


    @Override
    public String getDefaultPhysicalSchemaName() {
        return "PUBLIC";
    }


}
