/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.jdbc.stores;


import com.google.common.collect.ImmutableList;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.TransactionalConnectionFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.dialect.MonetdbSqlDialect;
import org.polypheny.db.type.PolyType;


@Slf4j
public class MonetdbStore extends AbstractJdbcStore {

    public static final String ADAPTER_NAME = "MonetDB";

    public static final String DESCRIPTION = "MonetDB is an open-source column-oriented database management system. It is based on an optimistic concurrency control.";

    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingString( "host", false, true, false, "localhost" ),
            new AdapterSettingInteger( "port", false, true, false, 50000 ),
            new AdapterSettingString( "database", false, true, false, "polypheny" ),
            new AdapterSettingString( "username", false, true, false, "polypheny" ),
            new AdapterSettingString( "password", false, true, false, "" ),
            new AdapterSettingInteger( "maxConnections", false, true, false, 25 )
    );


    public MonetdbStore( int storeId, String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, createConnectionFactory( settings ), MonetdbSqlDialect.DEFAULT, true );
    }


    public static ConnectionFactory createConnectionFactory( final Map<String, String> settings ) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName( "nl.cwi.monetdb.jdbc.MonetDriver" );

        final String connectionUrl = getConnectionUrl( settings.get( "host" ), Integer.parseInt( settings.get( "port" ) ), settings.get( "database" ) );
        dataSource.setUrl( connectionUrl );
        log.info( "MonetDB Connection URL: {}", connectionUrl );
        dataSource.setUsername( settings.get( "username" ) );
        dataSource.setPassword( settings.get( "password" ) );
        dataSource.setDefaultAutoCommit( false );
        return new TransactionalConnectionFactory( dataSource, Integer.parseInt( settings.get( "maxConnections" ) ) );
    }


    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn ) {
        // MonetDB does not support updating the column type directly. We need to do a work-around
        CatalogTable catalogTable;
        try {
            catalogTable = Catalog.getInstance().getTable( catalogColumn.tableId );
        } catch ( UnknownTableException | GenericCatalogException e ) {
            throw new RuntimeException( e );
        }
        String tmpColName = columnPlacement.physicalColumnName + "tmp";
        StringBuilder builder;

        // (1) Create a temporary column `alter table tabX add column colXtemp NEW_TYPE;`
        builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( columnPlacement.physicalSchemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( columnPlacement.physicalTableName ) );
        builder.append( " ADD COLUMN " )
                .append( dialect.quoteIdentifier( tmpColName ) )
                .append( " " )
                .append( getTypeString( catalogColumn.type ) );
        executeUpdate( builder, context );

        // (2) Set data in temporary column to original data `update tabX set colXtemp=colX;`
        builder = new StringBuilder();
        builder.append( "UPDATE " )
                .append( dialect.quoteIdentifier( columnPlacement.physicalSchemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( columnPlacement.physicalTableName ) );
        builder.append( " SET " )
                .append( dialect.quoteIdentifier( tmpColName ) )
                .append( "=" )
                .append( dialect.quoteIdentifier( columnPlacement.physicalColumnName ) );
        executeUpdate( builder, context );

        // (3) Remove the original column `alter table tabX drop column colX;`
        builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( columnPlacement.physicalSchemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( columnPlacement.physicalTableName ) );
        builder.append( " DROP COLUMN " )
                .append( dialect.quoteIdentifier( columnPlacement.physicalColumnName ) );
        executeUpdate( builder, context );

        // (4) Re-create the original column with the new type `alter table tabX add column colX NEW_TYPE;
        builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( columnPlacement.physicalSchemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( columnPlacement.physicalTableName ) );
        builder.append( " ADD COLUMN " )
                .append( dialect.quoteIdentifier( columnPlacement.physicalColumnName ) )
                .append( " " )
                .append( getTypeString( catalogColumn.type ) );
        executeUpdate( builder, context );

        // (5) Move data from temporary column to new column `update tabX set colX=colXtemp`;
        builder = new StringBuilder();
        builder.append( "UPDATE " )
                .append( dialect.quoteIdentifier( columnPlacement.physicalSchemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( columnPlacement.physicalTableName ) );
        builder.append( " SET " )
                .append( dialect.quoteIdentifier( columnPlacement.physicalColumnName ) )
                .append( "=" )
                .append( dialect.quoteIdentifier( tmpColName ) );
        executeUpdate( builder, context );

        // (6) Drop the temporary column `alter table tabX drop column colXtemp;`
        builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( columnPlacement.physicalSchemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( columnPlacement.physicalTableName ) );
        builder.append( " DROP COLUMN " )
                .append( dialect.quoteIdentifier( tmpColName ) );
        executeUpdate( builder, context );
    }


    @Override
    public Table createTableSchema( CatalogTable catalogTable ) {
        return currentJdbcSchema.createJdbcTable( catalogTable );
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
            connectionFactory.close();
        } catch ( SQLException e ) {
            log.warn( "Exception while shutting down {}", getUniqueName(), e );
        }
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        // TODO: Implement disconnect and reconnect to MonetDB instance.
    }


    @Override
    protected String getTypeString( PolyType type ) {
        switch ( type ) {
            case BOOLEAN:
                return "BOOLEAN";
            case VARBINARY:
                throw new RuntimeException( "Unsupported datatype: " + type.name() );
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
                return "DOUBLE";
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
                return "TEXT";
        }
        throw new RuntimeException( "Unknown type: " + type.name() );
    }


    @Override
    protected String getDefaultPhysicalSchemaName() {
        return "public";
    }


    private static String getConnectionUrl( final String dbHostname, final int dbPort, final String dbName ) {
        return String.format( "jdbc:monetdb://%s:%d/%s", dbHostname, dbPort, dbName );
    }

}
