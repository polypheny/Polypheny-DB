/*
 * Copyright 2019-2021 The Polypheny Project
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
import java.sql.Connection;
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
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.SqlDialect;
import org.polypheny.db.sql.dialect.PostgresqlSqlDialect;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;


@Slf4j
public class PostgresqlStore extends AbstractJdbcStore {

    public static final String ADAPTER_NAME = "PostgreSQL";

    public static final String DESCRIPTION = "Relational database system optimized for transactional workload that provides an advanced set of features. PostgreSQL is fully ACID compliant and ensures that all requirements are met.";

    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingString( "host", false, true, false, "localhost" ),
            new AdapterSettingInteger( "port", false, true, false, 5432 ),
            new AdapterSettingString( "database", false, true, false, "polypheny" ),
            new AdapterSettingString( "username", false, true, false, "polypheny" ),
            new AdapterSettingString( "password", false, true, false, "polypheny" ),
            new AdapterSettingInteger( "maxConnections", false, true, false, 25 )
    );


    public PostgresqlStore( int storeId, String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, createConnectionFactory( settings, PostgresqlSqlDialect.DEFAULT ), PostgresqlSqlDialect.DEFAULT, true );
    }


    public static ConnectionFactory createConnectionFactory( final Map<String, String> settings, SqlDialect dialect ) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName( "org.postgresql.Driver" );

        final String connectionUrl = getConnectionUrl( settings.get( "host" ), Integer.parseInt( settings.get( "port" ) ), settings.get( "database" ) );
        dataSource.setUrl( connectionUrl );
        if ( log.isInfoEnabled() ) {
            log.info( "Postgres Connection URL: {}", connectionUrl );
        }
        dataSource.setUsername( settings.get( "username" ) );
        dataSource.setPassword( settings.get( "password" ) );
        dataSource.setDefaultAutoCommit( false );
        dataSource.setDefaultTransactionIsolation( Connection.TRANSACTION_READ_UNCOMMITTED );
        return new TransactionalConnectionFactory( dataSource, Integer.parseInt( settings.get( "maxConnections" ) ), dialect );
    }


    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn, PolyType oldType ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( columnPlacement.physicalSchemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( columnPlacement.physicalTableName ) );
        builder.append( " ALTER COLUMN " ).append( dialect.quoteIdentifier( columnPlacement.physicalColumnName ) );
        builder.append( " TYPE " ).append( getTypeString( catalogColumn.type ) );
        if ( catalogColumn.collectionsType != null ) {
            builder.append( " " ).append( catalogColumn.collectionsType.toString() );
        }
        if ( catalogColumn.length != null ) {
            builder.append( "(" );
            builder.append( catalogColumn.length );
            if ( catalogColumn.scale != null ) {
                builder.append( "," ).append( catalogColumn.scale );
            }
            builder.append( ")" );
        }
        builder.append( " USING " )
                .append( dialect.quoteIdentifier( columnPlacement.physicalColumnName ) )
                .append( "::" )
                .append( getTypeString( catalogColumn.type ) );
        if ( catalogColumn.collectionsType != null ) {
            builder.append( " " ).append( catalogColumn.collectionsType.toString() );
        }
        executeUpdate( builder, context );
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

        builder.append( " USING " );
        switch ( catalogIndex.method ) {
            case "btree":
            case "btree_unique":
                builder.append( "btree" );
                break;
            case "hash":
            case "hash_unique":
                builder.append( "hash" );
                break;
            case "gin":
            case "gin_unique":
                builder.append( "gin" );
                break;
            case "brin":
                builder.append( "gin" );
                break;
        }

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
                new AvailableIndexMethod( "btree", "B-TREE" ),
                new AvailableIndexMethod( "hash", "HASH" ),
                new AvailableIndexMethod( "gin", "GIN (Generalized Inverted Index)" ),
                new AvailableIndexMethod( "brin", "BRIN (Block Range index)" )
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
            connectionFactory.close();
        } catch ( SQLException e ) {
            log.warn( "Exception while shutting down {}", getUniqueName(), e );
        }
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        // TODO: Implement disconnect and reconnect to PostgreSQL instance.
    }


    @Override
    protected String getTypeString( PolyType type ) {
        if ( type.getFamily() == PolyTypeFamily.MULTIMEDIA ) {
            return "BYTEA";
        }
        switch ( type ) {
            case BOOLEAN:
                return "BOOLEAN";
            case VARBINARY:
                return "VARBINARY";
            case TINYINT:
                return "SMALLINT";
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
        }
        throw new RuntimeException( "Unknown type: " + type.name() );
    }


    @Override
    protected String getDefaultPhysicalSchemaName() {
        return "public";
    }


    private static String getConnectionUrl( final String dbHostname, final int dbPort, final String dbName ) {
        return String.format( "jdbc:postgresql://%s:%d/%s", dbHostname, dbPort, dbName );
    }

}
