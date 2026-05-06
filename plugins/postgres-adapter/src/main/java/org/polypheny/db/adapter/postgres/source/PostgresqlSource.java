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

package org.polypheny.db.adapter.postgres.source;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RelationalDataSource;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingList;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandler;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.adapter.jdbc.sources.AbstractJdbcSource;
import org.polypheny.db.adapter.postgres.PostgresqlSqlDialect;
import org.polypheny.db.sql.language.SqlDbFeature;
import org.polypheny.db.transaction.PUID;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;

import static org.polypheny.db.adapter.postgres.source.PostgresqlCatalogQueries.SQL_COLUMN_TYPE_MODIFIERS_AND_ATTR_DIMENSIONS;
import static org.polypheny.db.adapter.postgres.source.PostgresqlCatalogQueries.SQL_INSTALLED_EXTENSIONS;


@Slf4j
@AdapterProperties(
        name = "PostgreSQL",
        description = "Relational database system optimized for transactional workload that provides an advanced set of features. PostgreSQL is fully ACID compliant and ensures that all requirements are met.",
        usedModes = DeployMode.REMOTE,
        defaultMode = DeployMode.REMOTE)
@AdapterSettingString(name = "host", defaultValue = "localhost", position = 1,
        description = "Hostname or IP address of the remote PostgreSQL instance.")
@AdapterSettingInteger(name = "port", defaultValue = 5432, position = 2,
        description = "JDBC port number on the remote PostgreSQL instance.")
@AdapterSettingString(name = "database", defaultValue = "polypheny", position = 3,
        description = "Name of the database to connect to.")
@AdapterSettingString(name = "username", defaultValue = "polypheny", position = 4,
        description = "Username to be used for authenticating at the remote instance.")
@AdapterSettingString(name = "password", defaultValue = "polypheny", position = 5,
        description = "Password to be used for authenticating at the remote instance.")
@AdapterSettingInteger(name = "maxConnections", defaultValue = 25,
        description = "Maximum number of concurrent JDBC connections.")
@AdapterSettingList(name = "transactionIsolation", options = { "SERIALIZABLE", "READ_UNCOMMITTED", "READ_COMMITTED", "REPEATABLE_READ" }, defaultValue = "SERIALIZABLE",
        description = "Which level of transaction isolation should be used.")
@AdapterSettingString(name = "tables", defaultValue = "foo,bar",
        description = "List of tables which should be imported. The names must to be separated by a comma.")
public class PostgresqlSource extends AbstractJdbcSource {

    public PostgresqlSource( final long storeId, final String uniqueName, final Map<String, String> settings, final DeployMode mode ) {
        super(
                storeId,
                uniqueName,
                settings,
                mode,
                "org.postgresql.Driver",
                new PostgresqlSqlDialect(),
                false );
        try {
            PolyXid xid = PolyXid.generateLocalTransactionIdentifier( PUID.EMPTY_PUID, PUID.EMPTY_PUID );
            ConnectionHandler connectionHandler = connectionFactory.getOrCreateConnectionHandler( xid );
            try ( Statement statement = connectionHandler.getStatement() ) {
                Connection connection = statement.getConnection();
                Set<SqlDbFeature> features = detectFeatures( connection );
                dialect.addSupportedFeatures( features );
            }
        }  catch ( SQLException | ConnectionHandlerException e) {
            log.error( "Could not query feature information.", e );
        }
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
    protected String getConnectionUrl( final String dbHostname, final int dbPort, final String dbName ) {
        return String.format( "jdbc:postgresql://%s:%d/%s", dbHostname, dbPort, dbName );
    }


    @Override
    protected boolean requiresSchema() {
        return true;
    }


    @Override
    public RelationalDataSource asRelationalDataSource() {
        return this;
    }


    /**
     * {@inheritDoc}
     *
     * <p>Handled type names:
     * <ul>
     *   <li>{@code vector, halfvec}   - pgvector float4 and float2 vector, mapped to {@code
    ARRAY<REAL>}</li>
     *  <li>{@code bit}       - bitvectors mappte to {@code ARRAY<BOOLEAN>}</li>
     *   <li>{@code _float4}  - PostgreSQL float4 array, mapped to {@code
    ARRAY<REAL>}</li>
     *   <li>{@code _float8}  - PostgreSQL float8 array, mapped to {@code
    ARRAY<DOUBLE>}</li>
     *   <li>{@code _int4}    - PostgreSQL int4 array, mapped to {@code
    ARRAY<INTEGER>}</li>
     *   <li>{@code _int8}    - PostgreSQL int8 array, mapped to {@code
    ARRAY<BIGINT>}</li>
     * </ul>
     * <p><b>Note:</b> PostgreSQL has no enforced array size limits. We therefore only detect the specified (but not enforced) dimensions.</p>
     * @see <a href="https://www.postgresql.org/docs/current/arrays.html">PostgreSQL Arrays Documentation</a>
     */
    @Override
    protected Optional<ColumnTypeInfo> resolveNativeColumnType( Map<String, CollectionMetadata> metadata, String typeName, ResultSet columnRow ) throws SQLException {
        CollectionMetadata meta = metadata.get( columnRow.getString( "COLUMN_NAME" ).toLowerCase() );
        return switch ( typeName ) {
            case "vector", "halfvec", "sparsevec" -> Optional.of(  new ColumnTypeInfo( PolyType.REAL, PolyType.ARRAY,
                    null, null, 1,  meta != null ? meta.typeModifier() : null) );
            case "bit" -> Optional.of(  new ColumnTypeInfo( PolyType.BOOLEAN, PolyType.ARRAY,
                    null, null, 1,  meta != null ? meta.typeModifier() : null) );
            case "_float4" -> Optional.of( new ColumnTypeInfo( PolyType.REAL, PolyType.ARRAY,
                    null, null, arrayDim( meta ), null ) );
            case "_float8" -> Optional.of( new ColumnTypeInfo( PolyType.DOUBLE, PolyType.ARRAY,
                    null, null, arrayDim( meta ), null ) );
            case "_int4"   -> Optional.of( new ColumnTypeInfo( PolyType.INTEGER, PolyType.ARRAY,
                    null, null, arrayDim( meta ), null ) );
            case "_int8"   -> Optional.of( new ColumnTypeInfo( PolyType.BIGINT, PolyType.ARRAY,
                    null, null, arrayDim( meta ), null ) );
            case "_bool" -> Optional.of( new ColumnTypeInfo( PolyType.BOOLEAN, PolyType.ARRAY,
                    null, null, arrayDim( meta ), null ) );
            default        -> Optional.empty();
        };
    }


    private int arrayDim( CollectionMetadata meta ) {
        return (meta != null && meta.arrayDimensions() > 0) ? meta.arrayDimensions() : -1;
    }


    @Override
    public boolean isNativeVectorType( String typeName ) {
        return (typeName.equals( "vector" )
                || typeName.equals( "bit" ))
                || typeName.equals( "halfvec" )
                || typeName.equals( "sparsevec" );
    }


    @Override
    protected Map<String, CollectionMetadata> fetchColumnMetadata( Connection conn, String schema, String table ) throws SQLException {
        Map<String, CollectionMetadata> result = new HashMap<>();
        try ( PreparedStatement ps = conn.prepareStatement( SQL_COLUMN_TYPE_MODIFIERS_AND_ATTR_DIMENSIONS ) ) {
            ps.setString( 1, table );
            ps.setString( 2, schema );
            try ( ResultSet rs = ps.executeQuery() ) {
                while ( rs.next() ) {
                    String col      = rs.getString( "attname" );
                    int    dims     = rs.getInt( "attndims" );
                    int    rawMod   = rs.getInt( "atttypmod" );
                    Integer typeMod = rs.wasNull() ? null : rawMod;
                    result.put( col, new CollectionMetadata( dims, typeMod ) );
                    log.debug( "Column metadata: {} -> dims={}, typeMod={}", col, dims, typeMod );
                }
            }
        }
        return result;
    }


    public static Set<SqlDbFeature> detectFeatures( Connection conn ) throws SQLException {
        Set<PostgresqlFeature> found = EnumSet.noneOf( PostgresqlFeature.class );
        PreparedStatement ps = conn.prepareStatement( SQL_INSTALLED_EXTENSIONS );
        String[] featureNames = Arrays.stream( PostgresqlFeature.values() )
                .map( PostgresqlFeature::featureName )
                .toArray( String[]::new );
        ps.setArray( 1, conn.createArrayOf( "text", featureNames ) );
        ResultSet rs = ps.executeQuery();
        while ( rs.next() ) {
            String name = rs.getString( 1 );
            Arrays.stream( PostgresqlFeature.values() )
                    .filter( f -> f.featureName().equals( name ) )
                    .findFirst()
                    .ifPresent( found::add );
        }
        return Collections.unmodifiableSet( found );
    }

}
