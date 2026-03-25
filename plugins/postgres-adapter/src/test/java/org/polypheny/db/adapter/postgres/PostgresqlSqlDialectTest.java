/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.postgres;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.polypheny.db.adapter.postgres.source.PostgresqlFeature;
import org.polypheny.db.adapter.postgres.source.PostgresqlSource;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.sql.language.SqlDbFeature;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;


import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.polypheny.db.adapter.postgres.source.PostgresqlFeature.PGVECTOR;

public class PostgresqlSqlDialectTest {

    private final PolyTypeFactoryImpl typeFactory = new PolyTypeFactoryImpl(
            AlgDataTypeSystem.DEFAULT );
    private final PostgresqlSqlDialect dialect = (PostgresqlSqlDialect)
            PostgresqlSqlDialect.DEFAULT;
    private final ParameterExpression resultSet = Expressions.parameter(
            ResultSet.class, "rs" );

    @BeforeAll
    static void init() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }

    /*
    ---------------------- Tests for handleArrayRetrieval method ----------------------
     */
    @Test
    void returnsEmptyforNonArrayType() {
        AlgDataType varcharType = typeFactory.createPolyType( PolyType.VARCHAR );
        Optional<Expression> result = dialect.handleArrayRetrieval( resultSet, 0, varcharType);
        assertTrue( result.isEmpty() );
    }


    @Test void returnsEmptyforNestedArray() {
        AlgDataType floatType = typeFactory.createPolyType( PolyType.FLOAT );
        AlgDataType floatArray = typeFactory.createArrayType( floatType, -1 );
        AlgDataType nestedArray = typeFactory.createArrayType( floatArray, -1 );
        Optional<Expression> result = dialect.handleArrayRetrieval( resultSet, 0, nestedArray );
        assertTrue( result.isEmpty() );
    }


    @Test void callsParseFloatforFloatComponent() {
        AlgDataType floatType = typeFactory.createPolyType( PolyType.FLOAT );
        AlgDataType floatArray = typeFactory.createArrayType( floatType, -1 );
        dialect.addSupportedFeatures( Set.of( PGVECTOR ) );
        Optional<Expression> result = dialect.handleArrayRetrieval( resultSet, 0, floatArray );
        assertTrue( result.isPresent() );
        assertTrue( result.get().toString().contains( "parseVectorAsFloat" ) );
    }


    @Test void callsParseDoubleforDoubleComponent() {
        AlgDataType doubleType = typeFactory.createPolyType( PolyType.DOUBLE );
        AlgDataType doubleArray = typeFactory.createArrayType( doubleType, -1 );
        dialect.addSupportedFeatures( Set.of( PGVECTOR ) );
        Optional<Expression> result = dialect.handleArrayRetrieval( resultSet, 0, doubleArray );
        assertTrue( result.isPresent() );
        assertTrue( result.get().toString().contains( "parseVectorAsDouble" ) );
    }


    /*
    ---------------------- Tests detectFeatures ----------------------
     */
    @Test
    void returnsEmptyWhenNoExtensionInstalled() throws SQLException {
        Connection conn = mock( Connection.class );
        PreparedStatement ps = mock( PreparedStatement.class );
        ResultSet rs = mock( ResultSet.class );
        Array arr = mock( Array.class );

        when( conn.prepareStatement( any() ) ).thenReturn( ps );
        when( conn.createArrayOf( eq("text"), any() ) ).thenReturn( arr );
        when( ps.executeQuery() ).thenReturn( rs );
        when( rs.next() ).thenReturn( false );

        Set<SqlDbFeature> features = PostgresqlSource.detectFeatures( conn );
        assertTrue( features.isEmpty() );
    }


    @Test
    void detectsPgVectorExtensionWhenPresent() throws SQLException {
        Connection conn = mock( Connection.class );
        PreparedStatement ps = mock( PreparedStatement.class );
        ResultSet rs = mock( ResultSet.class );
        Array arr = mock( Array.class );

        when( conn.prepareStatement( any() ) ).thenReturn( ps );
        when( conn.createArrayOf( eq("text"), any() ) ).thenReturn( arr );
        when( ps.executeQuery() ).thenReturn( rs );
        when( rs.next() ).thenReturn( true, false );
        when( rs.getString( 1 ) ).thenReturn( "vector" );

        Set<SqlDbFeature> features = PostgresqlSource.detectFeatures( conn );
        assertTrue( features.contains( PGVECTOR ) );
        assertFalse( features.contains( PostgresqlFeature.POSTGIS ) );
    }


    @Test
    void detectsPostgisExtensionWhenPresent() throws SQLException {
        Connection conn = mock( Connection.class );
        PreparedStatement ps = mock( PreparedStatement.class );
        ResultSet rs = mock( ResultSet.class );
        Array arr = mock( Array.class );

        when( conn.prepareStatement( any() ) ).thenReturn( ps );
        when( conn.createArrayOf( eq("text"), any() ) ).thenReturn( arr );
        when( ps.executeQuery() ).thenReturn( rs );
        when( rs.next() ).thenReturn( true, false );
        when( rs.getString( 1 ) ).thenReturn( "postgis" );

        Set<SqlDbFeature> features = PostgresqlSource.detectFeatures( conn );
        assertTrue( features.contains( PostgresqlFeature.POSTGIS ) );
        assertFalse( features.contains( PGVECTOR ) );
    }


    @Test
    void detectsAllFeaturesWhenPresent() throws SQLException {
        Connection conn = mock( Connection.class );
        PreparedStatement ps = mock( PreparedStatement.class );
        ResultSet rs = mock( ResultSet.class );
        Array arr = mock( Array.class );

        when( conn.prepareStatement( any() ) ).thenReturn( ps );
        when( conn.createArrayOf( eq("text"), any() ) ).thenReturn( arr );
        when( ps.executeQuery() ).thenReturn( rs );
        when( rs.next() ).thenReturn( true, true,false );
        when( rs.getString( 1 ) ).thenReturn( "postgis", "vector" );

        Set<SqlDbFeature> features = PostgresqlSource.detectFeatures( conn );
        assertTrue( features.contains( PostgresqlFeature.POSTGIS ) );
        assertTrue( features.contains( PGVECTOR ) );
    }


    @Test
    void resultsImmutable() throws SQLException {
        Connection conn = mock( Connection.class );
        PreparedStatement ps = mock( PreparedStatement.class );
        ResultSet rs = mock( ResultSet.class );
        Array arr = mock( Array.class );

        when( conn.prepareStatement( any() ) ).thenReturn( ps );
        when( conn.createArrayOf( eq("text"), any() ) ).thenReturn( arr );
        when( ps.executeQuery() ).thenReturn( rs );
        when( rs.next() ).thenReturn( false );

        Set<SqlDbFeature> features = PostgresqlSource.detectFeatures( conn );
        assertThrows( UnsupportedOperationException.class,
                () -> features.add( PGVECTOR ) );
    }


    @Test
    void dialectSupportsFeatureAfterAdded() throws SQLException {
        Connection conn = mock( Connection.class );
        PreparedStatement ps = mock( PreparedStatement.class );
        ResultSet rs = mock( ResultSet.class );
        Array arr = mock( Array.class );

        when( conn.prepareStatement( any() ) ).thenReturn( ps );
        when( conn.createArrayOf( eq("text"), any() ) ).thenReturn( arr );
        when( ps.executeQuery() ).thenReturn( rs );
        when( rs.next() ).thenReturn( true, false );
        when( rs.getString( 1 ) ).thenReturn( "vector" );

        Set<SqlDbFeature> features = PostgresqlSource.detectFeatures( conn );
        PostgresqlSqlDialect d = new PostgresqlSqlDialect();
        d.addSupportedFeatures( features );
        assertTrue( d.supportsVector() );
    }


}
