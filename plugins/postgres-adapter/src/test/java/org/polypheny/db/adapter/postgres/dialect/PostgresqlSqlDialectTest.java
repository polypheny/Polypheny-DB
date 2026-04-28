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

package org.polypheny.db.adapter.postgres.dialect;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.polypheny.db.adapter.postgres.source.PostgresqlFeature.PGVECTOR;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.postgres.PostgresqlSqlDialect;
import org.polypheny.db.adapter.postgres.source.PostgresqlFeature;
import org.polypheny.db.adapter.postgres.source.PostgresqlSource;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.sql.language.SqlDbFeature;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.VectorType;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

public class PostgresqlSqlDialectTest {

    @BeforeAll
    static void init() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    // ---- detectFeatures ----------------------------------------------------------------

    @Test
    void returnsEmptyWhenNoExtensionInstalled() throws SQLException {
        Connection conn = mockConnection( false );
        assertTrue( PostgresqlSource.detectFeatures( conn ).isEmpty() );
    }


    @Test
    void detectsPgVectorExtensionWhenPresent() throws SQLException {
        Connection conn = mockConnectionWithExtensions( "vector" );
        Set<SqlDbFeature> features = PostgresqlSource.detectFeatures( conn );
        assertTrue( features.contains( PGVECTOR ) );
        assertFalse( features.contains( PostgresqlFeature.POSTGIS ) );
    }


    @Test
    void detectsPostgisExtensionWhenPresent() throws SQLException {
        Connection conn = mockConnectionWithExtensions( "postgis" );
        Set<SqlDbFeature> features = PostgresqlSource.detectFeatures( conn );
        assertTrue( features.contains( PostgresqlFeature.POSTGIS ) );
        assertFalse( features.contains( PGVECTOR ) );
    }


    @Test
    void detectsAllFeaturesWhenBothPresent() throws SQLException {
        Connection conn = mockConnectionWithExtensions( "postgis", "vector" );
        Set<SqlDbFeature> features = PostgresqlSource.detectFeatures( conn );
        assertTrue( features.contains( PostgresqlFeature.POSTGIS ) );
        assertTrue( features.contains( PGVECTOR ) );
    }


    @Test
    void detectedFeaturesSetIsImmutable() throws SQLException {
        Set<SqlDbFeature> features = PostgresqlSource.detectFeatures( mockConnection( false ) );
        assertThrows( UnsupportedOperationException.class, () -> features.add( PGVECTOR ) );
    }


    @Test
    void dialectReflectsDetectedFeatures() throws SQLException {
        Connection conn = mockConnectionWithExtensions( "vector" );
        PostgresqlSqlDialect d = new PostgresqlSqlDialect();
        d.addSupportedFeatures( PostgresqlSource.detectFeatures( conn ) );
        assertTrue( d.supportsVector() );
    }


    // ---- getCustomArrayRetrievalExpression ----------------------------------------------------------------

    @Test
    void bitVectorAlwaysUsesGetString() {
        // bit(n) is a native PostgreSQL type — no pgvector required
        PostgresqlSqlDialect dialect = new PostgresqlSqlDialect();
        AlgDataType bitVec = bitVectorType( 3 );
        ParameterExpression rs = Expressions.parameter( ResultSet.class, "rs" );

        Optional<Expression> expr = dialect.getCustomArrayRetrievalExpression( rs, 0, bitVec );

        assertTrue( expr.isPresent() );
        assertTrue( expr.get().toString().contains( "getString" ) );
        assertFalse( expr.get().toString().contains( "getObject" ) );
    }


    @Test
    void floatVectorReturnsEmptyWithoutPgvector() {
        PostgresqlSqlDialect dialect = new PostgresqlSqlDialect();
        ParameterExpression rs = Expressions.parameter( ResultSet.class, "rs" );

        Optional<Expression> expr = dialect.getCustomArrayRetrievalExpression( rs, 0, floatVectorType( 3 ) );

        assertTrue( expr.isEmpty() );
    }


    @Test
    void floatVectorUsesGetObjectWithPgvector() {
        PostgresqlSqlDialect dialect = new PostgresqlSqlDialect();
        dialect.addSupportedFeatures( Set.of( PGVECTOR ) );
        ParameterExpression rs = Expressions.parameter( ResultSet.class, "rs" );

        Optional<Expression> expr = dialect.getCustomArrayRetrievalExpression( rs, 0, floatVectorType( 3 ) );

        assertTrue( expr.isPresent() );
        assertTrue( expr.get().toString().contains( "getObject" ) );
        assertFalse( expr.get().toString().contains( "getString" ) );
    }


    @Test
    void nonArrayTypeReturnsEmpty() {
        PostgresqlSqlDialect dialect = new PostgresqlSqlDialect();
        dialect.addSupportedFeatures( Set.of( PGVECTOR ) );
        AlgDataType intType = AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.INTEGER );
        ParameterExpression rs = Expressions.parameter( ResultSet.class, "rs" );

        assertTrue( dialect.getCustomArrayRetrievalExpression( rs, 0, intType ).isEmpty() );
    }


    // ---- vectorPushdownTypeIsPresent ----------------------------------------------------------------

    @Test
    void bitPushdownAlwaysPresent() {
        // bit(n) is native PostgreSQL — pushdown does not depend on pgvector
        PostgresqlSqlDialect dialect = new PostgresqlSqlDialect();
        assertTrue( dialect.vectorPushdownTypeIsPresent( VectorType.ElementType.BIT ) );
    }


    @Test
    void floatPushdownRequiresPgvector() {
        PostgresqlSqlDialect dialect = new PostgresqlSqlDialect();
        assertFalse( dialect.vectorPushdownTypeIsPresent( VectorType.ElementType.FLOAT ) );

        dialect.addSupportedFeatures( Set.of( PGVECTOR ) );
        assertTrue( dialect.vectorPushdownTypeIsPresent( VectorType.ElementType.FLOAT ) );
    }


    // ---- helpers ----------------------------------------------------------------

    private static AlgDataType bitVectorType( int dim ) {
        return AlgDataTypeFactory.DEFAULT.createVectorType(
                AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.BOOLEAN ), dim );
    }


    private static AlgDataType floatVectorType( int dim ) {
        return AlgDataTypeFactory.DEFAULT.createVectorType(
                AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.REAL ), dim );
    }


    private static Connection mockConnection( boolean hasRows ) throws SQLException {
        Connection conn = mock( Connection.class );
        PreparedStatement ps = mock( PreparedStatement.class );
        ResultSet rs = mock( ResultSet.class );
        Array arr = mock( Array.class );
        when( conn.prepareStatement( any() ) ).thenReturn( ps );
        when( conn.createArrayOf( eq( "text" ), any() ) ).thenReturn( arr );
        when( ps.executeQuery() ).thenReturn( rs );
        when( rs.next() ).thenReturn( hasRows, false );
        return conn;
    }


    private static Connection mockConnectionWithExtensions( String... extensions ) throws SQLException {
        Connection conn = mock( Connection.class );
        PreparedStatement ps = mock( PreparedStatement.class );
        ResultSet rs = mock( ResultSet.class );
        Array arr = mock( Array.class );
        when( conn.prepareStatement( any() ) ).thenReturn( ps );
        when( conn.createArrayOf( eq( "text" ), any() ) ).thenReturn( arr );
        when( ps.executeQuery() ).thenReturn( rs );
        Boolean[] hasNext = new Boolean[extensions.length + 1];
        for ( int i = 0; i < extensions.length; i++ ) hasNext[i] = true;
        hasNext[extensions.length] = false;
        when( rs.next() ).thenReturn( hasNext[0], java.util.Arrays.copyOfRange( hasNext, 1, hasNext.length ) );
        String[] rest = java.util.Arrays.copyOfRange( extensions, 1, extensions.length );
        when( rs.getString( 1 ) ).thenReturn( extensions[0], rest );
        return conn;
    }

}
