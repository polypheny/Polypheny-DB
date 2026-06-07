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


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.json.JsonExistsErrorBehavior;
import org.polypheny.db.algebra.json.JsonValueEmptyOrErrorBehavior;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.language.SqlDataTypeSpec;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.fun.SqlCastFunction;
import org.polypheny.db.sql.language.fun.SqlJsonApiCommonSyntaxOperator;
import org.polypheny.db.sql.language.fun.SqlJsonExistsFunction;
import org.polypheny.db.sql.language.fun.SqlJsonValueExpressionOperator;
import org.polypheny.db.sql.language.fun.SqlJsonValueFunction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class PostgresqlSqlDialectTest {

    private static final ParserPos POS = ParserPos.ZERO;


    @BeforeAll
    static void registerOperators() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
        OperatorRegistry.register( OperatorName.CAST, new SqlCastFunction() );
        OperatorRegistry.register( OperatorName.JSON_VALUE_EXPRESSION, new SqlJsonValueExpressionOperator( "JSON_VALUE_EXPRESSION", false ) );
        OperatorRegistry.register( OperatorName.JSON_API_COMMON_SYNTAX, new SqlJsonApiCommonSyntaxOperator() );
        OperatorRegistry.register( OperatorName.JSON_VALUE_ANY, new SqlJsonValueFunction( "JSON_VALUE_ANY", true ) );
        OperatorRegistry.register( OperatorName.JSON_EXISTS, new SqlJsonExistsFunction() );
    }


    @Test
    void documentCastSpecUsesJsonb() {
        PolyTypeFactoryImpl typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        AlgDataType documentType = typeFactory.createPolyType( PolyType.DOCUMENT );

        String sql = PostgresqlSqlDialect.DEFAULT.getCastSpec( documentType ).toSqlString( PostgresqlSqlDialect.DEFAULT ).getSql();

        assertEquals( "JSONB", sql );
    }


    @Test
    void documentParametersAreBoundAsNativeJson() throws Exception {
        AtomicInteger index = new AtomicInteger();
        AtomicReference<Object> value = new AtomicReference<>();
        AtomicInteger type = new AtomicInteger();
        PreparedStatement preparedStatement = (PreparedStatement) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class[]{ PreparedStatement.class },
                ( proxy, method, args ) -> {
                    if ( method.getName().equals( "setObject" ) && args.length == 3 ) {
                        index.set( (Integer) args[0] );
                        value.set( args[1] );
                        type.set( (Integer) args[2] );
                        return null;
                    }
                    throw new AssertionError( "Unexpected PreparedStatement call: " + method );
                } );

        PostgresqlSqlDialect.DEFAULT.setDocumentDynamicParam( preparedStatement, 7, PolyDocument.EMPTY_DOCUMENT );

        assertEquals( 7, index.get() );
        assertEquals( "{}", value.get() );
        assertEquals( Types.OTHER, type.get() );
    }


    @Test
    void jsonValueUsesJsonbPathQueryFirst() {
        SqlNode call = jsonValueAny( jsonCommon( castToVarchar( new SqlIdentifier( "d", POS ) ), "lax $.patient_id" ) );

        String sql = call.toSqlString( PostgresqlSqlDialect.DEFAULT ).getSql();

        assertEquals(
                "(CASE WHEN jsonb_typeof(jsonb_path_query_first((\"d\" )::jsonb, 'lax $.patient_id' , '{}'::jsonb, true)) IN ('object', 'array') THEN NULL ELSE jsonb_path_query_first((\"d\" )::jsonb, 'lax $.patient_id' , '{}'::jsonb, true) #>> '{}' END)",
                sql );
    }


    @Test
    void jsonExistsUsesJsonbPathExists() {
        SqlNode call = op(
                OperatorName.JSON_EXISTS,
                jsonCommon( new SqlIdentifier( "d", POS ), "lax $.tags[*] ? (@ == \"urgent\")" ),
                SqlLiteral.createSymbol( JsonExistsErrorBehavior.FALSE, POS ) );

        String sql = call.toSqlString( PostgresqlSqlDialect.DEFAULT ).getSql();

        assertEquals(
                "jsonb_path_exists((\"d\" )::jsonb, 'lax $.tags[*] ? (@ == \"urgent\")' , '{}'::jsonb, true)",
                sql );
    }


    @Test
    void postgresDeclaresJsonPushdownOnlyForSupportedFunctions() {
        assertTrue( PostgresqlSqlDialect.DEFAULT.supportsJsonFunctions() );
    }


    @Test
    void postgresJsonPushdownEligibilityIsConservative() {
        PolyTypeFactoryImpl typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        RexBuilder rexBuilder = new RexBuilder( typeFactory );
        AlgDataType anyType = typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.ANY ), true );
        RexNode document = rexBuilder.makeInputRef( typeFactory.createPolyType( PolyType.DOCUMENT ), 0 );
        RexNode commonSyntax = rexBuilder.makeCall(
                anyType,
                OperatorRegistry.get( OperatorName.JSON_API_COMMON_SYNTAX ),
                List.of(
                        rexBuilder.makeCall( anyType, OperatorRegistry.get( OperatorName.JSON_VALUE_EXPRESSION ), List.of( document ) ),
                        rexBuilder.makeLiteral( "lax $.a" ) ) );
        RexCall defaultJsonValue = (RexCall) rexBuilder.makeCall(
                anyType,
                OperatorRegistry.get( OperatorName.JSON_VALUE_ANY ),
                List.of(
                        commonSyntax,
                        rexBuilder.makeFlag( JsonValueEmptyOrErrorBehavior.NULL ),
                        rexBuilder.constantNull(),
                        rexBuilder.makeFlag( JsonValueEmptyOrErrorBehavior.NULL ),
                        rexBuilder.constantNull() ) );
        RexCall errorOnEmptyJsonValue = (RexCall) rexBuilder.makeCall(
                anyType,
                OperatorRegistry.get( OperatorName.JSON_VALUE_ANY ),
                List.of(
                        commonSyntax,
                        rexBuilder.makeFlag( JsonValueEmptyOrErrorBehavior.ERROR ),
                        rexBuilder.constantNull(),
                        rexBuilder.makeFlag( JsonValueEmptyOrErrorBehavior.NULL ),
                        rexBuilder.constantNull() ) );

        assertTrue( PostgresqlSqlDialect.DEFAULT.supportsJsonFunction( defaultJsonValue ) );
        assertFalse( PostgresqlSqlDialect.DEFAULT.supportsJsonFunction( errorOnEmptyJsonValue ) );
    }


    private SqlNode jsonValueAny( SqlNode commonSyntax ) {
        return op(
                OperatorName.JSON_VALUE_ANY,
                commonSyntax,
                SqlLiteral.createSymbol( JsonValueEmptyOrErrorBehavior.NULL, POS ),
                SqlLiteral.createNull( POS ),
                SqlLiteral.createSymbol( JsonValueEmptyOrErrorBehavior.NULL, POS ),
                SqlLiteral.createNull( POS ) );
    }


    private SqlNode jsonCommon( SqlNode input, String path ) {
        return op(
                OperatorName.JSON_API_COMMON_SYNTAX,
                op( OperatorName.JSON_VALUE_EXPRESSION, input ),
                SqlLiteral.createCharString( path, POS ) );
    }


    private SqlNode castToVarchar( SqlNode node ) {
        return op(
                OperatorName.CAST,
                node,
                new SqlDataTypeSpec( new SqlIdentifier( "VARCHAR", POS ), 2050, -1, null, null, POS ) );
    }


    private SqlNode op( OperatorName operatorName, Node... operands ) {
        return (SqlNode) OperatorRegistry.get( operatorName ).createCall( POS, operands );
    }

}
