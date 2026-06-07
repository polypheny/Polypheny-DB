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

package org.polypheny.db.hsqldb.stores;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.json.JsonExistsErrorBehavior;
import org.polypheny.db.algebra.json.JsonValueEmptyOrErrorBehavior;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
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
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class HsqldbSqlDialectTest {

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
    void hsqldbDeclaresJsonPushdownSupport() {
        assertTrue( HsqldbSqlDialect.DEFAULT.supportsJsonFunctions() );
    }


    @Test
    void jsonValueUsesHsqldbJsonRoutine() {
        SqlNode call = jsonValueAny( jsonCommon( castToVarchar( new SqlIdentifier( "d", POS ) ), "lax $.name" ) );

        String sql = call.toSqlString( HsqldbSqlDialect.DEFAULT ).getSql();

        assertEquals( "POLYPHENY_JSON_VALUE(\"d\", 'lax $.name')", sql );
    }


    @Test
    void jsonExistsUsesHsqldbJsonRoutine() {
        SqlNode call = op(
                OperatorName.JSON_EXISTS,
                jsonCommon( new SqlIdentifier( "d", POS ), "lax $.tags[?(@ == \"urgent\")]" ),
                SqlLiteral.createSymbol( JsonExistsErrorBehavior.FALSE, POS ) );

        String sql = call.toSqlString( HsqldbSqlDialect.DEFAULT ).getSql();

        assertEquals( "POLYPHENY_JSON_EXISTS(\"d\", 'lax $.tags[?(@ == \"urgent\")]')", sql );
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
