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
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import java.sql.ResultSet;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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


    @Test
    void handleArrayRetrieval_returnsEmpty_forNonArrayType() {
        AlgDataType varcharType = typeFactory.createPolyType( PolyType.VARCHAR );
        Optional<Expression> result = dialect.handleArrayRetrieval( resultSet, 0, varcharType);
        assertTrue( result.isEmpty() );
    }


    @Test void handleArrayRetrieval_returnsEmpty_forNestedArray() {
        AlgDataType floatType = typeFactory.createPolyType( PolyType.FLOAT );
        AlgDataType floatArray = typeFactory.createArrayType( floatType, -1 );
        AlgDataType nestedArray = typeFactory.createArrayType( floatArray, -1 );
        Optional<Expression> result = dialect.handleArrayRetrieval( resultSet, 0, nestedArray );
        assertTrue( result.isEmpty() );
    }


    @Test void handleArrayRetrieval_callsParseFloat_forFloatComponent() {
        AlgDataType floatType = typeFactory.createPolyType( PolyType.FLOAT );
        AlgDataType floatArray = typeFactory.createArrayType( floatType, -1 );
        Optional<Expression> result = dialect.handleArrayRetrieval( resultSet, 0, floatArray );
        assertTrue( result.isPresent() );
        assertTrue( result.get().toString().contains( "parseVectorAsFloat" ) );
    }


    @Test void handleArrayRetrieval_callsParseDouble_forDoubleComponent() {
        AlgDataType doubleType = typeFactory.createPolyType( PolyType.DOUBLE );
        AlgDataType doubleArray = typeFactory.createArrayType( doubleType, -1 );
        Optional<Expression> result = dialect.handleArrayRetrieval( resultSet, 0, doubleArray );
        assertTrue( result.isPresent() );
        assertTrue( result.get().toString().contains( "parseVectorAsDouble" ) );
    }
}
