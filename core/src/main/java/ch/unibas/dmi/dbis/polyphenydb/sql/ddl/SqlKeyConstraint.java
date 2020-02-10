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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.ddl;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSpecialOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableNullableList;
import java.util.List;
import lombok.Getter;


/**
 * Parse tree for {@code UNIQUE}, {@code PRIMARY KEY} constraints.
 *
 * And {@code FOREIGN KEY}, when we support it.
 */
public class SqlKeyConstraint extends SqlCall {

    public static final SqlSpecialOperator UNIQUE = new SqlSpecialOperator( "UNIQUE", SqlKind.UNIQUE );

    protected static final SqlSpecialOperator PRIMARY = new SqlSpecialOperator( "PRIMARY KEY", SqlKind.PRIMARY_KEY );

    @Getter
    private final SqlIdentifier name;
    @Getter
    private final SqlNodeList columnList;


    /**
     * Creates a SqlKeyConstraint.
     */
    SqlKeyConstraint( SqlParserPos pos, SqlIdentifier name, SqlNodeList columnList ) {
        super( pos );
        this.name = name;
        this.columnList = columnList;
    }


    /**
     * Creates a UNIQUE constraint.
     */
    public static SqlKeyConstraint unique( SqlParserPos pos, SqlIdentifier name, SqlNodeList columnList ) {
        return new SqlKeyConstraint( pos, name, columnList );
    }


    /**
     * Creates a PRIMARY KEY constraint.
     */
    public static SqlKeyConstraint primary( SqlParserPos pos, SqlIdentifier name, SqlNodeList columnList ) {
        return new SqlKeyConstraint( pos, name, columnList ) {
            @Override
            public SqlOperator getOperator() {
                return PRIMARY;
            }
        };
    }


    @Override
    public SqlOperator getOperator() {
        return UNIQUE;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( name, columnList );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        if ( name != null ) {
            writer.keyword( "CONSTRAINT" );
            name.unparse( writer, 0, 0 );
        }
        writer.keyword( getOperator().getName() ); // "UNIQUE" or "PRIMARY KEY"
        columnList.unparse( writer, 1, 1 );
    }
}

