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

package org.polypheny.db.sql.ddl;


import org.polypheny.db.sql.SqlCreate;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlLiteral;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlNodeList;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;


/**
 * Parse tree for {@code CREATE FUNCTION} statement.
 */
public class SqlCreateFunction extends SqlCreate {

    private final SqlIdentifier name;
    private final SqlNode className;
    private final SqlNodeList usingList;

    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator( "CREATE FUNCTION", SqlKind.CREATE_FUNCTION );


    /**
     * Creates a SqlCreateFunction.
     */
    public SqlCreateFunction( SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, SqlNode className, SqlNodeList usingList ) {
        super( OPERATOR, pos, replace, ifNotExists );
        this.name = Objects.requireNonNull( name );
        this.className = className;
        this.usingList = Objects.requireNonNull( usingList );
        Preconditions.checkArgument( usingList.size() % 2 == 0 );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( getReplace() ? "CREATE OR REPLACE" : "CREATE" );
        writer.keyword( "FUNCTION" );
        if ( ifNotExists ) {
            writer.keyword( "IF NOT EXISTS" );
        }
        name.unparse( writer, 0, 0 );
        writer.keyword( "AS" );
        className.unparse( writer, 0, 0 );
        if ( usingList.size() > 0 ) {
            writer.keyword( "USING" );
            final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.SIMPLE );
            for ( Pair<SqlLiteral, SqlLiteral> using : pairs() ) {
                writer.sep( "," );
                using.left.unparse( writer, 0, 0 ); // FILE, URL or ARCHIVE
                using.right.unparse( writer, 0, 0 ); // e.g. 'file:foo/bar.jar'
            }
            writer.endList( frame );
        }
    }


    @SuppressWarnings("unchecked")
    private List<Pair<SqlLiteral, SqlLiteral>> pairs() {
        return Util.pairs( (List) usingList.getList() );
    }


    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList( name, className, usingList );
    }
}

