/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.sql.language.ddl;


import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.sql.language.SqlCreate;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Parse tree for {@code CREATE FUNCTION} statement.
 */
public class SqlCreateFunction extends SqlCreate {

    private final SqlIdentifier name;
    private final SqlNode className;
    private final SqlNodeList usingList;

    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator( "CREATE FUNCTION", Kind.CREATE_FUNCTION );


    /**
     * Creates a SqlCreateFunction.
     */
    public SqlCreateFunction( ParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, SqlNode className, SqlNodeList usingList ) {
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
    public Operator getOperator() {
        return OPERATOR;
    }


    @Override
    public List<Node> getOperandList() {
        return Arrays.asList( name, className, usingList );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return Arrays.asList( name, className, usingList );
    }

}
