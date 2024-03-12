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

package org.polypheny.db.sql.language.ddl;


import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.sql.language.SqlAlter;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER INTERFACES DROP queryInterfaceUniqueName} statement.
 */
@Slf4j
public class SqlAlterInterfacesDrop extends SqlAlter {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "ALTER INTERFACES DROP", Kind.OTHER_DDL );

    private final SqlNode uniqueName;


    public SqlAlterInterfacesDrop( ParserPos pos, SqlNode uniqueName ) {
        super( OPERATOR, pos );
        this.uniqueName = Objects.requireNonNull( uniqueName );
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( uniqueName );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( uniqueName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "INTERFACES" );
        writer.keyword( "DROP" );
        uniqueName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        String uniqueNameStr = uniqueName.toString();
        if ( uniqueNameStr.startsWith( "'" ) ) {
            uniqueNameStr = uniqueNameStr.substring( 1 );
        }
        if ( uniqueNameStr.endsWith( "'" ) ) {
            uniqueNameStr = StringUtils.chop( uniqueNameStr );
        }

        // TODO: Check if the query interface has any running transactions

        try {
            QueryInterfaceManager.getInstance().removeQueryInterface( Catalog.getInstance(), uniqueNameStr );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Could not remove query interface " + uniqueNameStr, e );
        }
    }

}

