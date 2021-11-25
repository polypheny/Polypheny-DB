/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.languages.sql.ddl;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.UnknownQueryInterfaceException;
import org.polypheny.db.core.util.CoreUtil;
import org.polypheny.db.core.enums.Kind;
import org.polypheny.db.core.nodes.Node;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.languages.sql.SqlAlter;
import org.polypheny.db.languages.sql.SqlNode;
import org.polypheny.db.languages.sql.SqlOperator;
import org.polypheny.db.languages.sql.SqlSpecialOperator;
import org.polypheny.db.languages.sql.SqlWriter;
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
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
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
        } catch ( UnknownQueryInterfaceException e ) {
            throw CoreUtil.newContextException( uniqueName.getPos(), RESOURCE.unknownQueryInterface( e.getIfaceName() ) );
        } catch ( Exception e ) {
            throw new RuntimeException( "Could not remove query interface " + uniqueNameStr, e );
        }
    }

}

