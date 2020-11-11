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
 */

package org.polypheny.db.sql.ddl;


import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlAlter;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER INTERFACES DROP queryInterfaceUniqueName} statement.
 */
@Slf4j
public class SqlAlterInterfacesDrop extends SqlAlter {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "ALTER INTERFACES DROP", SqlKind.OTHER_DDL );

    private final SqlNode uniqueName;


    /**
     * Creates a SqlAlterSchemaOwner.
     */
    public SqlAlterInterfacesDrop( SqlParserPos pos, SqlNode uniqueName ) {
        super( OPERATOR, pos );
        this.uniqueName = Objects.requireNonNull( uniqueName );
    }


    @Override
    public List<SqlNode> getOperandList() {
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
    public void execute( Context context, Statement statement ) {
        String uniqueNameStr = uniqueName.toString();
        if ( uniqueNameStr.startsWith( "'" ) ) {
            uniqueNameStr = uniqueNameStr.substring( 1 );
        }
        if ( uniqueNameStr.endsWith( "'" ) ) {
            uniqueNameStr = StringUtils.chop( uniqueNameStr );
        }
        try {
            QueryInterfaceManager.getInstance().removeQueryInterface( Catalog.getInstance(), uniqueNameStr );
        } catch ( Exception e ) {
            throw new RuntimeException( "Could not remove query interface " + uniqueNameStr, e );
        }
    }

}

