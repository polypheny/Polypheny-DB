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


import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.core.enums.Kind;
import org.polypheny.db.core.nodes.Node;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.core.QueryParameters;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.languages.sql.SqlAlter;
import org.polypheny.db.languages.sql.SqlNode;
import org.polypheny.db.languages.sql.SqlOperator;
import org.polypheny.db.languages.sql.SqlSpecialOperator;
import org.polypheny.db.languages.sql.SqlWriter;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER CONFIG key SET value} statement.
 */
public class SqlAlterConfig extends SqlAlter {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "ALTER CONFIG", Kind.OTHER_DDL );

    private final SqlNode key;
    private final SqlNode value;


    /**
     * Creates a SqlAlterSchemaOwner.
     */
    public SqlAlterConfig( ParserPos pos, SqlNode key, SqlNode value ) {
        super( OPERATOR, pos );
        this.key = Objects.requireNonNull( key );
        this.value = Objects.requireNonNull( value );
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( key, value );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( key, value );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "CONFIG" );
        key.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "SET" );
        value.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        String keyStr = key.toString();
        String valueStr = value.toString();
        if ( keyStr.startsWith( "'" ) ) {
            keyStr = keyStr.substring( 1 );
        }
        if ( keyStr.endsWith( "'" ) ) {
            keyStr = StringUtils.chop( keyStr );
        }
        if ( valueStr.startsWith( "'" ) ) {
            valueStr = valueStr.substring( 1 );
        }
        if ( valueStr.endsWith( "'" ) ) {
            valueStr = StringUtils.chop( valueStr );
        }
        Config config = ConfigManager.getInstance().getConfig( keyStr );
        config.parseStringAndSetValue( valueStr );
    }

}

