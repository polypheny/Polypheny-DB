/*
 * Copyright 2019-2022 The Polypheny Project
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


import com.google.gson.Gson;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.language.SqlAlter;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER ADAPTERS ADD uniqueName USING adapterClass WITH config} statement.
 */
@Slf4j
public class SqlAlterAdaptersAdd extends SqlAlter {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "ALTER ADAPTERS ADD", Kind.OTHER_DDL );

    private final SqlNode uniqueName;
    private final SqlNode adapterName;
    private final SqlNode config;


    public SqlAlterAdaptersAdd( ParserPos pos, SqlNode uniqueName, SqlNode adapterName, SqlNode config ) {
        super( OPERATOR, pos );
        this.uniqueName = Objects.requireNonNull( uniqueName );
        this.adapterName = Objects.requireNonNull( adapterName );
        this.config = Objects.requireNonNull( config );
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( uniqueName, adapterName, config );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( uniqueName, adapterName, config );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "ADAPTERS" );
        writer.keyword( "ADD" );
        uniqueName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "USING" );
        adapterName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "WITH" );
        config.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {

        @SuppressWarnings("unchecked")
        Map<String, String> configMap = new Gson().fromJson( removeQuotationMarks( config.toString() ), Map.class );

        DdlManager.getInstance().addAdapter(
                removeQuotationMarks( uniqueName.toString() ),
                removeQuotationMarks( adapterName.toString() ),
                configMap );
    }


    private String removeQuotationMarks( String str ) {
        if ( str.startsWith( "'" ) ) {
            str = str.substring( 1 );
        }
        if ( str.endsWith( "'" ) ) {
            str = StringUtils.chop( str );
        }
        return str;
    }

}

