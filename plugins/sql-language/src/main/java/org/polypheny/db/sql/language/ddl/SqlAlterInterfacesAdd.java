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


import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
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
 * Parse tree for {@code ALTER INTERFACES ADD uniqueName USING clazzName WITH config} statement.
 */
@Slf4j
public class SqlAlterInterfacesAdd extends SqlAlter {

    public static final ObjectMapper mapper = new ObjectMapper();

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "ALTER INTERFACES ADD", Kind.OTHER_DDL );

    private final SqlNode uniqueName;
    private final SqlNode clazzName;
    private final SqlNode config;


    public SqlAlterInterfacesAdd( ParserPos pos, SqlNode uniqueName, SqlNode clazzName, SqlNode config ) {
        super( OPERATOR, pos );
        this.uniqueName = Objects.requireNonNull( uniqueName );
        this.clazzName = Objects.requireNonNull( clazzName );
        this.config = Objects.requireNonNull( config );
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( uniqueName, clazzName, config );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( uniqueName, clazzName, config );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "INTERFACES" );
        writer.keyword( "ADD" );
        uniqueName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "USING" );
        clazzName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "WÍTH" );
        config.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        String uniqueNameStr = removeQuotationMarks( uniqueName.toString() );
        String clazzNameStr = removeQuotationMarks( clazzName.toString() );
        Map<String, String> configMap = null;
        try {
            configMap = mapper.readValue( removeQuotationMarks( config.toString() ), Map.class );
            QueryInterfaceManager.getInstance().addQueryInterface( Catalog.getInstance(), clazzNameStr, uniqueNameStr, configMap );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Unable to deploy query interface", e );
        }
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

