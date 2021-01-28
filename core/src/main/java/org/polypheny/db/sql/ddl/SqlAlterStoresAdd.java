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


import com.google.gson.Gson;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.catalog.Catalog;
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
 * Parse tree for {@code ALTER STORES ADD storeName USING adapterName WITH config} statement.
 */
@Slf4j
public class SqlAlterStoresAdd extends SqlAlter {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "ALTER STORES ADD", SqlKind.OTHER_DDL );

    private final SqlNode storeName;
    private final SqlNode adapterName;
    private final SqlNode config;


    /**
     * Creates a SqlAlterSchemaOwner.
     */
    public SqlAlterStoresAdd( SqlParserPos pos, SqlNode storeName, SqlNode adapterName, SqlNode config ) {
        super( OPERATOR, pos );
        this.storeName = Objects.requireNonNull( storeName );
        this.adapterName = Objects.requireNonNull( adapterName );
        this.config = Objects.requireNonNull( config );
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( storeName, adapterName, config );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "STORES" );
        writer.keyword( "ADD" );
        storeName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "USING" );
        adapterName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "WÍTH" );
        config.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        String storeNameStr = removeQuotationMarks( storeName.toString() );
        String adapterNameStr = removeQuotationMarks( adapterName.toString() );
        Map<String, String> configMap = new Gson().fromJson( removeQuotationMarks( config.toString() ), Map.class );
        try {
            AdapterManager.getInstance().addStore( Catalog.getInstance(), adapterNameStr, storeNameStr, configMap );
        } catch ( Exception e ) {
            throw new RuntimeException( "Could not deploy store", e );
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

