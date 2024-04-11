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

package org.polypheny.db.sql.web;


import com.google.gson.Gson;
import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.dialect.PolyphenyDbSqlDialect;
import org.polypheny.db.sql.language.pretty.SqlPrettyWriter;
import org.polypheny.db.type.PolyType;


public class SchemaToJsonMapper {

    private final static Gson gson = new Gson();


    public static String getTableNameFromJson( @NonNull String json ) {
        JsonTable table = gson.fromJson( json, JsonTable.class );
        return table.tableName;
    }


    public static String getCreateTableStatementFromJson( @NonNull String json, boolean createPrimaryKey, boolean addDefaultValueDefinition, @NonNull String schemaName, String tableName, String storeName ) {
        JsonTable table = gson.fromJson( json, JsonTable.class );
        SqlWriter writer = new SqlPrettyWriter( PolyphenyDbSqlDialect.DEFAULT, true, null );
        writer.keyword( "CREATE TABLE" );

        // Print schemaName.tableName
        SqlWriter.Frame identifierFrame = writer.startList( SqlWriter.FrameTypeEnum.IDENTIFIER );
        writer.identifier( schemaName );
        writer.sep( ".", true );
        if ( tableName != null ) {
            writer.identifier( tableName );
        } else {
            writer.identifier( table.tableName );
        }
        writer.endList( identifierFrame );

        // Print column list
        SqlWriter.Frame columnFrame = writer.startList( "(", ")" );
        for ( JsonColumn column : table.columns ) {
            writer.sep( "," );
            writer.identifier( column.columnName );
            writer.keyword( column.type );
            if ( column.length != null ) {
                SqlWriter.Frame columnTypeFrame = writer.startList( SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")" );
                writer.print( column.length );
                if ( column.scale != null ) {
                    writer.sep( ",", true );
                    writer.print( column.scale );
                }
                writer.endList( columnTypeFrame );
            }
            if ( !column.nullable ) {
                writer.keyword( "NOT NULL" );
            }
            if ( addDefaultValueDefinition ) {
                if ( column.defaultValue != null ) {
                    writer.keyword( "DEFAULT" );
                    if ( Arrays.asList( PolyType.VARCHAR.name(), PolyType.DATE.name(), PolyType.TIME.name(), PolyType.TIMESTAMP.name() ).contains( column.type ) ) {
                        writer.literal( "'" + column.defaultValue + "'" );
                    } else {
                        writer.literal( column.defaultValue );
                    }
                } else if ( column.defaultFunctionName != null ) {
                    writer.keyword( "DEFAULT" );
                    writer.print( column.defaultFunctionName );
                }
            }
        }
        if ( createPrimaryKey && table.primaryKeyColumnNames != null ) {
            writer.sep( "," );
            writer.keyword( "PRIMARY KEY" );
            SqlWriter.Frame primaryKeyFrame = writer.startList( SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")" );
            for ( String columnName : table.primaryKeyColumnNames ) {
                writer.sep( "," );
                writer.identifier( columnName );
            }
            writer.endList( primaryKeyFrame );
        }
        writer.endList( columnFrame );

        // ON STORE storeName
        if ( storeName != null ) {
            writer.keyword( "ON STORE" );
            writer.identifier( storeName );
        }

        return writer.toSqlString().getSql();
    }


    @AllArgsConstructor
    @Getter
    static class JsonTable {

        public final String tableName;
        public final List<JsonColumn> columns;
        public final List<String> primaryKeyColumnNames;

    }


    @AllArgsConstructor
    static class JsonColumn {

        public final String columnName;
        public final String type;
        public final Integer length;
        public final Integer scale;
        public final boolean nullable;
        public final String defaultValue;
        public final String defaultFunctionName;

    }

}
