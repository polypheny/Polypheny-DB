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

package ch.unibas.dmi.dbis.polyphenydb.webui;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogKey;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.PolyphenyDbSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.pretty.SqlPrettyWriter;
import com.google.gson.Gson;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;


public class SchemaToJsonMapper {

    private final static Gson gson = new Gson();


    public static String exportTableDefinitionAsJson( @NonNull CatalogCombinedTable combinedTable, boolean exportPrimaryKey, boolean exportDefaultValues ) {
        List<JsonColumn> columns = new LinkedList<>();
        for ( CatalogColumn catalogColumn : combinedTable.getColumns() ) {
            String defaultValue = null;
            String defaultFunctionName = null;
            if ( exportDefaultValues ) {
                if ( catalogColumn.defaultValue != null ) {
                    defaultValue = catalogColumn.defaultValue.value;
                    defaultFunctionName = catalogColumn.defaultValue.functionName;
                }
            }
            columns.add( new JsonColumn(
                    catalogColumn.name,
                    catalogColumn.type.name(),
                    catalogColumn.length,
                    catalogColumn.scale,
                    catalogColumn.nullable,
                    defaultValue,
                    defaultFunctionName ) );
        }
        List<String> primaryKeyColumnNames = null;
        if ( exportPrimaryKey ) {
            for ( CatalogKey catalogKey : combinedTable.getKeys() ) {
                if ( catalogKey.id == combinedTable.getTable().primaryKey ) {
                    primaryKeyColumnNames = catalogKey.columnNames;
                    break;
                }
            }
        }
        JsonTable table = new JsonTable( combinedTable.getTable().name, columns, primaryKeyColumnNames );
        return gson.toJson( table );
    }


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
                    if ( Arrays.asList( PolySqlType.VARCHAR.name(), PolySqlType.TEXT.name(), PolySqlType.DATE.name(), PolySqlType.TIME.name(), PolySqlType.TIMESTAMP.name() ).contains( column.type ) ) {
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
