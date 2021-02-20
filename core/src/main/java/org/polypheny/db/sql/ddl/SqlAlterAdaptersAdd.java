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

package org.polypheny.db.sql.ddl;


import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DataSource.ExportedColumn;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
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
 * Parse tree for {@code ALTER ADAPTERS ADD uniqueName USING adapterClass WITH config} statement.
 */
@Slf4j
public class SqlAlterAdaptersAdd extends SqlAlter {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "ALTER ADAPTERS ADD", SqlKind.OTHER_DDL );

    private final SqlNode uniqueName;
    private final SqlNode adapterName;
    private final SqlNode config;


    /**
     * Creates a SqlAlterSchemaOwner.
     */
    public SqlAlterAdaptersAdd( SqlParserPos pos, SqlNode uniqueName, SqlNode adapterName, SqlNode config ) {
        super( OPERATOR, pos );
        this.uniqueName = Objects.requireNonNull( uniqueName );
        this.adapterName = Objects.requireNonNull( adapterName );
        this.config = Objects.requireNonNull( config );
    }


    @Override
    public List<SqlNode> getOperandList() {
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
        writer.keyword( "WÍTH" );
        config.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        String storeNameStr = removeQuotationMarks( uniqueName.toString() );
        String adapterNameStr = removeQuotationMarks( adapterName.toString() );
        Map<String, String> configMap = new Gson().fromJson( removeQuotationMarks( config.toString() ), Map.class );

        Adapter adapter = AdapterManager.getInstance().addAdapter( adapterNameStr, storeNameStr, configMap );
        if ( adapter instanceof DataSource ) {
            Catalog catalog = Catalog.getInstance();
            Map<String, List<ExportedColumn>> exportedColumns;
            try {
                exportedColumns = ((DataSource) adapter).getExportedColumns();
            } catch ( Exception e ) {
                AdapterManager.getInstance().removeAdapter( adapter.getAdapterId() );
                throw new RuntimeException( "Could not deploy adapter", e );
            }
            // Create table, columns etc.
            for ( Map.Entry<String, List<ExportedColumn>> entry : exportedColumns.entrySet() ) {
                // Make sure the table name is unique
                String tableName = entry.getKey();
                if ( catalog.checkIfExistsTable( 1, tableName ) ) {
                    int i = 0;
                    while ( catalog.checkIfExistsTable( 1, tableName + i ) ) {
                        i++;
                    }
                    tableName += i;
                }

                long tableId = catalog.addTable( tableName, 1, 1, TableType.SOURCE, !((DataSource) adapter).isDataReadOnly(), null );
                List<Long> primaryKeyColIds = new ArrayList<>();
                int colPos = 1;
                for ( ExportedColumn exportedColumn : entry.getValue() ) {
                    long columnId = catalog.addColumn(
                            exportedColumn.name,
                            tableId,
                            colPos++,
                            exportedColumn.type,
                            exportedColumn.collectionsType,
                            exportedColumn.length,
                            exportedColumn.scale,
                            exportedColumn.dimension,
                            exportedColumn.cardinality,
                            exportedColumn.nullable,
                            Collation.CASE_INSENSITIVE );
                    catalog.addColumnPlacement(
                            adapter.getAdapterId(),
                            columnId,
                            PlacementType.STATIC,
                            exportedColumn.physicalSchemaName,
                            exportedColumn.physicalTableName,
                            exportedColumn.physicalColumnName );
                    catalog.updateColumnPlacementPhysicalPosition( adapter.getAdapterId(), columnId, exportedColumn.physicalPosition );
                    if ( exportedColumn.primary ) {
                        primaryKeyColIds.add( columnId );
                    }
                }
                try {
                    catalog.addPrimaryKey( tableId, primaryKeyColIds );
                } catch ( GenericCatalogException e ) {
                    throw new RuntimeException( "Exception while adding primary key" );
                }
            }
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

