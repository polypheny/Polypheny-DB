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

package org.polypheny.db.statistic;


import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.type.PolyType;


/**
 * Boilerplate of a column to guide the handling and pattern of a column
 */
@Slf4j
class QueryColumn {

    @Getter
    private String schema;

    @Getter
    private String table;

    @Getter
    private String name;

    @Getter
    private PolyType type;

    @Getter
    private PolyType collectionType;


    QueryColumn( String schema, String table, String name, PolyType type, PolyType collectionType ) {
        this.schema = schema.replace( "\\", "" ).replace( "\"", "" );
        this.table = table.replace( "\\", "" ).replace( "\"", "" );
        this.name = name.replace( "\\", "" ).replace( "\"", "" );
        this.type = type;
        this.collectionType = collectionType;
    }


    QueryColumn( String schemaTableName, PolyType type, PolyType collectionType ) {
        this( schemaTableName.split( "\\." )[0], schemaTableName.split( "\\." )[1], schemaTableName.split( "\\." )[2], type, collectionType );
    }


    /**
     * Builds the qualified name of the column
     *
     * @return qualifiedColumnName
     */
    public String getQualifiedColumnName() {
        return getQualifiedColumnName( schema, table, name );
    }


    public String getQualifiedTableName() {
        return getQualifiedTableName( schema, table );
    }


    /**
     * QualifiedTableName Builder
     *
     * @return fullTableName
     */
    public static String getQualifiedTableName( String schema, String table ) {
        return schema + "\".\"" + table;
    }


    public static String getQualifiedColumnName( String schema, String table, String column ) {
        return "\"" + getQualifiedTableName( schema, table ) + "\".\"" + column;
    }


    public static String[] getSplitColumn( String schemaTableColumn ) {
        return schemaTableColumn.split( "\\." );
    }


    public static QueryColumn fromCatalogColumn( CatalogColumn column ) {
        return new QueryColumn( column.getSchemaName(), column.getTableName(), column.name, column.type, column.collectionsType );
    }


}
