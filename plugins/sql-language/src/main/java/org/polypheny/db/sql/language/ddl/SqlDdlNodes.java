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


import java.util.List;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.partition.raw.RawPartitionInformation;
import org.polypheny.db.schema.ColumnStrategy;
import org.polypheny.db.sql.language.SqlCollation;
import org.polypheny.db.sql.language.SqlDataTypeSpec;
import org.polypheny.db.sql.language.SqlDrop;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;


/**
 * Utilities concerning {@link SqlNode} for DDL.
 */
public class SqlDdlNodes {

    private SqlDdlNodes() {
    }


    /**
     * Creates a CREATE NAMESPACE.
     */
    public static SqlCreateNamespace createNamespace( ParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, DataModel dataModel ) {
        return new SqlCreateNamespace( pos, replace, ifNotExists, name, dataModel );
    }


    /**
     * Creates a CREATE TYPE.
     */
    public static SqlCreateType createType( ParserPos pos, boolean replace, SqlIdentifier name, SqlNodeList attributeList, SqlDataTypeSpec dataTypeSpec ) {
        return new SqlCreateType( pos, replace, name, attributeList, dataTypeSpec );
    }


    /**
     * Creates a CREATE TABLE.
     */
    public static SqlCreateTable createTable( ParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, SqlNodeList columnList, SqlNode query, SqlIdentifier store, SqlIdentifier partitionType, SqlIdentifier partitionColumn, int numPartitionGroups, int numPartitions, List<SqlIdentifier> partitionNamesList, List<List<SqlNode>> partitionQualifierList, RawPartitionInformation rawPartitionInfo ) {
        return new SqlCreateTable( pos, replace, ifNotExists, name, columnList, query, store, partitionType, partitionColumn, numPartitionGroups, numPartitions, partitionNamesList, partitionQualifierList, rawPartitionInfo );
    }


    /**
     * Creates a CREATE VIEW.
     */
    public static SqlCreateView createView( ParserPos pos, boolean replace, SqlIdentifier name, SqlNodeList columnList, SqlNode query ) {
        return new SqlCreateView( pos, replace, name, columnList, query );
    }


    /**
     * Creates a CREATE MATERIALIZED VIEW.
     */
    public static SqlCreateMaterializedView createMaterializedView( ParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, SqlNodeList columnList, SqlNode query, List<SqlIdentifier> store, String freshnessType, Integer freshnessNumber, SqlIdentifier freshnessUnit ) {
        return new SqlCreateMaterializedView( pos, replace, ifNotExists, name, columnList, query, store, freshnessType, freshnessNumber, freshnessUnit );
    }


    /**
     * Creates a CREATE FUNCTION.
     */
    public static SqlCreateFunction createFunction( ParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, SqlNode className, SqlNodeList usingList ) {
        return new SqlCreateFunction( pos, replace, ifNotExists, name, className, usingList );
    }


    /**
     * Creates a DROP NAMESPACE.
     */
    public static SqlDropNamespace dropNamespace( ParserPos pos, boolean ifExists, SqlIdentifier name ) {
        return new SqlDropNamespace( pos, ifExists, name );
    }


    /**
     * Creates a DROP TYPE.
     */
    public static SqlDropType dropType( ParserPos pos, boolean ifExists, SqlIdentifier name ) {
        return new SqlDropType( pos, ifExists, name );
    }


    /**
     * Creates a DROP TABLE.
     */
    public static SqlDropTable dropTable( ParserPos pos, boolean ifExists, SqlIdentifier name ) {
        return new SqlDropTable( pos, ifExists, name );
    }


    /**
     * Creates a DROP VIEW.
     */
    public static SqlDrop dropView( ParserPos pos, boolean ifExists, SqlIdentifier name ) {
        return new SqlDropView( pos, ifExists, name );
    }


    /**
     * Creates a DROP MATERIALIZED VIEW.
     */
    public static SqlDrop dropMaterializedView( ParserPos pos, boolean ifExists, SqlIdentifier name ) {
        return new SqlDropMaterializedView( pos, ifExists, name );
    }


    /**
     * Creates a DROP FUNCTION.
     */
    public static SqlDrop dropFunction( ParserPos pos, boolean ifExists, SqlIdentifier name ) {
        return new SqlDropFunction( pos, ifExists, name );
    }


    /**
     * Creates a column declaration.
     */
    public static SqlNode column( ParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType, String collation, SqlNode expression, ColumnStrategy strategy ) {
        return new SqlColumnDeclaration( pos, name, dataType, collation, expression, strategy );
    }


    /**
     * Creates a attribute definition.
     */
    public static SqlNode attribute( ParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType, SqlNode expression, SqlCollation collation ) {
        return new SqlAttributeDefinition( pos, name, dataType, expression, collation );
    }


    /**
     * Creates a CHECK constraint.
     */
    public static SqlNode check( ParserPos pos, SqlIdentifier name, SqlNode expression ) {
        return new SqlCheckConstraint( pos, name, expression );
    }


    /**
     * Creates a UNIQUE constraint.
     */
    public static SqlKeyConstraint unique( ParserPos pos, SqlIdentifier name, SqlNodeList columnList ) {
        return new SqlKeyConstraint( pos, name, columnList );
    }


    /**
     * Creates a PRIMARY KEY constraint.
     */
    public static SqlKeyConstraint primary( ParserPos pos, SqlIdentifier name, SqlNodeList columnList ) {
        return new SqlKeyConstraint( pos, name, columnList ) {
            @Override
            public Operator getOperator() {
                return PRIMARY;
            }
        };
    }


    /**
     * Creates a PRIMARY KEY constraint.
     */
    public static SqlKeyConstraint foreign( ParserPos pos, SqlIdentifier name, SqlNodeList columnList, SqlIdentifier referencedTable, SqlIdentifier referencedColumn ) {
        return new SqlForeignKeyConstraint( pos, name, columnList, referencedTable, referencedColumn );
    }


    /**
     * File type for CREATE FUNCTION.
     */
    public enum FileType {
        FILE,
        JAR,
        ARCHIVE
    }

}
