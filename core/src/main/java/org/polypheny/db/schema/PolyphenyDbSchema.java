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

package org.polypheny.db.schema;


import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.rel.type.RelProtoDataType;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.polypheny.db.util.BuiltInMethod;


public interface PolyphenyDbSchema {

    static PolyphenyDbSchema from( SchemaPlus plus ) {
        return plus.polyphenyDbSchema();
    }

    void setCache( boolean cache );

    TableEntry add( String tableName, Table table );

    TableEntry add( String tableName, Table table, ImmutableList<String> sqls );

    TypeEntry add( String name, RelProtoDataType type );

    PolyphenyDbSchema root();

    boolean isRoot();

    List<String> path( String name );

    PolyphenyDbSchema getSubSchema( String schemaName, boolean caseSensitive );

    /**
     * Adds a child schema of this schema.
     */
    PolyphenyDbSchema add( String name, Schema schema, SchemaType type );

    TableEntry getTable( String tableName, boolean caseSensitive );

    String getName();

    PolyphenyDbSchema getParent();

    Schema getSchema();

    void setSchema( Schema schema );

    SchemaPlus plus();

    List<? extends List<String>> getPath();

    NavigableMap<String, PolyphenyDbSchema> getSubSchemaMap();

    NavigableSet<String> getTableNames();

    NavigableSet<String> getTypeNames();

    TypeEntry getType( String name, boolean caseSensitive );

    Collection<Function> getFunctions( String name, boolean caseSensitive );

    NavigableSet<String> getFunctionNames();

    NavigableMap<String, Table> getTablesBasedOnNullaryFunctions();

    TableEntry getTableBasedOnNullaryFunction( String tableName, boolean caseSensitive );

    @Experimental
    boolean removeSubSchema( String name );

    @Experimental
    boolean removeTable( String name );

    @Experimental
    boolean removeFunction( String name );

    @Experimental
    boolean removeType( String name );



    /**
     * Entry in a schema, such as a table or sub-schema.
     *
     * Each object's name is a property of its membership in a schema; therefore in principle it could belong to several schemas, or even the same schema several times, with different names. In this
     * respect, it is like an inode in a Unix file system.
     *
     * The members of a schema must have unique names.
     */
    abstract class Entry {

        public final PolyphenyDbSchema schema;
        public final String name;


        public Entry( PolyphenyDbSchema schema, String name ) {
            this.schema = Objects.requireNonNull( schema );
            this.name = Objects.requireNonNull( name );
        }


        /**
         * Returns this object's path. For example ["hr", "emps"].
         */
        public final List<String> path() {
            return schema.path( name );
        }
    }


    /**
     * Membership of a table in a schema.
     */
    abstract class TableEntry extends Entry {

        public final ImmutableList<String> sqls;


        public TableEntry( PolyphenyDbSchema schema, String name, ImmutableList<String> sqls ) {
            super( schema, name );
            this.sqls = Objects.requireNonNull( sqls );
        }


        public abstract Table getTable();
    }


    /**
     * Membership of a type in a schema.
     */
    abstract class TypeEntry extends Entry {

        public TypeEntry( PolyphenyDbSchema schema, String name ) {
            super( schema, name );
        }


        public abstract RelProtoDataType getType();
    }


    /**
     * Membership of a function in a schema.
     */
    abstract class FunctionEntry extends Entry {

        public FunctionEntry( PolyphenyDbSchema schema, String name ) {
            super( schema, name );
        }


        public abstract Function getFunction();

    }


    /**
     * Implementation of {@link PolyphenyDbSchema.TableEntry} where all properties are held in fields.
     */
    class TableEntryImpl extends TableEntry {

        private final Table table;


        /**
         * Creates a TableEntryImpl.
         */
        public TableEntryImpl( PolyphenyDbSchema schema, String name, Table table, ImmutableList<String> sqls ) {
            super( schema, name, sqls );
            this.table = Objects.requireNonNull( table );
        }


        @Override
        public Table getTable() {
            return table;
        }
    }


    /**
     * Implementation of {@link TypeEntry} where all properties are held in fields.
     */
    class TypeEntryImpl extends TypeEntry {

        private final RelProtoDataType protoDataType;


        /**
         * Creates a TypeEntryImpl.
         */
        public TypeEntryImpl( PolyphenyDbSchema schema, String name, RelProtoDataType protoDataType ) {
            super( schema, name );
            this.protoDataType = protoDataType;
        }


        @Override
        public RelProtoDataType getType() {
            return protoDataType;
        }
    }


    /**
     * Implementation of {@link FunctionEntry} where all properties are held in fields.
     */
    class FunctionEntryImpl extends FunctionEntry {

        private final Function function;


        /**
         * Creates a FunctionEntryImpl.
         */
        public FunctionEntryImpl( PolyphenyDbSchema schema, String name, Function function ) {
            super( schema, name );
            this.function = function;
        }


        @Override
        public Function getFunction() {
            return function;
        }

    }


    /**
     * Schema that has no parents.
     */
    class RootSchema extends AbstractSchema {

        RootSchema() {
            super();
        }


        @Override
        public Expression getExpression( SchemaPlus parentSchema, String name ) {
            return Expressions.call( DataContext.ROOT, BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method );
        }
    }
}
