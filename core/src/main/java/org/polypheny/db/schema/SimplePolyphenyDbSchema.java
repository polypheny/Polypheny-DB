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
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import java.util.Collection;
import java.util.List;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.rel.type.RelProtoDataType;
import org.polypheny.db.util.NameMap;
import org.polypheny.db.util.NameMultimap;
import org.polypheny.db.util.NameSet;


/**
 * A concrete implementation of {@link AbstractPolyphenyDbSchema} that maintains minimal state.
 */
class SimplePolyphenyDbSchema extends AbstractPolyphenyDbSchema {

    /**
     * Creates a SimplePolyphenyDbSchema.
     *
     * Use {@link AbstractPolyphenyDbSchema#createRootSchema(boolean)} or {@link #add(String, Schema)}.
     */
    SimplePolyphenyDbSchema( AbstractPolyphenyDbSchema parent, Schema schema, String name, SchemaType schemaType ) {
        this(
                parent,
                schema,
                name,
                schemaType,
                null,
                null,
                null,
                null,
                null,
                null,
                null );
    }


    private SimplePolyphenyDbSchema(
            AbstractPolyphenyDbSchema parent,
            Schema schema,
            String name,
            SchemaType schemaType,
            NameMap<PolyphenyDbSchema> subSchemaMap,
            NameMap<TableEntry> tableMap,
            NameMap<TypeEntry> typeMap,
            NameMultimap<FunctionEntry> functionMap,
            NameSet functionNames,
            NameMap<FunctionEntry> nullaryFunctionMap,
            List<? extends List<String>> path ) {
        super( parent, schema, name, schemaType, subSchemaMap, tableMap, typeMap, functionMap, functionNames, nullaryFunctionMap, path );
    }


    @Override
    public void setCache( boolean cache ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public PolyphenyDbSchema add( String name, Schema schema, SchemaType schemaType ) {
        final PolyphenyDbSchema polyphenyDbSchema = new SimplePolyphenyDbSchema( this, schema, name, schemaType );
        subSchemaMap.put( name, polyphenyDbSchema );
        return polyphenyDbSchema;
    }


    @Override
    protected AbstractPolyphenyDbSchema getImplicitSubSchema( String schemaName, boolean caseSensitive ) {
        // Check implicit schemas.
        Schema s = schema.getSubSchema( schemaName );
        if ( s != null ) {
            return new SimplePolyphenyDbSchema( this, s, schemaName, schemaType );
        }
        return null;
    }


    @Override
    protected TableEntry getImplicitTable( String tableName, boolean caseSensitive ) {
        // Check implicit tables.
        Table table = schema.getTable( tableName );
        if ( table != null ) {
            return tableEntry( tableName, table );
        }
        return null;
    }


    @Override
    protected TypeEntry getImplicitType( String name, boolean caseSensitive ) {
        // Check implicit types.
        RelProtoDataType type = schema.getType( name );
        if ( type != null ) {
            return typeEntry( name, type );
        }
        return null;
    }


    @Override
    protected void addImplicitSubSchemaToBuilder( ImmutableSortedMap.Builder<String, PolyphenyDbSchema> builder ) {
        ImmutableSortedMap<String, PolyphenyDbSchema> explicitSubSchemas = builder.build();
        for ( String schemaName : schema.getSubSchemaNames() ) {
            if ( explicitSubSchemas.containsKey( schemaName ) ) {
                // explicit subschema wins.
                continue;
            }
            Schema s = schema.getSubSchema( schemaName );
            if ( s != null ) {
                PolyphenyDbSchema polyphenyDbSchema = new SimplePolyphenyDbSchema( this, s, schemaName, schemaType );
                builder.put( schemaName, polyphenyDbSchema );
            }
        }
    }


    @Override
    protected void addImplicitTableToBuilder( ImmutableSortedSet.Builder<String> builder ) {
        builder.addAll( schema.getTableNames() );
    }


    @Override
    protected void addImplicitFunctionsToBuilder( ImmutableList.Builder<Function> builder, String name, boolean caseSensitive ) {
        Collection<Function> functions = schema.getFunctions( name );
        if ( functions != null ) {
            builder.addAll( functions );
        }
    }


    @Override
    protected void addImplicitFuncNamesToBuilder( ImmutableSortedSet.Builder<String> builder ) {
        builder.addAll( schema.getFunctionNames() );
    }


    @Override
    protected void addImplicitTypeNamesToBuilder( ImmutableSortedSet.Builder<String> builder ) {
        builder.addAll( schema.getTypeNames() );
    }


    @Override
    protected void addImplicitTablesBasedOnNullaryFunctionsToBuilder( ImmutableSortedMap.Builder<String, Table> builder ) {
        ImmutableSortedMap<String, Table> explicitTables = builder.build();

        for ( String s : schema.getFunctionNames() ) {
            // explicit table wins.
            if ( explicitTables.containsKey( s ) ) {
                continue;
            }
            for ( Function function : schema.getFunctions( s ) ) {
                if ( function instanceof TableMacro && function.getParameters().isEmpty() ) {
                    final Table table = ((TableMacro) function).apply( ImmutableList.of() );
                    builder.put( s, table );
                }
            }
        }
    }


    @Override
    protected TableEntry getImplicitTableBasedOnNullaryFunction( String tableName, boolean caseSensitive ) {
        Collection<Function> functions = schema.getFunctions( tableName );
        if ( functions != null ) {
            for ( Function function : functions ) {
                if ( function instanceof TableMacro && function.getParameters().isEmpty() ) {
                    final Table table = ((TableMacro) function).apply( ImmutableList.of() );
                    return tableEntry( tableName, table );
                }
            }
        }
        return null;
    }


    @Override
    protected PolyphenyDbSchema snapshot( AbstractPolyphenyDbSchema parent, SchemaVersion version ) {
        AbstractPolyphenyDbSchema snapshot = new SimplePolyphenyDbSchema(
                parent,
                schema.snapshot( version ),
                name,
                schemaType,
                null,
                tableMap,
                typeMap,
                functionMap,
                functionNames,
                nullaryFunctionMap,
                getPath() );
        for ( PolyphenyDbSchema subSchema : subSchemaMap.map().values() ) {
            PolyphenyDbSchema subSchemaSnapshot = ((AbstractPolyphenyDbSchema) subSchema).snapshot( snapshot, version );
            snapshot.subSchemaMap.put( subSchema.getName(), subSchemaSnapshot );
        }
        return snapshot;
    }


    @Override
    protected boolean isCacheEnabled() {
        return false;
    }

}

