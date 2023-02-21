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

package org.polypheny.db.schema;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import java.util.Collection;
import java.util.List;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
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
     * Use {@link AbstractPolyphenyDbSchema#createRootSchema(String)} or {@link #add(String, Namespace, NamespaceType)}.
     */
    SimplePolyphenyDbSchema( AbstractPolyphenyDbSchema parent, Namespace namespace, String name, NamespaceType namespaceType, boolean caseSensitive ) {
        this(
                parent,
                namespace,
                name,
                namespaceType,
                caseSensitive,
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
            Namespace namespace,
            String name,
            NamespaceType namespaceType,
            boolean caseSensitive,
            NameMap<PolyphenyDbSchema> subSchemaMap,
            NameMap<TableEntry> tableMap,
            NameMap<TypeEntry> typeMap,
            NameMultimap<FunctionEntry> functionMap,
            NameSet functionNames,
            NameMap<FunctionEntry> nullaryFunctionMap,
            List<? extends List<String>> path ) {
        super( parent, namespace, name, namespaceType, caseSensitive, subSchemaMap, tableMap, typeMap, functionMap, functionNames, nullaryFunctionMap, path );
    }


    @Override
    public void setCache( boolean cache ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public PolyphenyDbSchema add( String name, Namespace namespace, NamespaceType namespaceType ) {
        final PolyphenyDbSchema polyphenyDbSchema = new SimplePolyphenyDbSchema( this, namespace, name, namespaceType, false );
        subSchemaMap.put( name, polyphenyDbSchema );
        return polyphenyDbSchema;
    }


    @Override
    protected AbstractPolyphenyDbSchema getImplicitSubSchema( String schemaName, boolean caseSensitive ) {
        // Check implicit schemas.
        Namespace s = namespace.getSubNamespace( schemaName );
        if ( s != null ) {
            return new SimplePolyphenyDbSchema( this, s, schemaName, namespaceType, false );
        }
        return null;
    }


    @Override
    protected TableEntry getImplicitTable( String tableName ) {
        // Check implicit tables.
        Entity entity = namespace.getEntity( tableName );
        if ( entity != null ) {
            return tableEntry( tableName, entity );
        }
        return null;
    }


    @Override
    protected TypeEntry getImplicitType( String name, boolean caseSensitive ) {
        // Check implicit types.
        AlgProtoDataType type = namespace.getType( name );
        if ( type != null ) {
            return typeEntry( name, type );
        }
        return null;
    }


    @Override
    protected void addImplicitSubSchemaToBuilder( ImmutableSortedMap.Builder<String, PolyphenyDbSchema> builder ) {
        ImmutableSortedMap<String, PolyphenyDbSchema> explicitSubSchemas = builder.build();
        for ( String schemaName : namespace.getSubNamespaceNames() ) {
            if ( explicitSubSchemas.containsKey( schemaName ) ) {
                // explicit subschema wins.
                continue;
            }
            Namespace s = namespace.getSubNamespace( schemaName );
            if ( s != null ) {
                PolyphenyDbSchema polyphenyDbSchema = new SimplePolyphenyDbSchema( this, s, schemaName, namespaceType, false );
                builder.put( schemaName, polyphenyDbSchema );
            }
        }
    }


    @Override
    protected void addImplicitTableToBuilder( ImmutableSortedSet.Builder<String> builder ) {
        builder.addAll( namespace.getEntityNames() );
    }


    @Override
    protected void addImplicitFunctionsToBuilder( ImmutableList.Builder<Function> builder, String name, boolean caseSensitive ) {
        Collection<Function> functions = namespace.getFunctions( name );
        if ( functions != null ) {
            builder.addAll( functions );
        }
    }


    @Override
    protected void addImplicitFuncNamesToBuilder( ImmutableSortedSet.Builder<String> builder ) {
        builder.addAll( namespace.getFunctionNames() );
    }


    @Override
    protected void addImplicitTypeNamesToBuilder( ImmutableSortedSet.Builder<String> builder ) {
        builder.addAll( namespace.getTypeNames() );
    }


    @Override
    protected void addImplicitTablesBasedOnNullaryFunctionsToBuilder( ImmutableSortedMap.Builder<String, Entity> builder ) {
        ImmutableSortedMap<String, Entity> explicitTables = builder.build();

        for ( String s : namespace.getFunctionNames() ) {
            // explicit table wins.
            if ( explicitTables.containsKey( s ) ) {
                continue;
            }
            for ( Function function : namespace.getFunctions( s ) ) {
                if ( function instanceof TableMacro && function.getParameters().isEmpty() ) {
                    final Entity entity = ((TableMacro) function).apply( ImmutableList.of() );
                    builder.put( s, entity );
                }
            }
        }
    }


    @Override
    protected TableEntry getImplicitTableBasedOnNullaryFunction( String tableName, boolean caseSensitive ) {
        Collection<Function> functions = namespace.getFunctions( tableName );
        if ( functions != null ) {
            for ( Function function : functions ) {
                if ( function instanceof TableMacro && function.getParameters().isEmpty() ) {
                    final Entity entity = ((TableMacro) function).apply( ImmutableList.of() );
                    return tableEntry( tableName, entity );
                }
            }
        }
        return null;
    }


    @Override
    protected PolyphenyDbSchema snapshot( AbstractPolyphenyDbSchema parent, SchemaVersion version ) {
        AbstractPolyphenyDbSchema snapshot = new SimplePolyphenyDbSchema(
                parent,
                namespace.snapshot( version ),
                name,
                namespaceType,
                isCaseSensitive(),
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

