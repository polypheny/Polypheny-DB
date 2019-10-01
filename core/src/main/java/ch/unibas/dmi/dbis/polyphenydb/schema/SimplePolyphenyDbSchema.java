/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.schema;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.util.NameMap;
import ch.unibas.dmi.dbis.polyphenydb.util.NameMultimap;
import ch.unibas.dmi.dbis.polyphenydb.util.NameSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import java.util.Collection;
import java.util.List;


/**
 * A concrete implementation of {@link AbstractPolyphenyDbSchema} that maintains minimal state.
 */
class SimplePolyphenyDbSchema extends AbstractPolyphenyDbSchema {

    /**
     * Creates a SimplePolyphenyDbSchema.
     *
     * Use {@link AbstractPolyphenyDbSchema#createRootSchema(boolean)} or {@link #add(String, Schema)}.
     */
    SimplePolyphenyDbSchema( AbstractPolyphenyDbSchema parent, Schema schema, String name ) {
        this( parent, schema, name, null, null, null, null, null, null, null );
    }


    private SimplePolyphenyDbSchema(
            AbstractPolyphenyDbSchema parent,
            Schema schema,
            String name,
            NameMap<PolyphenyDbSchema> subSchemaMap,
            NameMap<TableEntry> tableMap,
            NameMap<TypeEntry> typeMap,
            NameMultimap<FunctionEntry> functionMap,
            NameSet functionNames,
            NameMap<FunctionEntry> nullaryFunctionMap,
            List<? extends List<String>> path ) {
        super( parent, schema, name, subSchemaMap, tableMap, typeMap, functionMap, functionNames, nullaryFunctionMap, path );
    }


    @Override
    public void setCache( boolean cache ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public PolyphenyDbSchema add( String name, Schema schema ) {
        final PolyphenyDbSchema polyphenyDbSchema = new SimplePolyphenyDbSchema( this, schema, name );
        subSchemaMap.put( name, polyphenyDbSchema );
        return polyphenyDbSchema;
    }


    @Override
    protected AbstractPolyphenyDbSchema getImplicitSubSchema( String schemaName, boolean caseSensitive ) {
        // Check implicit schemas.
        Schema s = schema.getSubSchema( schemaName );
        if ( s != null ) {
            return new SimplePolyphenyDbSchema( this, s, schemaName );
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
                PolyphenyDbSchema polyphenyDbSchema = new SimplePolyphenyDbSchema( this, s, schemaName );
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
        AbstractPolyphenyDbSchema snapshot = new SimplePolyphenyDbSchema( parent, schema.snapshot( version ), name, null, tableMap, typeMap, functionMap, functionNames, nullaryFunctionMap, getPath() );
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

