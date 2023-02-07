/*
 * Copyright 2019-2023 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.prepare;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.MonikerType;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.schema.Function;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.Wrapper;
import org.polypheny.db.schema.graph.Graph;
import org.polypheny.db.util.Moniker;
import org.polypheny.db.util.MonikerImpl;
import org.polypheny.db.util.NameMatcher;
import org.polypheny.db.util.NameMatchers;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.ValidatorUtil;


/**
 * Implementation of {@link org.polypheny.db.prepare.Prepare.CatalogReader} and also {@link OperatorTable} based on
 * tables and functions defined schemas.
 */
public class PolyphenyDbCatalogReader implements Prepare.CatalogReader {

    protected final PolyphenyDbSchema rootSchema;
    protected final AlgDataTypeFactory typeFactory;
    private final List<List<String>> schemaPaths;
    protected final NameMatcher nameMatcher;


    public PolyphenyDbCatalogReader( PolyphenyDbSchema rootSchema, List<String> defaultSchema, AlgDataTypeFactory typeFactory ) {
        this(
                rootSchema,
                NameMatchers.withCaseSensitive( RuntimeConfig.RELATIONAL_CASE_SENSITIVE.getBoolean() ),
                ImmutableList.of( Objects.requireNonNull( defaultSchema ), ImmutableList.of() ),
                typeFactory );
    }


    protected PolyphenyDbCatalogReader( PolyphenyDbSchema rootSchema, NameMatcher nameMatcher, List<List<String>> schemaPaths, AlgDataTypeFactory typeFactory ) {
        this.rootSchema = Objects.requireNonNull( rootSchema );
        this.nameMatcher = nameMatcher;
        this.schemaPaths =
                Util.immutableCopy( Util.isDistinct( schemaPaths )
                        ? schemaPaths
                        : new LinkedHashSet<>( schemaPaths ) );
        this.typeFactory = typeFactory;
    }


    @Override
    public PolyphenyDbCatalogReader withSchemaPath( List<String> schemaPath ) {
        return new PolyphenyDbCatalogReader( rootSchema, nameMatcher, ImmutableList.of( schemaPath, ImmutableList.of() ), typeFactory );
    }


    @Override
    public Prepare.PreparingTable getTable( final List<String> names ) {
        // First look in the default schema, if any. If not found, look in the root schema.
        PolyphenyDbSchema.TableEntry entry = ValidatorUtil.getTableEntry( this, names );
        if ( entry != null ) {
            final Table table = entry.getTable();
            if ( table instanceof Wrapper ) {
                final Prepare.PreparingTable algOptTable = ((Wrapper) table).unwrap( Prepare.PreparingTable.class );
                if ( algOptTable != null ) {
                    return algOptTable;
                }
            }
            return AlgOptTableImpl.create( this, table.getRowType( typeFactory ), entry, null );
        }
        return null;
    }


    @Override
    public AlgOptTable getCollection( final List<String> names ) {
        // First look in the default schema, if any. If not found, look in the root schema.
        PolyphenyDbSchema.TableEntry entry = ValidatorUtil.getTableEntry( this, names );
        if ( entry != null ) {
            final Table table = entry.getTable();
            return AlgOptTableImpl.create( this, table.getRowType( typeFactory ), entry, null );
        }
        return null;
    }


    @Override
    public Graph getGraph( final String name ) {
        PolyphenyDbSchema schema = rootSchema.getSubSchema( name, true );
        return schema == null ? null : (Graph) schema.getSchema();
    }


    public Collection<Function> getFunctionsFrom( List<String> names ) {
        final List<Function> functions2 = new ArrayList<>();
        final List<List<String>> schemaNameList = new ArrayList<>();
        if ( names.size() > 1 ) {
            // Name qualified: ignore path. But we do look in "/catalog" and "/", the last 2 items in the path.
            if ( schemaPaths.size() > 1 ) {
                schemaNameList.addAll( Util.skip( schemaPaths ) );
            } else {
                schemaNameList.addAll( schemaPaths );
            }
        } else {
            for ( List<String> schemaPath : schemaPaths ) {
                PolyphenyDbSchema schema = ValidatorUtil.getSchema( rootSchema, schemaPath, nameMatcher );
                if ( schema != null ) {
                    schemaNameList.addAll( schema.getPath() );
                }
            }
        }
        for ( List<String> schemaNames : schemaNameList ) {
            PolyphenyDbSchema schema = ValidatorUtil.getSchema( rootSchema, Iterables.concat( schemaNames, Util.skipLast( names ) ), nameMatcher );
            if ( schema != null ) {
                final String name = Util.last( names );
                functions2.addAll( schema.getFunctions( name, true ) );
            }
        }
        return functions2;
    }


    @Override
    public AlgDataType getNamedType( Identifier typeName ) {
        PolyphenyDbSchema.TypeEntry typeEntry = ValidatorUtil.getTypeEntry( getRootSchema(), typeName );
        if ( typeEntry != null ) {
            return typeEntry.getType().apply( typeFactory );
        } else {
            return null;
        }
    }


    @Override
    public List<Moniker> getAllSchemaObjectNames( List<String> names ) {
        final PolyphenyDbSchema schema = ValidatorUtil.getSchema( rootSchema, names, nameMatcher );
        if ( schema == null ) {
            return ImmutableList.of();
        }
        final List<Moniker> result = new ArrayList<>();

        // Add root schema if not anonymous
        if ( !schema.getName().equals( "" ) ) {
            result.add( moniker( schema, null, MonikerType.SCHEMA ) );
        }

        final Map<String, PolyphenyDbSchema> schemaMap = schema.getSubSchemaMap();

        for ( String subSchema : schemaMap.keySet() ) {
            result.add( moniker( schema, subSchema, MonikerType.SCHEMA ) );
        }

        for ( String table : schema.getTableNames() ) {
            result.add( moniker( schema, table, MonikerType.TABLE ) );
        }

        final NavigableSet<String> functions = schema.getFunctionNames();
        for ( String function : functions ) { // views are here as well
            result.add( moniker( schema, function, MonikerType.FUNCTION ) );
        }
        return result;
    }


    private Moniker moniker( PolyphenyDbSchema schema, String name, MonikerType type ) {
        final List<String> path = schema.path( name );
        if ( path.size() == 1 && !schema.root().getName().equals( "" ) && type == MonikerType.SCHEMA ) {
            type = MonikerType.CATALOG;
        }
        return new MonikerImpl( path, type );
    }


    @Override
    public List<List<String>> getSchemaPaths() {
        return schemaPaths;
    }


    @Override
    public Prepare.PreparingTable getTableForMember( List<String> names ) {
        return getTable( names );
    }


    @Override
    public AlgDataType createTypeFromProjection( final AlgDataType type, final List<String> columnNameList ) {
        return ValidatorUtil.createTypeFromProjection( type, columnNameList, typeFactory, nameMatcher.isCaseSensitive() );
    }


    @Override
    public void lookupOperatorOverloads( Identifier opName, FunctionCategory category, Syntax syntax, List<Operator> operatorList ) {
        throw new UnsupportedOperationException( "This operation is not longer supported" );
    }


    @Override
    public List<Operator> getOperatorList() {
        return null;
    }


    @Override
    public PolyphenyDbSchema getRootSchema() {
        return rootSchema;
    }


    @Override
    public AlgDataTypeFactory getTypeFactory() {
        return typeFactory;
    }


    @Override
    public void registerRules( AlgOptPlanner planner ) {
    }


    @Override
    public NameMatcher nameMatcher() {
        return nameMatcher;
    }


    @Override
    public <C> C unwrap( Class<C> aClass ) {
        if ( aClass.isInstance( this ) ) {
            return aClass.cast( this );
        }
        return null;
    }

}

