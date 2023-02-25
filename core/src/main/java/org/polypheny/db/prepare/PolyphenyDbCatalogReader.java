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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.MonikerType;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.entity.LogicalCollection;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgOptEntity;
import org.polypheny.db.prepare.Prepare.PreparingEntity;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.Wrapper;
import org.polypheny.db.util.Moniker;
import org.polypheny.db.util.MonikerImpl;
import org.polypheny.db.util.ValidatorUtil;


/**
 * Implementation of {@link Prepare.CatalogReader} and also {@link OperatorTable} based on
 * tables and functions defined schemas.
 */
public class PolyphenyDbCatalogReader implements Prepare.CatalogReader {

    protected final PolyphenyDbSchema rootSchema;
    protected final AlgDataTypeFactory typeFactory;


    public PolyphenyDbCatalogReader( PolyphenyDbSchema rootSchema, AlgDataTypeFactory typeFactory ) {
        this.rootSchema = Objects.requireNonNull( rootSchema );
        this.typeFactory = typeFactory;
    }


    @Override
    public PreparingEntity getTable( final List<String> names ) {
        // First look in the default schema, if any. If not found, look in the root schema.
        CatalogEntity entity = rootSchema.getTable( names );
        return AlgOptEntityImpl.create( this, entity.getRowType(), entity, null, null );

    }


    @Override
    public AlgOptEntity getCollection( final List<String> names ) {
        // First look in the default schema, if any. If not found, look in the root schema.
        LogicalCollection collection = rootSchema.getCollection( names );
        if ( collection != null ) {
            return AlgOptEntityImpl.create( this, collection.getRowType(), collection, null, null );
        }
        return null;
    }


    @Override
    public LogicalGraph getGraph( final String name ) {
        return rootSchema.getGraph( List.of( name ) );
    }


    @Override
    public AlgDataType getNamedType( Identifier typeName ) {
        LogicalTable table = rootSchema.getTable( typeName.getNames() );
        if ( table != null ) {
            return table.getRowType();
        } else {
            return null;
        }
    }


    @Override
    public List<Moniker> getAllSchemaObjectNames( List<String> names ) {
        final PolyphenyDbSchema schema = ValidatorUtil.getSchema( rootSchema, names, Wrapper.nameMatcher );
        final List<Moniker> result = new ArrayList<>();

        for ( String subSchema : rootSchema.getNamespaceNames() ) {
            result.add( moniker( schema, subSchema, MonikerType.SCHEMA ) );
        }

        return result;
    }


    private Moniker moniker( PolyphenyDbSchema schema, String name, MonikerType type ) {
        /*final List<String> path = schema.path( name );
        if ( path.size() == 1 && !schema.root().getName().equals( "" ) && type == MonikerType.SCHEMA ) {
            type = MonikerType.CATALOG;
        }*/
        return new MonikerImpl( name, type );
    }


    @Override
    public PreparingEntity getTableForMember( List<String> names ) {
        return getTable( names );
    }


    @Override
    public AlgDataType createTypeFromProjection( final AlgDataType type, final List<String> columnNameList ) {
        return ValidatorUtil.createTypeFromProjection( type, columnNameList, typeFactory, Wrapper.nameMatcher.isCaseSensitive() );
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


}

