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

package org.polypheny.db.algebra.logical.lpg;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.relational.RelationalTransformable;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptTable.ToAlgContext;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.schema.Function;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.SchemaVersion;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.TranslatableGraph;
import org.polypheny.db.schema.graph.Graph;
import org.polypheny.db.type.PolyType;


@Getter
public class LogicalGraph implements RelationalTransformable, Schema, Graph, TranslatableGraph {

    private final long id;


    /**
     * {@link org.polypheny.db.catalog.Catalog.NamespaceType#GRAPH} implementation of an entity, called graph
     */
    public LogicalGraph( long id ) {
        this.id = id;
    }


    @Override
    public List<AlgNode> getRelationalEquivalent( List<AlgNode> values, List<AlgOptTable> entities, CatalogReader catalogReader ) {
        return null;
    }


    @Override
    public Table getTable( String name ) {
        return null;
    }


    @Override
    public Set<String> getTableNames() {
        return Set.of();
    }


    @Override
    public AlgProtoDataType getType( String name ) {
        return AlgDataTypeImpl.proto( PolyType.GRAPH, false );
    }


    @Override
    public Set<String> getTypeNames() {
        return Set.of();
    }


    @Override
    public Collection<Function> getFunctions( String name ) {
        return ImmutableList.of();
    }


    @Override
    public Set<String> getFunctionNames() {
        return Set.of();
    }


    @Override
    public Schema getSubSchema( String name ) {
        return null;
    }


    @Override
    public Set<String> getSubSchemaNames() {
        return Set.of();
    }


    @Override
    public Expression getExpression( SchemaPlus parentSchema, String name ) {
        return Schemas.subSchemaExpression( parentSchema, name, LogicalGraph.class );
    }


    @Override
    public boolean isMutable() {
        return true;
    }


    @Override
    public Schema snapshot( SchemaVersion version ) {
        return new LogicalGraph( id );
    }


    @Override
    public <C> C unwrap( Class<C> aClass ) {
        return null;
    }


    @Override
    public AlgNode toAlg( ToAlgContext context, Graph graph ) {
        throw new RuntimeException( "toAlg() is not implemented for Logical Graphs!" );
    }

}
