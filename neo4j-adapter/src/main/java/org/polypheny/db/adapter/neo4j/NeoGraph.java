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

package org.polypheny.db.adapter.neo4j;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;
import org.neo4j.driver.Driver;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.logical.graph.GraphModify;
import org.polypheny.db.algebra.logical.graph.LogicalGraphModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable.ToAlgContext;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModifiableGraph;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Statistic;
import org.polypheny.db.schema.TranslatableGraph;
import org.polypheny.db.schema.graph.Graph;
import org.polypheny.db.schema.impl.AbstractSchema;

public class NeoGraph extends AbstractSchema implements ModifiableGraph, TranslatableGraph {

    private final String name;
    private final TransactionProvider transactionProvider;
    private final Driver db;
    @Getter
    private final long id;


    public NeoGraph( String name, TransactionProvider transactionProvider, Driver db, long id ) {
        this.name = name;
        this.id = id;
        this.transactionProvider = transactionProvider;
        this.db = db;
    }


    @Override
    public GraphModify toModificationAlg( AlgOptCluster cluster, AlgTraitSet traits, Graph graph, PolyphenyDbCatalogReader catalogReader, AlgNode input, Operation operation, List<String> ids, List<? extends RexNode> operations ) {
        NeoConvention.INSTANCE.register( cluster.getPlanner() );
        return new LogicalGraphModify(
                cluster,
                cluster.traitSet().replace( Convention.NONE ),
                graph,
                catalogReader,
                input,
                operation,
                ids,
                operations );
    }


    @Override
    public Expression getExpression( SchemaPlus schema, String tableName, Class<?> clazz ) {
        return null;
    }


    @Override
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
        return null;
    }


    @Override
    public Statistic getStatistic() {
        return null;
    }


    @Override
    public <C> C unwrap( Class<C> aClass ) {
        return null;
    }


    @Override
    public AlgNode toAlg( ToAlgContext context, Graph graph ) {
        return null;
    }

}
