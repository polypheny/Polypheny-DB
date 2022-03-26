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

import java.util.Collection;
import java.util.List;
import org.apache.calcite.linq4j.Queryable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.AbstractQueryableTable;
import org.polypheny.db.adapter.neo4j.rules.NeoScan;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Modify;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.logical.LogicalModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptTable.ToAlgContext;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.TranslatableTable;

public class NeoEntity extends AbstractQueryableTable implements TranslatableTable, ModifiableTable {

    public final String phsicalEntityName;
    public final long id;
    public final AlgProtoDataType rowType;


    protected NeoEntity( String physicalEntityName, AlgProtoDataType proto, long id ) {
        super( Object[].class );
        this.phsicalEntityName = physicalEntityName;
        this.rowType = proto;
        this.id = id;
    }


    @Override
    public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
        return null;
    }


    @Override
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
        return null;
    }


    @Override
    public AlgNode toAlg( ToAlgContext context, AlgOptTable algOptTable ) {
        final AlgOptCluster cluster = context.getCluster();
        return new NeoScan( cluster, cluster.traitSetOf( NeoConvention.INSTANCE ), algOptTable, this );
    }


    @Override
    public Collection<?> getModifiableCollection() {
        throw new UnsupportedOperationException( "getModifiableCollection is not supported by the NEO4j adapter." );
    }


    @Override
    public Modify toModificationAlg( AlgOptCluster cluster, AlgOptTable table, CatalogReader catalogReader, AlgNode child, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        NeoConvention.INSTANCE.register( cluster.getPlanner() );
        return new LogicalModify(
                cluster,
                cluster.traitSetOf( Convention.NONE ),
                table,
                catalogReader,
                child,
                operation,
                updateColumnList,
                sourceExpressionList,
                flattened );
    }

}
