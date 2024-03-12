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

package org.polypheny.db.adapter.googlesheet;

import java.net.URL;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Base table class based on individual Google Sheets.
 */
public class GoogleSheetTable extends PhysicalTable implements TranslatableEntity {

    protected final URL sheetsUrl;
    protected final int querySize;
    protected final String tableName;
    protected final AlgProtoDataType protoRowType;
    protected final int[] fields;
    protected final GoogleSheetSource googleSheetSource;
    protected List<GoogleSheetFieldType> fieldTypes;


    public GoogleSheetTable(
            PhysicalTable table,
            URL sheetsUrl,
            int querySize,
            String tableName,
            AlgProtoDataType protoRowType,
            int[] fields,
            GoogleSheetSource googleSheetSource,
            List<GoogleSheetFieldType> fieldTypes ) {
        super( table.id,
                table.allocationId,
                table.logicalId,
                table.name,
                table.columns,
                table.namespaceId,
                table.namespaceName,
                table.uniqueFieldIds,
                table.adapterId );
        this.sheetsUrl = sheetsUrl;
        this.querySize = querySize;
        this.tableName = tableName;
        this.protoRowType = protoRowType;
        this.fields = fields;
        this.googleSheetSource = googleSheetSource;
        this.fieldTypes = fieldTypes;
    }


    public String toString() {
        return "GoogleSheetTable";
    }


    @Override
    public AlgDataType getTupleType( AlgDataTypeFactory typeFactory ) {
        return typeFactory.createStructType( this.protoRowType.apply( typeFactory ).getFields() );
    }


    /**
     * Returns an enumerator over a given projection
     */
    public Enumerable<PolyValue[]> project( final DataContext dataContext, final int[] fields ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( googleSheetSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new GoogleSheetEnumerator<>( sheetsUrl, querySize, tableName, cancelFlag, fieldTypes, fields, googleSheetSource );
            }
        };
    }


    @Override
    public AlgNode toAlg( AlgCluster cluster, AlgTraitSet traitSet ) {
        // Request all fields.
        return new GoogleSheetTableScanProject( cluster, this, this, fields );
    }


    public Expression getExpression() {
        return Expressions.convert_(
                Expressions.call(
                        Expressions.call(
                                googleSheetSource.getCatalogAsExpression(),
                                "getPhysical", Expressions.constant( id ) ),
                        "unwrapOrThrow", Expressions.constant( GoogleSheetTable.class ) ),
                GoogleSheetTable.class );
    }

}
