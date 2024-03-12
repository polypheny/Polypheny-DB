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

package org.polypheny.db.adapter.excel;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;

public class ExcelTranslatableTable extends ExcelTable implements TranslatableEntity {

    /**
     * Creates a ExcelTable.
     */
    ExcelTranslatableTable( PhysicalTable table, Source source, AlgProtoDataType protoRowType, List<ExcelFieldType> fieldTypes, int[] fields, ExcelSource excelSource ) {
        this( table, source, protoRowType, fieldTypes, fields, excelSource, "" );
    }


    ExcelTranslatableTable( PhysicalTable table, Source source, AlgProtoDataType protoRowType, List<ExcelFieldType> fieldTypes, int[] fields, ExcelSource excelSource, String sheet ) {
        super( table, source, protoRowType, fieldTypes, fields, excelSource, sheet );
    }


    public String toString() {
        return "ExcelTranslatableTable";
    }


    /**
     * Returns an enumerable over a given projection of the fields.
     * <p>
     * Called from generated code.
     */
    public Enumerable<PolyValue[]> project( final DataContext dataContext, final int[] fields ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( excelSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<PolyValue[]>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new ExcelEnumerator( source, cancelFlag, fieldTypes, fields, sheet );
            }
        };
    }


    @Override
    public AlgNode toAlg( AlgCluster cluster, AlgTraitSet traitSet ) {
        return new ExcelTableScan( this, cluster, this, fields );
    }

}
