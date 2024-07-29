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
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.schema.types.ScannableEntity;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;

public class ExcelScannableTable extends ExcelTable implements ScannableEntity {

    private final String sheet;


    /**
     * Creates a ExcelScannableTable.
     */
    protected ExcelScannableTable( PhysicalTable table, Source source, AlgProtoDataType protoRowType, List<ExcelFieldType> fieldTypes, int[] fields, ExcelSource excelSource ) {
        this( table, source, protoRowType, fieldTypes, fields, excelSource, "" );
    }


    protected ExcelScannableTable( PhysicalTable table, Source source, AlgProtoDataType protoRowType, List<ExcelFieldType> fieldTypes, int[] fields, ExcelSource excelSource, String sheet ) {
        super( table, source, protoRowType, fieldTypes, fields, excelSource, sheet );
        this.sheet = sheet;
    }


    public String toString() {
        return "ExcelScannableTable";
    }


    @Override
    public Enumerable<PolyValue[]> scan( DataContext dataContext ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( excelSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new ExcelEnumerator( source, cancelFlag, false, null, new ExcelEnumerator.ArrayRowConverter( fieldTypes, fields ), sheet );
            }
        };
    }

}
