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

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.util.Source;

public class ExcelTable extends PhysicalTable {

    protected final Source source;
    protected final AlgProtoDataType protoRowType;
    protected List<ExcelFieldType> fieldTypes;
    protected final int[] fields;
    protected final ExcelSource excelSource;
    protected final String sheet;


    /**
     * Creates a ExcelTable.
     */
    ExcelTable( PhysicalTable table, Source source, AlgProtoDataType protoRowType, List<ExcelFieldType> fieldTypes, int[] fields, ExcelSource excelSource, String sheet ) {
        super( table.id,
                table.allocationId,
                table.logicalId,
                table.name,
                table.columns,
                table.namespaceId,
                table.namespaceName,
                table.uniqueFieldIds,
                table.adapterId );
        this.source = source;
        this.protoRowType = protoRowType;
        this.fieldTypes = fieldTypes;
        this.fields = fields;
        this.excelSource = excelSource;
        this.sheet = sheet;
    }


    @Override
    public AlgDataType getTupleType( AlgDataTypeFactory typeFactory ) {
        if ( protoRowType != null ) {
            return protoRowType.apply( typeFactory );
        }
        if ( this.sheet.isEmpty() ) {
            if ( fieldTypes == null ) {
                fieldTypes = new ArrayList<>();
                return ExcelEnumerator.deduceRowType( (JavaTypeFactory) typeFactory, source, fieldTypes );
            } else {
                return ExcelEnumerator.deduceRowType( (JavaTypeFactory) typeFactory, source, null );
            }
        } else {
            if ( fieldTypes == null ) {
                fieldTypes = new ArrayList<>();
                ExcelEnumerator.setSheet( sheet );
                return ExcelEnumerator.deduceRowType( (JavaTypeFactory) typeFactory, source, sheet, fieldTypes );
            } else {
                return ExcelEnumerator.deduceRowType( (JavaTypeFactory) typeFactory, source, sheet, null );
            }
        }
    }


    /**
     * Various degrees of table "intelligence".
     */
    public enum Flavor {
        SCANNABLE, FILTERABLE, TRANSLATABLE
    }

}
