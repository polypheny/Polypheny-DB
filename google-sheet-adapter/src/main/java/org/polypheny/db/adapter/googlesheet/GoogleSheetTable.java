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

package org.polypheny.db.adapter.googlesheet;

/**
 * Questions:
 * - What is protoRowType?
 */

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.FilterableTable;
import org.polypheny.db.schema.impl.AbstractTable;
import org.polypheny.db.util.Pair;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Variables: sheets url, protoRowType, []int fields, List of SheetFieldTypes,  SheetDataSource, (?) Mapper
 *
 * - String: return "Google Sheets table"
 *
 * - getRowType: there are generally two different structures that we can see here.
 * (ETH): you create a list of types, a list of names, stitch them together, call typeFactory to handle it. Does not handle case where protoRowType is none.
 * (CSV): do the above in one line (not sure they're the same), and if protoRowType is null, we call deduceRowType from Enum... what does it to, well:
 *  it goes into the file, looks at the names and associated, puts them into names[] and types[] list, calls typeFactory.createStructType on them.
 *
 * - scan: optional, but not really - implement for Sheets.
 * (ETH): registerInvolvedAdapter for Context, then create a bunch of options to create the ETH Enumerator.
 * For Google Sheets, you need: cancelFlag, RowConverter (which is the CSV.RowConverter) - CP
 *
 *
 */

public class GoogleSheetTable extends AbstractTable implements FilterableTable {
    protected final URL sheetsUrl;
    protected final String tableName;
    protected final AlgProtoDataType protoRowType;
    protected final int[] fields;
    protected final GoogleSheetSource googleSheetSource;
    protected List<GoogleSheetFieldType> fieldTypes;

    public GoogleSheetTable (
            URL sheetsUrl,
            String tableName,
            AlgProtoDataType protoRowType,
            int[] fields,
            GoogleSheetSource googleSheetSource,
            List<GoogleSheetFieldType> fieldTypes ) {
        this.sheetsUrl = sheetsUrl;
        this.tableName = tableName;
        this.protoRowType = protoRowType;
        this.fields = fields;
        this.googleSheetSource = googleSheetSource;
        this.fieldTypes = fieldTypes;
    }

    public String toString() {
        return "GoogleSheetTable";
    }

    /**
     * Could change to more detailed implementation such as with CSV later?
     */
    @Override
    public AlgDataType getRowType(AlgDataTypeFactory typeFactory ) {
        final List<AlgDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();
        for ( AlgDataTypeField field : this.protoRowType.apply( typeFactory ).getFieldList() ) {
            types.add( field.getType() );
            names.add( field.getName() );
        }
        return typeFactory.createStructType( Pair.zip( names, types ) );
    }

    /**
     * Not really filtering, but can't tell if it's necessary or not for any scan. Assuming it is.
     */
    @Override
    public Enumerable scan(DataContext dataContext, List<RexNode> filters ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( googleSheetSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );

        // TODO: check this.
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new GoogleSheetEnumerator<>(sheetsUrl, tableName, cancelFlag, false, new GoogleSheetEnumerator.ArrayRowConverter( fieldTypes, fields ));
            }
        };
    }



}
