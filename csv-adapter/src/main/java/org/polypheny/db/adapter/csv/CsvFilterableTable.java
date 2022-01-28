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

package org.polypheny.db.adapter.csv;


import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.FilterableTable;
import org.polypheny.db.util.Source;


/**
 * Table based on a CSV file that can implement simple filtering.
 *
 * It implements the {@link FilterableTable} interface, so Polypheny-DB gets data by calling the {@link #scan(DataContext, List)} method.
 */
public class CsvFilterableTable extends CsvTable implements FilterableTable {

    /**
     * Creates a CsvFilterableTable.
     */
    public CsvFilterableTable( Source source, AlgProtoDataType protoRowType, List<CsvFieldType> fieldTypes, int[] fields, CsvSource csvSource, Long tableId ) {
        super( source, protoRowType, fieldTypes, fields, csvSource, tableId );
    }


    public String toString() {
        return "CsvFilterableTable";
    }


    @Override
    public Enumerable<Object[]> scan( DataContext dataContext, List<RexNode> filters ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( csvSource );
        final String[] filterValues = new String[fieldTypes.size()];
        filters.removeIf( filter -> addFilter( filter, filterValues ) );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new CsvEnumerator<>( source, cancelFlag, false, filterValues, new CsvEnumerator.ArrayRowConverter( fieldTypes, fields ) );
            }
        };
    }


    private boolean addFilter( RexNode filter, Object[] filterValues ) {
        if ( filter.isA( Kind.EQUALS ) ) {
            final RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get( 0 );
            if ( left.isA( Kind.CAST ) ) {
                left = ((RexCall) left).operands.get( 0 );
            }
            final RexNode right = call.getOperands().get( 1 );
            if ( left instanceof RexInputRef && right instanceof RexLiteral ) {
                final int index = ((RexInputRef) left).getIndex();
                if ( filterValues[index] == null ) {
                    filterValues[index] = ((RexLiteral) right).getValue2().toString();
                    return true;
                }
            }
        }
        return false;
    }

}

