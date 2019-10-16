/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.csv;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexCall;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexInputRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.schema.FilterableTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.util.Source;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;


/**
 * Table based on a CSV file that can implement simple filtering.
 *
 * It implements the {@link FilterableTable} interface, so Polypheny-DB gets data by calling the {@link #scan(DataContext, List)} method.
 */
public class CsvFilterableTable extends CsvTable implements FilterableTable {

    /**
     * Creates a CsvFilterableTable.
     */
    public CsvFilterableTable( Source source, RelProtoDataType protoRowType, List<CsvFieldType> fieldTypes ) {
        super( source, protoRowType, fieldTypes );
    }


    public String toString() {
        return "CsvFilterableTable";
    }


    @Override
    public Enumerable<Object[]> scan( DataContext root, List<RexNode> filters ) {
        final String[] filterValues = new String[fieldTypes.size()];
        filters.removeIf( filter -> addFilter( filter, filterValues ) );
        final int[] fields = CsvEnumerator.identityList( fieldTypes.size() );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( root );
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new CsvEnumerator<>( source, cancelFlag, false, filterValues, new CsvEnumerator.ArrayRowConverter( fieldTypes, fields ) );
            }
        };
    }


    private boolean addFilter( RexNode filter, Object[] filterValues ) {
        if ( filter.isA( SqlKind.EQUALS ) ) {
            final RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get( 0 );
            if ( left.isA( SqlKind.CAST ) ) {
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

