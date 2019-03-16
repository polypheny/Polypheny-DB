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

package ch.unibas.dmi.dbis.polyphenydb.adapter.pig;


import ch.unibas.dmi.dbis.polyphenydb.schema.TranslatableTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable.ToRelContext;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.TranslatableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractTable;

import org.apache.pig.data.DataType;


/**
 * Represents a Pig relation that is created by Pig Latin <a href="https://pig.apache.org/docs/r0.13.0/basic.html#load">LOAD</a> statement.
 *
 * Only the default load function is supported at this point (PigStorage()).
 *
 * Only VARCHAR (CHARARRAY in Pig) type supported at this point.
 *
 * @see PigTableFactory
 */
public class PigTable extends AbstractTable implements TranslatableTable {

    private final String filePath;
    private final String[] fieldNames;


    /**
     * Creates a PigTable.
     */
    public PigTable( String filePath, String[] fieldNames ) {
        this.filePath = filePath;
        this.fieldNames = fieldNames;
    }


    @Override
    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        for ( String fieldName : fieldNames ) {
            // only supports CHARARRAY types for now
            final RelDataType relDataType = typeFactory.createSqlType( PigDataType.valueOf( DataType.CHARARRAY ).getSqlType() );
            final RelDataType nullableRelDataType = typeFactory.createTypeWithNullability( relDataType, true );
            builder.add( fieldName, nullableRelDataType );
        }
        return builder.build();
    }


    public String getFilePath() {
        return filePath;
    }


    @Override
    public RelNode toRel( ToRelContext context, RelOptTable relOptTable ) {
        final RelOptCluster cluster = context.getCluster();
        return new PigTableScan( cluster, cluster.traitSetOf( PigRel.CONVENTION ), relOptTable );
    }
}

