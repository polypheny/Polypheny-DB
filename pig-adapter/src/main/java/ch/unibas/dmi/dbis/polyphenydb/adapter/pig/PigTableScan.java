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


import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRules;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.AggregateExpandDistinctAggregatesRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;

import org.apache.pig.data.DataType;

import java.util.ArrayList;
import java.util.List;


/**
 * Implementation of {@link TableScan} in {@link PigRel#CONVENTION Pig calling convention}.
 */
public class PigTableScan extends TableScan implements PigRel {

    /**
     * Creates a PigTableScan.
     */
    public PigTableScan( RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table ) {
        super( cluster, traitSet, table );
        assert getConvention() == CONVENTION;
    }


    @Override
    public void implement( Implementor implementor ) {
        final PigTable pigTable = getPigTable( implementor.getTableName( this ) );
        final String alias = implementor.getPigRelationAlias( this );
        final String schema = '(' + getSchemaForPigStatement( implementor ) + ')';
        final String statement = alias + " = LOAD '" + pigTable.getFilePath() + "' USING PigStorage() AS " + schema + ';';
        implementor.addStatement( statement );
    }


    private PigTable getPigTable( String name ) {
        final PolyphenyDbSchema schema = getTable().unwrap( PolyphenyDbSchema.class );
        return (PigTable) schema.getTable( name, false ).getTable();
    }


    private String getSchemaForPigStatement( Implementor implementor ) {
        final List<String> fieldNamesAndTypes = new ArrayList<>( getTable().getRowType().getFieldList().size() );
        for ( RelDataTypeField f : getTable().getRowType().getFieldList() ) {
            fieldNamesAndTypes.add( getConcatenatedFieldNameAndTypeForPigSchema( implementor, f ) );
        }
        return String.join( ", ", fieldNamesAndTypes );
    }


    private String getConcatenatedFieldNameAndTypeForPigSchema( Implementor implementor, RelDataTypeField field ) {
        final PigDataType pigDataType = PigDataType.valueOf( field.getType().getSqlTypeName() );
        final String fieldName = implementor.getFieldName( this, field.getIndex() );
        return fieldName + ':' + DataType.findTypeName( pigDataType.getPigType() );
    }


    @Override
    public void register( RelOptPlanner planner ) {
        planner.addRule( PigToEnumerableConverterRule.INSTANCE );
        for ( RelOptRule rule : PigRules.ALL_PIG_OPT_RULES ) {
            planner.addRule( rule );
        }
        // Don't move Aggregates around, otherwise PigAggregate.implement() won't know how to correctly procuce Pig Latin
        planner.removeRule( AggregateExpandDistinctAggregatesRule.INSTANCE );
        // Make sure planner picks PigJoin over EnumerableJoin. Should there be a rule for this instead for removing ENUMERABLE_JOIN_RULE here?
        planner.removeRule( EnumerableRules.ENUMERABLE_JOIN_RULE );
    }
}
