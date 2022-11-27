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

package org.polypheny.db.adapter.pig;


import java.util.ArrayList;
import java.util.List;
import org.apache.pig.data.DataType;
import org.polypheny.db.adapter.enumerable.EnumerableRules;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.rules.AggregateExpandDistinctAggregatesRule;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.PolyphenyDbSchema;


/**
 * Implementation of {@link Scan} in {@link PigAlg#CONVENTION Pig calling convention}.
 */
public class PigScan extends Scan implements PigAlg {

    /**
     * Creates a PigScan.
     */
    public PigScan( AlgOptCluster cluster, AlgTraitSet traitSet, AlgOptTable table ) {
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
        return (PigTable) schema.getTable( name ).getTable();
    }


    private String getSchemaForPigStatement( Implementor implementor ) {
        final List<String> fieldNamesAndTypes = new ArrayList<>( getTable().getRowType().getFieldList().size() );
        for ( AlgDataTypeField f : getTable().getRowType().getFieldList() ) {
            fieldNamesAndTypes.add( getConcatenatedFieldNameAndTypeForPigSchema( implementor, f ) );
        }
        return String.join( ", ", fieldNamesAndTypes );
    }


    private String getConcatenatedFieldNameAndTypeForPigSchema( Implementor implementor, AlgDataTypeField field ) {
        final PigDataType pigDataType = PigDataType.valueOf( field.getType().getPolyType() );
        final String fieldName = implementor.getFieldName( this, field.getIndex() );
        return fieldName + ':' + DataType.findTypeName( pigDataType.getPigType() );
    }


    @Override
    public void register( AlgOptPlanner planner ) {
        planner.addRule( PigToEnumerableConverterRule.INSTANCE );
        for ( AlgOptRule rule : PigRules.ALL_PIG_OPT_RULES ) {
            planner.addRule( rule );
        }
        // Don't move Aggregates around, otherwise PigAggregate.implement() won't know how to correctly procuce Pig Latin
        planner.removeRule( AggregateExpandDistinctAggregatesRule.INSTANCE );
        // Make sure planner picks PigJoin over EnumerableJoin. Should there be a rule for this instead for removing ENUMERABLE_JOIN_RULE here?
        planner.removeRule( EnumerableRules.ENUMERABLE_JOIN_RULE );
    }

}
