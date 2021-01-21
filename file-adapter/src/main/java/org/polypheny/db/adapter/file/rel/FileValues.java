/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.file.rel;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.adapter.file.FileRel;
import org.polypheny.db.adapter.file.Value;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rel.type.RelRecordType;
import org.polypheny.db.rex.RexLiteral;


public class FileValues extends Values implements FileRel {

    protected FileValues( RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traits ) {
        super( cluster, rowType, tuples, traits );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public void implement( final FileImplementor implementor ) {
        RelRecordType recordType = (RelRecordType) getRowType();
        if ( recordType.toString().equals( "RecordType(INTEGER ZERO)" ) ) {
            implementor.setBatchInsert( true );
            return;
        }
        List<String> columns = new ArrayList<>();
        for ( RelDataTypeField type : recordType.getFieldList() ) {
            columns.add( type.getKey() );
        }
        implementor.setColumnNames( columns );

        for ( ImmutableList<RexLiteral> literalList : tuples ) {
            Value[] row = new Value[literalList.size()];
            int i = 0;
            for ( RexLiteral literal : literalList.asList() ) {
                row[i] = new Value( i, literal.getValueForFileAdapter(), false );
                i++;
            }
            implementor.addInsertValue( row );
        }
    }

}
