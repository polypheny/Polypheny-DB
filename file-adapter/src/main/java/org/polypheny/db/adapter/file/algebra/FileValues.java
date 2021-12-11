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

package org.polypheny.db.adapter.file.algebra;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.adapter.file.FileAlg;
import org.polypheny.db.adapter.file.Value;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexLiteral;


public class FileValues extends Values implements FileAlg {

    protected FileValues( AlgOptCluster cluster, AlgDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, AlgTraitSet traits ) {
        super( cluster, rowType, tuples, traits );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq );
    }


    @Override
    public void implement( final FileImplementor implementor ) {
        AlgRecordType recordType = (AlgRecordType) getRowType();
        if ( recordType.toString().equals( "RecordType(INTEGER ZERO)" ) ) {
            implementor.setBatchInsert( true );
            return;
        }
        List<String> columns = new ArrayList<>();
        for ( AlgDataTypeField type : recordType.getFieldList() ) {
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
