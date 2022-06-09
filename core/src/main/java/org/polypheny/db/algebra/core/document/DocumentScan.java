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

package org.polypheny.db.algebra.core.document;

import lombok.Getter;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;

public abstract class DocumentScan extends AbstractAlgNode implements DocumentAlg {

    @Getter
    private final AlgOptTable document;


    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     * @param cluster
     * @param traitSet
     */
    public DocumentScan( AlgOptCluster cluster, AlgTraitSet traitSet, AlgOptTable document ) {
        super( cluster, traitSet );
        this.document = document;
        this.rowType = document.getRowType();//new AlgRecordType( List.of( new AlgDataTypeFieldImpl( "d", 0, cluster.getTypeFactory().createPolyType( PolyType.DOCUMENT ) ) ) );
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() + "$" + document.getTable().getTableId() + "$";
    }


    @Override
    public DocType getDocType() {
        return DocType.SCAN;
    }


}
