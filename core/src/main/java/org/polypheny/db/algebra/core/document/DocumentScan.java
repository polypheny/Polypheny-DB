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

import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.PolyType;


public abstract class DocumentScan extends AbstractAlgNode implements DocumentAlg {

    @Getter
    private final AlgOptTable collection;


    /**
     * Creates a {@link DocumentScan}.
     * {@link org.polypheny.db.schema.ModelTrait#DOCUMENT} node, which scans the content of a collection.
     */
    public DocumentScan( AlgOptCluster cluster, AlgTraitSet traitSet, AlgOptTable collection ) {
        super( cluster, traitSet );
        this.collection = collection;

        AlgDataType docType = cluster.getTypeFactory().createPolyType( PolyType.DOCUMENT );
        // todo dl: change after RowType refactor
        if ( this.collection.getTable().getSchemaType() == NamespaceType.DOCUMENT ) {
            this.rowType = new AlgRecordType( List.of( new AlgDataTypeFieldImpl( "d", 0, docType ) ) );
        } else {
            List<AlgDataTypeField> list = collection.getRowType().getFieldList().stream()
                    .map( f -> new AlgDataTypeFieldImpl( f.getName(), f.getIndex(), cluster.getTypeFactory().createPolyType( PolyType.ANY ) ) )
                    .collect( Collectors.toList() );
            this.rowType = new AlgRecordType( list );
        }
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() + "$" + collection.getTable().getTableId() + "$";
    }


    @Override
    public DocType getDocType() {
        return DocType.SCAN;
    }

}
