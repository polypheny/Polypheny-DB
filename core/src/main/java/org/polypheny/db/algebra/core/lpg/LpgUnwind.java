/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.algebra.core.lpg;

import java.util.List;
import javax.annotation.Nullable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;


public abstract class LpgUnwind extends SingleAlg implements LpgAlg {

    public final String alias;
    public final int index;


    /**
     * Expands one input field into a row per element and replaces that field with the alias.
     */
    protected LpgUnwind( AlgCluster cluster, AlgTraitSet traits, AlgNode input, int index, @Nullable String alias ) {
        super( cluster, traits, input );
        this.index = index;
        this.alias = alias;
    }


    @Override
    protected AlgDataType deriveRowType() {
        List<AlgDataTypeField> fields = input.getTupleType().getFields();
        Builder builder = getCluster().getTypeFactory().builder();
        for ( AlgDataTypeField field : fields ) {
            if ( field.getIndex() != index ) {
                builder.add( field );
            }
        }
        AlgDataType value = fields.get( index ).getType();
        AlgDataType element = value.getComponentType();
        AlgDataType nullableElement = getCluster().getTypeFactory().createTypeWithNullability( element == null ? value : element, true );
        return builder.add( alias, null, nullableElement ).build();
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.UNWIND;
    }


    @Override
    public boolean isDataModifying() {
        return false;
    }

}
