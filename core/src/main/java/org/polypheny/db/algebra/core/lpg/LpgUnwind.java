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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;


public abstract class LpgUnwind extends SingleAlg implements LpgAlg {

    public final String alias;
    public final int index;


    /**
     * Creates a {@link LpgUnwind}. The operator tries to "unpack" a value in a property if it is an array.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input Input relational expression
     */
    protected LpgUnwind( AlgCluster cluster, AlgTraitSet traits, AlgNode input, int index, @Nullable String alias ) {
        super( cluster, traits, adjustInputIfNecessary( input, index ) );
        assert this.input.getTupleType().getFieldCount() == 1 : "Unwind is for now only able on a single field.";

        this.index = 0; //adjustIfNecessary( target );
        this.alias = alias;
    }


    private static AlgNode adjustInputIfNecessary( AlgNode input, int index ) {
        if ( input.getTupleType().getFields().get( index ).getType().getPolyType() == PolyType.ARRAY ) {
            return input;
        }
        AlgDataTypeField field = input.getTupleType().getFields().get( index );
        RexNode ref = input.getCluster().getRexBuilder().makeInputRef( field.getType(), field.getIndex() );
        // we wrap the field in a to-list operation, which wraps single values as list, leaves lists and replaces null with an empty list
        AlgDataType arrayType = input.getCluster().getTypeFactory().createArrayType( ref.getType(), -1 );
        ref = input.getCluster().getRexBuilder().makeCall( arrayType, OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_TO_LIST ), List.of( ref ) );
        return new LogicalLpgProject( input.getCluster(), input.getTraitSet(), input, Collections.singletonList( ref ), Collections.singletonList( PolyString.of( field.getName() ) ) );
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName() + "$"
                + index + "$"
                + (alias != null ? "$As$" + alias : "") + "$"
                + input.algCompareString() + "&";
    }


    @Override
    protected AlgDataType deriveRowType() {
        List<AlgDataTypeField> fields = new ArrayList<>();
        fields.add( new AlgDataTypeFieldImpl( -1L, alias, 0, input.getTupleType().getFields().get( index ).getType().getComponentType() ) );
        return new AlgRecordType( fields );
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.UNWIND;
    }

}
