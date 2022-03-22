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

package org.polypheny.db.algebra.core;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;

public abstract class Unwind extends SingleAlg {

    public final RexNode target;
    public final String alias;


    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input Input relational expression
     */
    protected Unwind( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, RexNode target, @Nullable String alias ) {
        super( cluster, traits, input );
        this.target = adjustIfNecessary( target );
        assert input.getRowType().getFieldList().size() == 1 : "Unwind is for now only able on a single field.";
        this.alias = alias;
    }


    protected RexNode adjustIfNecessary( RexNode target ) {
        if ( target.getType().getPolyType() == PolyType.ARRAY ) {
            return target;
        }
        return getCluster().getRexBuilder().makeCall(
                getCluster().getTypeFactory().createArrayType( getCluster().getTypeFactory().createPolyType( PolyType.VARCHAR, 255 ), -1 ),
                OperatorRegistry.get( QueryLanguage.CYPHER, OperatorName.CYPHER_TO_LIST ),
                List.of( target ) );
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName()
                + "$" + target.hashCode()
                + (alias != null ? "$As$" + alias : "")
                + "$" + input.algCompareString();
    }


    @Override
    protected AlgDataType deriveRowType() {
        List<AlgDataTypeField> fields = new ArrayList<>();
        fields.add( new AlgDataTypeFieldImpl( alias, 0, target.getType().getComponentType() ) );
        return new AlgRecordType( fields );
    }

}
