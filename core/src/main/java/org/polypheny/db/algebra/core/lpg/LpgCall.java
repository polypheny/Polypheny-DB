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

import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import java.util.ArrayList;

public abstract class LpgCall extends AbstractAlgNode implements LpgAlg {

    @Getter
    ArrayList<String> namespace;
    @Getter
    String procedureName;
    @Getter
    ArrayList<PolyValue> arguments;
    @Setter
    @Getter
    Adapter procedureProvider; // will only be used when we reach a Neo4j store, so normally just pass a  null to it

    @Getter
    boolean yieldAll;
    @Getter
    ArrayList<String> yieldItems;

    public LpgCall( AlgCluster cluster, AlgTraitSet traits, ArrayList<String> namespace, String procedureName, ArrayList<PolyValue> arguments, Adapter procedureProvider, boolean yieldAll, ArrayList<String> yieldItems ) {
        super( cluster, traits );
        this.namespace = namespace;
        this.procedureName = procedureName;
        this.arguments = arguments;
        this.procedureProvider = procedureProvider;
        this.yieldAll = yieldAll;
        this.yieldItems = yieldItems;
    }

    @Override
    public String algCompareString() {
        return getClass().getSimpleName().toString() + "$" + String.join( ".", namespace )
                + "." + procedureName + "$" + arguments.hashCode();
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.CALL;
    }

    @Override
    protected AlgDataType deriveRowType() {
        if (!(procedureName.equals( "labels" ))) {
            throw new UnsupportedOperationException("The called procedure is not supported");
        }
        ArrayList<AlgDataTypeField> fields = new ArrayList<>();
        fields.add( new AlgDataTypeFieldImpl( -1L, "Labels", 0, getCluster().getTypeFactory().createPolyType( PolyType.VARCHAR ) ) );
        return new AlgRecordType( fields );
    }

}
