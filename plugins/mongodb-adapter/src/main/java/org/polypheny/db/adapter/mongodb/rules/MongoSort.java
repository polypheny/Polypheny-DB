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

package org.polypheny.db.adapter.mongodb.rules;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.mongodb.MongoAlg;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link Sort} relational expression in MongoDB.
 */
public class MongoSort extends Sort implements MongoAlg {

    public MongoSort( AlgCluster cluster, AlgTraitSet traitSet, AlgNode child, AlgCollation collation, @Nullable List<RexNode> fieldExpr, RexNode offset, RexNode fetch ) {
        super( cluster, traitSet, child, collation, fieldExpr, offset, fetch );
        assert getConvention() == CONVENTION;
        assert getConvention() == child.getConvention();
    }


    @Override
    public Sort copy( AlgTraitSet traitSet, AlgNode newInput, AlgCollation newCollation, ImmutableList<RexNode> fieldExps, RexNode offset, RexNode fetch ) {
        return new MongoSort( getCluster(), traitSet, newInput, collation, fieldExps, offset, fetch );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.05 );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.visitChild( 0, getInput() );
        if ( !collation.getFieldCollations().isEmpty() ) {
            final List<String> keys = new ArrayList<>();
            final List<AlgDataTypeField> fields = getTupleType().getFields();
            for ( AlgFieldCollation fieldCollation : collation.getFieldCollations() ) {
                // we can only sort by field not by db.collection.field
                String name =
                        fields.get( fieldCollation.getFieldIndex() ).getName();
                String[] splits = name.split( "\\." );
                name = splits[splits.length - 1];
                name = MongoRules.adjustName( name );
                keys.add( name + ": " + direction( fieldCollation ) );
            }
            implementor.add( null, "{$sort: " + Util.toString( keys, "{", ", ", "}" ) + "}" );
        }
        if ( offset != null ) {
            implementor.add( null, "{$skip: " + ((RexLiteral) offset).getValue().toJson() + "}" );
        }

        if ( fetch != null && fetch instanceof RexLiteral literal ) {
            if ( implementor.isDML() && literal.value.asNumber().intValue() == 1 ) {
                implementor.onlyOne = true;
            } else {
                implementor.add( null, "{$limit: " + literal.value.toJson() + "}" );
            }
        }
    }


    private int direction( AlgFieldCollation fieldCollation ) {
        return switch ( fieldCollation.getDirection() ) {
            case DESCENDING, STRICTLY_DESCENDING -> -1;
            default -> 1;
        };
    }

}

