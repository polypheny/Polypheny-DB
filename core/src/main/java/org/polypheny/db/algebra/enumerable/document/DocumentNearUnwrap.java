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

package org.polypheny.db.algebra.enumerable.document;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgFieldCollation.Direction;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.document.DocumentFilter;
import org.polypheny.db.algebra.enumerable.EnumerableCalc;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.document.DocumentProjectToCalcRule.NearDetector;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentSort;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.*;
import org.polypheny.db.schema.document.DocumentUtil;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.spatial.InvalidGeometryException;
import org.polypheny.db.type.entity.spatial.PolyGeometry;
import org.polypheny.db.util.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;

import static org.polypheny.db.type.entity.spatial.PolyGeometry.WGS_84;

/**
 * The $near function cannot be executed as a single function, which is why this conversion
 * rule will replace the operation by multiple operations:
 *
 * 1. Projection: Add computed distance field
 * 2. Filter:     Filter out documents based on minDistance / maxDistance
 * 3. Sort:       Sort the resulting documents in ascending order by computed distance.
 * 4. Projection  Remove the computed distance field again.
 *
 * This conversion is done in the planner in instead of in the MqlToAlgConverter, so
 * that we can only apply it if the operation is executed internally in Polypheny. This way,
 * it is easier to translate the function when offloading to MongoDB.
 *
 * Other rules will be blocked by the {@link NearDetector} to force this plan.
 */
public class DocumentNearUnwrap extends ConverterRule {

    public static final DocumentNearUnwrap INSTANCE = new DocumentNearUnwrap();


    public DocumentNearUnwrap() {
        super( DocumentFilter.class, DocumentNearUnwrap::supports, Convention.NONE, Convention.NONE, AlgFactories.LOGICAL_BUILDER, DocumentNearUnwrap.class.getSimpleName() );
    }


    private AlgCluster cluster;

    public static boolean supports( DocumentFilter filter ) {
        Kind nearKind = filter.getCondition().getKind();
        return nearKind == Kind.MQL_NEAR || nearKind == Kind.MQL_NEAR_SPHERE;
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        if ( !(alg instanceof DocumentFilter filter) ) {
            throw new GenericRuntimeException( "todo" );
        }
        cluster = alg.getCluster();
        RexCall nearCall = (RexCall) filter.getCondition();
        AlgDataType rowType = alg.getTupleType();
        assert nearCall.operands.size() == 4;
        RexNameRef input = (RexNameRef) nearCall.operands.get( 0 );
        RexLiteral geometry = (RexLiteral) nearCall.operands.get( 1 );
        RexLiteral minDistance = (RexLiteral) nearCall.operands.get( 2 );
        RexLiteral maxDistance = (RexLiteral) nearCall.operands.get( 3 );

        //
        // Step 2:
        // Filter by minDistance, maxDistance
        final String distanceField = "__temp_%s".formatted( UUID.randomUUID().toString() );

        Map<String, RexNode> adds = new HashMap<>();
        adds.put( distanceField, getFixedCall( List.of(
                input,
                geometry,
                convertLiteral( new PolyInteger( 1 ) )
        ), OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_GEO_DISTANCE ), PolyType.ANY ) );
        AlgNode replacementNode = LogicalDocumentProject.create( filter.getInput(), Map.of(), List.of(), adds );
        replacementNode.getTupleType();

        //
        // Step 2:
        // Filter by minDistance, maxDistance
        List<RexNode> filterNodes = new ArrayList<>();
        if ( minDistance.getValue().asNumber().intValue() != -1 ) {
            filterNodes.add(
                    getFixedCall(
                            List.of(
                                    new RexNameRef( List.of( "_distance" ), null, DocumentType.ofDoc() ),
                                    convertLiteral( minDistance.getValue() ) ),
                            OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_GTE ),
                            PolyType.BOOLEAN ) );
        }
        if ( maxDistance.getValue().asNumber().intValue() != -1 ) {
            filterNodes.add(
                    getFixedCall(
                            List.of(
                                    new RexNameRef( List.of( "_distance" ), null, DocumentType.ofDoc() ),
                                    convertLiteral( maxDistance.getValue() ) ),
                            OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_LTE ),
                            PolyType.BOOLEAN ) );
        }
        if ( !filterNodes.isEmpty() ) {
            RexNode filterCondition = getFixedCall(
                    filterNodes,
                    OperatorRegistry.get( OperatorName.AND ),
                    PolyType.BOOLEAN
            );
            replacementNode = LogicalDocumentFilter.create( replacementNode, filterCondition );
            replacementNode.getTupleType();
        }

        //
        // Step 3:
        // Sort by _distance ascending
        List<String> names = List.of( "_distance" );
        replacementNode = LogicalDocumentSort.create(
                replacementNode,
                AlgCollations.of( generateCollation( List.of( Direction.ASCENDING ), names, names ) ),
                List.of( new RexNameRef( List.of( "_distance" ), null, DocumentType.ofDoc() ) ),
                null,
                null );
        replacementNode.getTupleType();

        //
        // Step 4:
        // Projection to remove field _distance
        replacementNode = LogicalDocumentProject.create( replacementNode, Map.of(), List.of( distanceField ), Map.of() );
        replacementNode.getTupleType();

        return replacementNode;
    }


    private List<AlgFieldCollation> generateCollation( List<Direction> dirs, List<String> names, List<String> rowNames ) {
        List<AlgFieldCollation> collations = new ArrayList<>();
        int pos = 0;
        int index;
        for ( String name : names ) {
            index = rowNames.indexOf( name );
            collations.add( new AlgFieldCollation( index, dirs.get( pos ) ) );
            pos++;
        }
        return collations;
    }


    private RexNode convertLiteral( PolyValue polyValue ) {
        Pair<PolyValue, PolyType> valuePair = RexLiteral.convertType( polyValue, new DocumentType() );
        return new RexLiteral( valuePair.left, new DocumentType(), valuePair.right );
    }


    private RexNode getFixedCall( List<RexNode> operands, Operator op, PolyType polyType ) {
        if ( operands.size() == 1 ) {
            if ( op.getKind() == Kind.NOT && operands.get( 0 ) instanceof RexCall && ((RexCall) operands.get( 0 )).op.getKind() == Kind.NOT ) {
                // we have a nested NOT, which can be removed
                return ((RexCall) operands.get( 0 )).operands.get( 0 );
            }

            return operands.get( 0 );
        } else {
            List<RexNode> toRemove = new ArrayList<>();
            List<RexNode> toAdd = new ArrayList<>();
            // maybe we have to fix nested AND or OR combinations
            for ( RexNode operand : operands ) {
                if ( operand instanceof RexCall && ((RexCall) operand).op.getName().equals( op.getName() ) ) { // TODO DL maybe remove if not longer same type
                    toAdd.addAll( ((RexCall) operand).operands );
                    toRemove.add( operand );
                }
            }
            if ( !toAdd.isEmpty() ) {
                operands.addAll( toAdd );
                operands.removeAll( toRemove );
            }

            return new RexCall( cluster.getTypeFactory().createTypeWithNullability( cluster.getTypeFactory().createPolyType( polyType ), true ), op, operands );
        }
    }

}
