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
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
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
    private RexBuilder builder;
    private final Optional<RexNode> subElement = Optional.empty();
    private Entity entity;

    public static boolean supports(DocumentFilter filter) {
        Kind nearKind = filter.getCondition().getKind();
        return nearKind == Kind.MQL_NEAR || nearKind == Kind.MQL_NEAR_SPHERE;
    }

    @Override
    public AlgNode convert( AlgNode alg ) {
        if (!(alg instanceof DocumentFilter filter)) {
            throw new GenericRuntimeException("todo");
        }

        cluster = alg.getCluster();
        builder = cluster.getRexBuilder();
        entity = alg.getEntity();
        RexCall nearCall = (RexCall)filter.getCondition();

        assert nearCall.operands.size() == 4;
        RexNameRef input = (RexNameRef) nearCall.operands.get( 0 );
        RexLiteral geometry = (RexLiteral) nearCall.operands.get( 1 );
        RexLiteral minDistance = (RexLiteral) nearCall.operands.get( 2 );
        RexLiteral maxDistance = (RexLiteral) nearCall.operands.get( 3 );

        // 1. Add distance field with projection.
        final String distanceField = "__temp_%s".formatted( UUID.randomUUID().toString() );
//        final boolean isSpherical = nearKind == Kind.MQL_NEAR_SPHERE;
//        AlgDataType rowType = alg.getTupleType();


        Map<String, RexNode> adds = new HashMap<>();
        adds.put( distanceField, getFixedCall( List.of(
                input,
                geometry,
                convertLiteral( new BsonInt32( 1 ) )
        ), OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_GEO_DISTANCE ), PolyType.ANY ) );
        AlgNode replacementNode = LogicalDocumentProject.create( filter.getInput(), Map.of(), List.of(), adds );
        replacementNode.getTupleType();

        // 2. Add filter for minDistance, maxDistance
//        if (minDistance.getValue().asNumber().intValue() != -1){
//            // TODO
//        }


        // Copied from DocumentFilterToCalcRule
//        final LogicalDocumentFilter filter = (LogicalDocumentFilter) alg;
//        final AlgNode input = filter.getInput();
//        // Create a program containing a filter.
//        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
//        final AlgDataType inputRowType = input.getTupleType();
//        NameRefReplacer replacer = new NameRefReplacer( filter.getCluster(), false );
//        programBuilder.addCondition( filter.condition.accept( replacer ) );

//        final RexProgramBuilder programBuilder = new RexProgramBuilder( replacementNode.getTupleType(), builder );
//        programBuilder.addIdentity();
//        final RexProgram program = programBuilder.getProgram();
//        return EnumerableCalc.create( convert( replacementNode, replacementNode.getTraitSet().replace( EnumerableConvention.INSTANCE ) ), program );

        return replacementNode;
    }

    private RexNode convertDistance( BsonValue bsonValue, boolean isSpherical, AlgDataType rowType ) {
        BsonArray bsonArray = bsonValue.asArray();
        List<RexNode> operands = new ArrayList<>();
        assert bsonArray.size() == 3;
        BsonValue distanceField = bsonArray.get( 0 );
        BsonValue coordinates = bsonArray.get( 1 );
        BsonValue distanceMultiplier = bsonArray.get( 2 );

        // Reference to field from document
        operands.add( getIdentifier( distanceField.asString().getValue().substring( 1 ), rowType, false ) );

        PolyGeometry polyGeometry;
        if ( coordinates.isDocument() ) {
            BsonDocument geometry = coordinates.asDocument();
            try {
                polyGeometry = PolyGeometry.fromGeoJson( geometry.toJson() );
            } catch ( InvalidGeometryException e ) {
                throw new RuntimeException( e );
            }
        } else if ( coordinates.isArray() ) {
            GeometryFactory geoFactory = isSpherical
                    ? new GeometryFactory( new PrecisionModel(), WGS_84 )
                    : new GeometryFactory();
            Coordinate point = convertArrayToCoordinate( coordinates.asArray() );
            polyGeometry = new PolyGeometry( geoFactory.createPoint( point ) );
        } else {
            throw new GenericRuntimeException( "$near supports either a legacy coordinate pair of the form [x, y] or a $geometry object." );
        }
        // Geometry from filter
        operands.add( convertGeometry( polyGeometry ) );

        operands.add( convertLiteral( distanceMultiplier ) );

        return getFixedCall( operands, OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_GEO_DISTANCE ), PolyType.ANY );
    }

    private RexNode getIdentifier( String parentKey, AlgDataType rowType, boolean useAccess ) {
        if ( useAccess ) {
            return attachAccess( parentKey, rowType );
        }

        // we look if we already extracted a part of the document

        return translateDocValue( null, parentKey );
    }

    public RexNode translateDocValue( @Nullable Integer index, String key ) {
        //RexCall filter;
        List<String> names = Arrays.asList( key.split( "\\." ) );

        return subElement.orElseGet( () -> new RexNameRef( names, index, DocumentType.ofDoc() ) );
    }

    private RexNode attachAccess( String parentKey, AlgDataType rowType ) {
        AlgDataTypeField field = rowType.getField( parentKey, false, false );
        return attachAccess( field.getIndex(), rowType );
    }

    private RexNode attachAccess( int index, AlgDataType rowType ) {
        CorrelationId correlId = cluster.createCorrel();
        cluster.getMapCorrelToAlg().put( correlId, LogicalDocumentScan.create( cluster, entity ) );
        return builder.makeFieldAccess( builder.makeCorrel( rowType, correlId ), index );
    }

    private PolyValue getPolyValue( BsonValue value ) {
        switch ( value.getBsonType() ) {
            case DOUBLE:
                return PolyDouble.of( value.asDouble().getValue() );
            case STRING:
                return PolyString.of( value.asString().getValue() );
            case DOCUMENT:
                Map<PolyString, PolyValue> map = new HashMap<>();
                for ( Entry<String, BsonValue> entry : value.asDocument().entrySet() ) {
                    map.put( PolyString.of( entry.getKey() ), getPolyValue( entry.getValue() ) );
                }

                return PolyDocument.ofDocument( map );
            case ARRAY:
                List<PolyValue> list = new ArrayList<>();
                for ( BsonValue bson : value.asArray() ) {
                    list.add( getPolyValue( bson ) );
                }
                return PolyList.of( list );
            case BOOLEAN:
                return new PolyBoolean( value.asBoolean().getValue() );
            case INT32:
                return new PolyInteger( value.asInt32().getValue() );
        }
        throw new GenericRuntimeException( "Not implemented Comparable transform: " + value );
    }

    private RexNode convertLiteral( BsonValue bsonValue ) {
        Pair<PolyValue, PolyType> valuePair = RexLiteral.convertType( getPolyValue( bsonValue ), new DocumentType() );
        return new RexLiteral( valuePair.left, new DocumentType(), valuePair.right );
    }


    private RexNode convertGeometry( PolyGeometry geometry ) {
        Pair<PolyValue, PolyType> valuePair = RexLiteral.convertType( geometry, new DocumentType() );
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

    private Coordinate convertArrayToCoordinate( BsonArray array ) {
        if ( array.size() != 2 ) {
            throw new GenericRuntimeException( "Coordinates need to be of the form [x,y]" );
        }
        double x = convertBsonValueToDouble( array.get( 0 ) );
        double y = convertBsonValueToDouble( array.get( 1 ) );
        return new Coordinate( x, y );
    }

    private double convertBsonValueToDouble( BsonValue bsonValue ) {
        Double result = null;
        if ( bsonValue.isDouble() ) {
            result = bsonValue.asDouble().getValue();
        }
        if ( bsonValue.isInt32() ) {
            int intValue = bsonValue.asInt32().getValue();
            result = (double) intValue;
        }
        if ( bsonValue.isInt64() ) {
            long intValue = bsonValue.asInt64().getValue();
            result = (double) intValue;
        }
        if ( result == null ) {
            throw new GenericRuntimeException( "Legacy Coordinates needs to be of type INTEGER or DOUBLE." );
        }
        return result;
    }


}
