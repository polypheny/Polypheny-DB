/*
 * Copyright 2019-2026 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.algebra;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl.JavaType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.nodes.IntervalQualifierImpl;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.BasicPolyType;
import org.polypheny.db.type.IntervalPolyType;
import org.polypheny.db.type.MapPolyType;
import org.polypheny.db.type.MultisetPolyType;
import org.polypheny.db.type.PathType;
import org.polypheny.db.type.VectorType;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyInterval;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolySymbol;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyTemporal;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.graph.PolyPath.PolySegment;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.ByteString;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Pair;

final class PlanKeyComponents {

    private PlanKeyComponents() {
    }


    static List<Object> snapshot( Collection<?> components ) {
        List<Object> snapshot = new ArrayList<>( components.size() );
        for ( Object component : components ) {
            Object value = snapshot( component );
            if ( value == null ) {
                return null;
            }
            snapshot.add( value );
        }
        return List.copyOf( snapshot );
    }


    private static Object snapshot( Object value ) {
        if ( value == null ) {
            return NullComponent.INSTANCE;
        }
        Class<?> type = value.getClass();
        if ( value instanceof PlanKey || value instanceof Enum<?> || value instanceof Class<?> || value instanceof String
                || value instanceof Boolean || value instanceof Character || value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long || type == BigInteger.class || type == BigDecimal.class
                || type == ByteString.class || value == Convention.NONE ) {
            return value;
        }
        if ( value instanceof Double number ) {
            return rawDouble( number );
        }
        if ( value instanceof Float number ) {
            return rawFloat( number );
        }
        if ( value instanceof RexNode rex ) {
            return expression( rex );
        }
        if ( value instanceof PolyValue polyValue ) {
            return polyValue( polyValue );
        }
        if ( value instanceof AlgDataType dataType ) {
            return dataType( dataType );
        }
        if ( value instanceof Snapshot snapshot ) {
            // A rollback can reuse a numeric snapshot ID for a different catalog snapshot.
            return new IdentityComponent( snapshot );
        }
        if ( value instanceof Operator operator ) {
            // Registered built-in operator instances carry the actual implementation and configuration.
            return OperatorRegistry.isRegistered( operator ) ? new RegisteredOperator( new IdentityComponent( operator ), operator.getOperatorName() ) : null;
        }
        if ( value instanceof AlgTraitSet traits ) {
            return traitSet( traits );
        }
        if ( value instanceof List<?> list ) {
            return snapshot( list );
        }
        if ( value instanceof Map<?, ?> map ) {
            // Some operators use the entry iteration order to construct the output row.
            return tagged( Map.class, map.entrySet().stream().map( e -> Pair.of( e.getKey(), e.getValue() ) ).toList() );
        }
        if ( value instanceof Set<?> set ) {
            return tagged( Set.class, set.toArray() );
        }
        if ( value instanceof byte[] bytes ) {
            return new Component( byte[].class, List.of( new ByteString( bytes ) ) );
        }
        if ( type == Pair.class && value instanceof Pair<?, ?> pair ) {
            return tagged( type, pair.left, pair.right );
        }
        return metadata( value );
    }


    private static Object traitSet( AlgTraitSet traits ) {
        List<Object> slots = new ArrayList<>( traits.size() );
        for ( int i = 0; i < traits.size(); i++ ) {
            AlgTrait<?> trait = traits.getTrait( i );
            slots.add( trait != null && trait.getTraitDef() == AlgCollationTraitDef.INSTANCE ? traits.getTraits( i ) : trait );
        }
        return tagged( AlgTraitSet.class, slots );
    }


    private static Object expression( RexNode rex ) {
        Class<?> type = rex.getClass();
        if ( type == RexLiteral.class && rex instanceof RexLiteral literal ) {
            return tagged( type, literal.getType(), literal.polyType, literal.value );
        }
        if ( type == RexCall.class && rex instanceof RexCall call ) {
            return tagged( type, call.getType(), call.getOperator(), call.getOperands() );
        }
        if ( type == RexDynamicParam.class && rex instanceof RexDynamicParam parameter ) {
            return tagged( type, parameter.getType(), parameter.getIndex() );
        }
        if ( type == RexIndexRef.class && rex instanceof RexIndexRef reference ) {
            return tagged( type, reference.getType(), reference.getIndex() );
        }
        if ( type == RexLocalRef.class && rex instanceof RexLocalRef reference ) {
            return tagged( type, reference.getType(), reference.getIndex() );
        }
        if ( type == RexNameRef.class && rex instanceof RexNameRef reference ) {
            return tagged( type, reference.getType(), reference.getName(), reference.names, reference.getIndex().orElse( null ) );
        }
        if ( type == RexCorrelVariable.class && rex instanceof RexCorrelVariable reference ) {
            return tagged( type, reference.getType(), reference.id );
        }
        if ( type == RexFieldAccess.class && rex instanceof RexFieldAccess access ) {
            return tagged( type, access.getReferenceExpr(), access.getField() );
        }
        return null;
    }


    private static Object dataType( AlgDataType dataType ) {
        Class<?> type = dataType.getClass();
        if ( type == BasicPolyType.class ) {
            return tagged( type, dataType.getPolyType(), dataType.isNullable(), dataType.getRawPrecision(), dataType.getPrecision(),
                    dataType.getScale(), dataType.getCharset(), dataType.getCollation() );
        }
        if ( type == AlgRecordType.class || type == GraphType.class || type == PathType.class ) {
            return tagged( type, dataType.getStructKind(), dataType.isNullable(), dataType.getFields() );
        }
        if ( type == DocumentType.class && dataType instanceof DocumentType document ) {
            return tagged( type, document.structKind, document.fixed, document.excluded, document.hasFixedSchema(), document.physicalName );
        }
        if ( type == ArrayType.class && dataType instanceof ArrayType array ) {
            return tagged( type, array.isNullable(), array.getComponentType(), array.getMaxCardinality(), array.getMaxDimension() );
        }
        if ( type == VectorType.class && dataType instanceof VectorType vector ) {
            return tagged( type, vector.isNullable(), vector.getComponentType(), vector.getVectorDimension(), vector.getMaxDimension(), vector.getVectorElementType() );
        }
        if ( type == MapPolyType.class && dataType instanceof MapPolyType map ) {
            return tagged( type, map.isNullable(), map.getKeyType(), map.getValueType() );
        }
        if ( type == MultisetPolyType.class ) {
            return tagged( type, dataType.isNullable(), dataType.getComponentType() );
        }
        if ( type == IntervalPolyType.class ) {
            return tagged( type, dataType.isNullable(), dataType.getPrecision(), dataType.getScale(), dataType.getIntervalQualifier() );
        }
        if ( type == JavaType.class && dataType instanceof JavaType javaType ) {
            return tagged( type, javaType.getJavaClass(), javaType.isNullable(), javaType.getCharset(), javaType.getCollation(),
                    javaType.isStruct() ? javaType.getFields() : List.of() );
        }
        return null;
    }


    private static Object metadata( Object value ) {
        Class<?> type = value.getClass();
        if ( type == AlgDataTypeFieldImpl.class && value instanceof AlgDataTypeField field ) {
            return tagged( type, field.getId(), field.getIndex(), field.getName(), field.getPhysicalName(), field.getType(), field.isDynamicStar() );
        }
        if ( type == AlgFieldCollation.class && value instanceof AlgFieldCollation collation ) {
            return tagged( type, collation.getFieldIndex(), collation.direction, collation.nullDirection );
        }
        if ( type == AlgCollationImpl.class && value instanceof AlgCollation collation ) {
            return tagged( type, collation.getFieldCollations() );
        }
        if ( type == AlgDistributions.ANY.getClass() && value instanceof AlgDistribution distribution ) {
            return tagged( type, distribution.getType(), distribution.getKeys() );
        }
        if ( value instanceof ModelTrait model ) {
            return tagged( type, model.dataModel() );
        }
        if ( value instanceof ImmutableBitSet bits ) {
            return tagged( type, bits.asList() );
        }
        if ( type == CorrelationId.class && value instanceof CorrelationId correlation ) {
            return tagged( type, correlation.getId(), correlation.getName() );
        }
        if ( type == Collation.class && value instanceof Collation collation ) {
            return tagged( type, collation.getCollationName(), collation.getCoercibility() );
        }
        if ( value instanceof Charset charset && type.getClassLoader() == null ) {
            return tagged( Charset.class, type, charset.name() );
        }
        if ( type == IntervalQualifierImpl.class && value instanceof IntervalQualifier qualifier ) {
            return tagged( type, qualifier.getTimeUnitRange(), qualifier.getStartPrecisionPreservingDefault(),
                    qualifier.getFractionalSecondPrecisionPreservingDefault() );
        }
        return null;
    }


    private static Object polyValue( PolyValue value ) {
        Class<?> type = value.getClass();
        if ( type == PolyNull.class ) {
            return tagged( type );
        }
        if ( type == PolyBoolean.class && value instanceof PolyBoolean bool ) {
            return tagged( type, bool.value );
        }
        if ( type == PolyString.class && value instanceof PolyString string ) {
            return tagged( type, string.value, string.charset );
        }
        if ( type == PolyBinary.class && value instanceof PolyBinary binary ) {
            return tagged( type, binary.value, binary.count );
        }
        if ( type == PolyInteger.class && value instanceof PolyInteger number ) {
            return tagged( type, value.getType(), number.value );
        }
        if ( type == PolyLong.class && value instanceof PolyLong number ) {
            return tagged( type, value.getType(), number.value );
        }
        if ( type == PolyBigDecimal.class && value instanceof PolyBigDecimal number ) {
            return tagged( type, number.value );
        }
        if ( type == PolyDouble.class && value instanceof PolyDouble number ) {
            return tagged( type, number.value );
        }
        if ( type == PolyFloat.class && value instanceof PolyFloat number ) {
            return tagged( type, number.value );
        }
        if ( (type == PolyDate.class || type == PolyTime.class || type == PolyTimestamp.class) && value instanceof PolyTemporal temporal ) {
            return tagged( type, temporal.getMillisSinceEpoch() );
        }
        if ( type == PolyInterval.class && value instanceof PolyInterval interval ) {
            return tagged( type, interval.millis, interval.months );
        }
        if ( type == PolySymbol.class && value instanceof PolySymbol symbol ) {
            return tagged( type, symbol.value, symbol.object );
        }
        if ( type == PolyList.class && value instanceof PolyList<?> list ) {
            return tagged( type, value.getType(), list.value );
        }
        if ( type == PolyDocument.class && value instanceof PolyDocument document ) {
            return tagged( type, document.map, document.isUnset );
        }
        if ( (type == PolyMap.class || type == PolyDictionary.class) && value instanceof PolyMap<?, ?> map ) {
            return tagged( type, value.getType(), map.mapType, map.map );
        }
        return graphValue( value );
    }


    private static Object graphValue( PolyValue value ) {
        Class<?> type = value.getClass();
        if ( type == PolyNode.class && value instanceof PolyNode node ) {
            return tagged( type, node.id, node.variableName, node.labels, node.properties, node.isVariable() );
        }
        if ( type == PolyEdge.class && value instanceof PolyEdge edge ) {
            return tagged( type, edge.id, edge.variableName, edge.labels, edge.properties, edge.left, edge.right, edge.direction, edge.fromTo );
        }
        if ( type == PolyPath.class && value instanceof PolyPath path ) {
            return tagged( type, path.id, path.variableName, path.getNodes(), path.getEdges(), path.getNames(), path.getPath(), path.getSegments() );
        }
        if ( type == PolyGraph.class && value instanceof PolyGraph graph ) {
            return tagged( type, graph.id, graph.variableName, graph.getNodes(), graph.getEdges() );
        }
        if ( type == PolySegment.class && value instanceof PolySegment segment ) {
            return tagged( type, segment.id, segment.variableName, segment.sourceId, segment.edgeId, segment.targetId,
                    segment.source, segment.edge, segment.target, segment.isRef, segment.direction );
        }
        return null;
    }


    private static Component tagged( Class<?> type, Object... components ) {
        List<Object> snapshot = snapshot( Arrays.asList( components ) );
        return snapshot == null ? null : new Component( type, snapshot );
    }


    private static Component rawDouble( double value ) {
        return new Component( Double.class, List.of( Double.doubleToRawLongBits( value ) ) );
    }


    private static Component rawFloat( float value ) {
        return new Component( Float.class, List.of( Float.floatToRawIntBits( value ) ) );
    }


    private enum NullComponent {
        INSTANCE
    }


    private record Component( Class<?> type, List<Object> attributes ) {
    }


    private record RegisteredOperator( IdentityComponent operator, OperatorName name ) {

    }


    private record IdentityComponent( Object reference ) {

        @Override
        public boolean equals( Object other ) {
            return other instanceof IdentityComponent component && reference == component.reference;
        }


        @Override
        public int hashCode() {
            return System.identityHashCode( reference );
        }

    }

}
