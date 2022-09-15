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

package org.polypheny.db.prepare;


import com.google.common.collect.Lists;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.java.Array;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.runtime.Unit;
import org.polypheny.db.runtime.functions.GeoFunctions;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyGraph;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.schema.graph.PolyPath;
import org.polypheny.db.type.BasicPolyType;
import org.polypheny.db.type.IntervalPolyType;
import org.polypheny.db.type.JavaToPolyTypeConversionRules;
import org.polypheny.db.type.PathType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link JavaTypeFactory}.
 *
 * <strong>NOTE: This class is experimental and subject to change/removal without notice</strong>.</p>
 */
public class JavaTypeFactoryImpl extends PolyTypeFactoryImpl implements JavaTypeFactory {

    private final Map<List<Pair<Type, Boolean>>, SyntheticRecordType> syntheticTypes = new HashMap<>();


    public JavaTypeFactoryImpl() {
        this( AlgDataTypeSystem.DEFAULT );
    }


    public JavaTypeFactoryImpl( AlgDataTypeSystem typeSystem ) {
        super( typeSystem );
    }


    @Override
    public AlgDataType createStructType( Class type ) {
        final List<AlgDataTypeField> list = new ArrayList<>();
        for ( Field field : type.getFields() ) {
            if ( !Modifier.isStatic( field.getModifiers() ) ) {
                // FIXME: watch out for recursion
                final Type fieldType = fieldType( field );
                list.add( new AlgDataTypeFieldImpl( field.getName(), list.size(), createType( fieldType ) ) );
            }
        }
        return canonize( new JavaRecordType( list, type ) );
    }


    /**
     * Returns the type of a field.
     *
     * Takes into account {@link Array} annotations if present.
     */
    private Type fieldType( Field field ) {
        final Class<?> klass = field.getType();
        final Array array = field.getAnnotation( Array.class );
        if ( array != null ) {
            return new Types.ArrayType( array.component(), array.componentIsNullable(), array.maximumCardinality() );
        }
        final org.polypheny.db.adapter.java.Map map = field.getAnnotation( org.polypheny.db.adapter.java.Map.class );
        if ( map != null ) {
            return new Types.MapType( map.key(), map.keyIsNullable(), map.value(), map.valueIsNullable() );
        }
        return klass;
    }


    @Override
    public AlgDataType createType( Type type ) {
        if ( type instanceof AlgDataType ) {
            return (AlgDataType) type;
        }
        if ( type instanceof SyntheticRecordType ) {
            final SyntheticRecordType syntheticRecordType = (SyntheticRecordType) type;
            return syntheticRecordType.algType;
        }
        if ( type instanceof Types.ArrayType ) {
            final Types.ArrayType arrayType = (Types.ArrayType) type;
            final AlgDataType componentRelType = createType( arrayType.getComponentType() );
            return createArrayType( createTypeWithNullability( componentRelType, arrayType.componentIsNullable() ), arrayType.maximumCardinality() );
        }
        if ( type instanceof Types.MapType ) {
            final Types.MapType mapType = (Types.MapType) type;
            final AlgDataType keyRelType = createType( mapType.getKeyType() );
            final AlgDataType valueRelType = createType( mapType.getValueType() );
            return createMapType(
                    createTypeWithNullability( keyRelType, mapType.keyIsNullable() ),
                    createTypeWithNullability( valueRelType, mapType.valueIsNullable() ) );
        }
        if ( !(type instanceof Class) ) {
            throw new UnsupportedOperationException( "TODO: implement " + type );
        }
        final Class clazz = (Class) type;
        switch ( Primitive.flavor( clazz ) ) {
            case PRIMITIVE:
                return createJavaType( clazz );
            case BOX:
                return createJavaType( Primitive.ofBox( clazz ).boxClass );
        }
        if ( JavaToPolyTypeConversionRules.instance().lookup( clazz ) != null ) {
            return createJavaType( clazz );
        } else if ( clazz.isArray() ) {
            return createMultisetType( createType( clazz.getComponentType() ), -1 );
        } else if ( List.class.isAssignableFrom( clazz ) ) {
            return createArrayType( createTypeWithNullability( createPolyType( PolyType.ANY ), true ), -1 );
        } else if ( Map.class.isAssignableFrom( clazz ) ) {
            return createMapType(
                    createTypeWithNullability( createPolyType( PolyType.ANY ), true ),
                    createTypeWithNullability( createPolyType( PolyType.ANY ), true ) );
        } else {
            return createStructType( clazz );
        }
    }


    @Override
    public Type getJavaClass( AlgDataType type ) {
        if ( type instanceof JavaType ) {
            JavaType javaType = (JavaType) type;
            return javaType.getJavaClass();
        }
        if ( type.isStruct() && type.getFieldCount() == 1 && type.getPolyType() != PolyType.PATH ) {
            return getJavaClass( type.getFieldList().get( 0 ).getType() );
        }
        if ( type instanceof BasicPolyType || type instanceof IntervalPolyType || type instanceof PathType ) {
            switch ( type.getPolyType() ) {
                case JSON:
                case VARCHAR:
                case CHAR:
                case DOCUMENT:
                    return String.class;
                case DATE:
                case TIME:
                case TIME_WITH_LOCAL_TIME_ZONE:
                case INTEGER:
                case INTERVAL_YEAR:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_MONTH:
                    return type.isNullable() ? Integer.class : int.class;
                case TIMESTAMP:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case BIGINT:
                case INTERVAL_DAY:
                case INTERVAL_DAY_HOUR:
                case INTERVAL_DAY_MINUTE:
                case INTERVAL_DAY_SECOND:
                case INTERVAL_HOUR:
                case INTERVAL_HOUR_MINUTE:
                case INTERVAL_HOUR_SECOND:
                case INTERVAL_MINUTE:
                case INTERVAL_MINUTE_SECOND:
                case INTERVAL_SECOND:
                    return type.isNullable() ? Long.class : long.class;
                case SMALLINT:
                    return type.isNullable() ? Short.class : short.class;
                case TINYINT:
                    return type.isNullable() ? Byte.class : byte.class;
                case DECIMAL:
                    return BigDecimal.class;
                case BOOLEAN:
                    return type.isNullable() ? Boolean.class : boolean.class;
                case DOUBLE:
                case FLOAT: // sic
                    return type.isNullable() ? Double.class : double.class;
                case REAL:
                    return type.isNullable() ? Float.class : float.class;
                case BINARY:
                case VARBINARY:
                    return ByteString.class;
                case GEOMETRY:
                    return GeoFunctions.Geom.class;
                case SYMBOL:
                    return Enum.class;
                case GRAPH:
                    return PolyGraph.class;
                case EDGE:
                    return PolyEdge.class;
                case NODE:
                    return PolyNode.class;
                case PATH:
                    return PolyPath.class;
                case FILE:
                case IMAGE:
                case VIDEO:
                case SOUND:
                case ANY:
                    return Object.class;
            }
        }
        switch ( type.getPolyType() ) {
            case ROW:
                assert type instanceof AlgRecordType;
                if ( type instanceof JavaRecordType ) {
                    return ((JavaRecordType) type).clazz;
                } else {
                    return createSyntheticType( (AlgRecordType) type );
                }
            case MAP:
                return Map.class;
            case ARRAY:
            case MULTISET:
                return List.class;
        }
        return null;
    }


    @Override
    public AlgDataType toSql( AlgDataType type ) {
        return toSql( this, type );
    }


    /**
     * Converts a type in Java format to a SQL-oriented type.
     */
    public static AlgDataType toSql( final AlgDataTypeFactory typeFactory, AlgDataType type ) {
        if ( type instanceof AlgRecordType ) {
            return typeFactory.createStructType(
                    Lists.transform( type.getFieldList(), field -> toSql( typeFactory, field.getType() ) ),
                    type.getFieldNames() );
        }
        if ( type instanceof JavaType ) {
            return typeFactory.createTypeWithNullability( typeFactory.createPolyType( type.getPolyType() ), type.isNullable() );
        }
        return type;
    }


    @Override
    public Type createSyntheticType( List<Type> types ) {
        if ( types.isEmpty() ) {
            // Unit is a pre-defined synthetic type to be used when there are 0 fields. Because all instances are the same, we use a singleton.
            return Unit.class;
        }
        final String name = "Record" + types.size() + "_" + syntheticTypes.size();
        final SyntheticRecordType syntheticType = new SyntheticRecordType( null, name );
        for ( final Ord<Type> ord : Ord.zip( types ) ) {
            syntheticType.fields.add( new RecordFieldImpl( syntheticType, "f" + ord.i, ord.e, !Primitive.is( ord.e ), Modifier.PUBLIC ) );
        }
        return register( syntheticType );
    }


    private SyntheticRecordType register( final SyntheticRecordType syntheticType ) {
        final List<Pair<Type, Boolean>> key =
                new AbstractList<Pair<Type, Boolean>>() {
                    @Override
                    public Pair<Type, Boolean> get( int index ) {
                        final Types.RecordField field = syntheticType.getRecordFields().get( index );
                        return Pair.of( field.getType(), field.nullable() );
                    }


                    @Override
                    public int size() {
                        return syntheticType.getRecordFields().size();
                    }
                };
        SyntheticRecordType syntheticType2 = syntheticTypes.get( key );
        if ( syntheticType2 == null ) {
            syntheticTypes.put( key, syntheticType );
            return syntheticType;
        } else {
            return syntheticType2;
        }
    }


    /**
     * Creates a synthetic Java class whose fields have the same names and relational types.
     */
    private Type createSyntheticType( AlgRecordType type ) {
        final String name = "Record" + type.getFieldCount() + "_" + syntheticTypes.size();
        final SyntheticRecordType syntheticType = new SyntheticRecordType( type, name );
        for ( final AlgDataTypeField recordField : type.getFieldList() ) {
            final Type javaClass = getJavaClass( recordField.getType() );
            syntheticType.fields.add( new RecordFieldImpl( syntheticType, recordField.getName(), javaClass, recordField.getType().isNullable() && !Primitive.is( javaClass ), Modifier.PUBLIC ) );
        }
        return register( syntheticType );
    }


    /**
     * Synthetic record type.
     */
    public static class SyntheticRecordType implements Types.RecordType {

        final List<Types.RecordField> fields = new ArrayList<>();
        final AlgDataType algType;
        private final String name;


        private SyntheticRecordType( AlgDataType algType, String name ) {
            this.algType = algType;
            this.name = name;
            assert algType == null
                    || Util.isDistinct( algType.getFieldNames() )
                    : "field names not distinct: " + algType;
        }


        @Override
        public String getName() {
            return name;
        }


        @Override
        public List<Types.RecordField> getRecordFields() {
            return fields;
        }


        public String toString() {
            return name;
        }

    }


    /**
     * Implementation of a field.
     */
    private static class RecordFieldImpl implements Types.RecordField {

        private final SyntheticRecordType syntheticType;
        private final String name;
        private final Type type;
        private final boolean nullable;
        private final int modifiers;


        RecordFieldImpl( SyntheticRecordType syntheticType, String name, Type type, boolean nullable, int modifiers ) {
            this.syntheticType = Objects.requireNonNull( syntheticType );
            this.name = Objects.requireNonNull( name );
            this.type = Objects.requireNonNull( type );
            this.nullable = nullable;
            this.modifiers = modifiers;
            assert !(nullable && Primitive.is( type )) : "type [" + type + "] can never be null";
        }


        @Override
        public Type getType() {
            return type;
        }


        @Override
        public String getName() {
            return name;
        }


        @Override
        public int getModifiers() {
            return modifiers;
        }


        @Override
        public boolean nullable() {
            return nullable;
        }


        @Override
        public Object get( Object o ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public Type getDeclaringClass() {
            return syntheticType;
        }

    }

}

