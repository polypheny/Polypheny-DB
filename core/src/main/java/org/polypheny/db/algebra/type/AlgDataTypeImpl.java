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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.algebra.type;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.type.BasicPolyType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.Util;


/**
 * RelDataTypeImpl is an abstract base for implementations of {@link AlgDataType}.
 *
 * Identity is based upon the {@link #digest} field, which each derived class should set during construction.
 */
public abstract class AlgDataTypeImpl implements AlgDataType, AlgDataTypeFamily {

    protected final List<AlgDataTypeField> fields;
    private final List<Long> ids;
    protected String digest;


    /**
     * Creates a AlgDataTypeImpl.
     *
     * @param fields List of fields
     */
    protected AlgDataTypeImpl( List<? extends AlgDataTypeField> fields ) {
        if ( fields != null ) {
            // Create a defensive copy of the list.
            this.fields = ImmutableList.copyOf( fields );
            this.ids = fields.stream().map( AlgDataTypeField::getId ).collect( Collectors.toList() );
        } else {
            this.fields = null;
            this.ids = null;
        }
    }


    /**
     * Default constructor, to allow derived classes such as {@link BasicPolyType} to be {@link Serializable}.
     * <p>
     * (The serialization specification says that a class can be serializable even if its base class is not serializable,
     * provided that the base class has a public or protected zero-args constructor.)
     */
    protected AlgDataTypeImpl() {
        this( null );
    }


    @Override
    public AlgDataTypeField getField( String fieldName, boolean caseSensitive, boolean elideRecord ) {
        for ( AlgDataTypeField field : fields ) {
            if ( Util.matches( caseSensitive, field.getName(), fieldName ) ) {
                return field;
            }
        }
        if ( elideRecord ) {
            final List<Slot> slots = new ArrayList<>();
            getFieldRecurse( slots, this, 0, fieldName, caseSensitive );
            loop:
            for ( Slot slot : slots ) {
                switch ( slot.count ) {
                    case 0:
                        break; // no match at this depth; try deeper
                    case 1:
                        return slot.field;
                    default:
                        break loop; // duplicate fields at this depth; abandon search
                }
            }
        }
        // Extra field
        if ( !fields.isEmpty() ) {
            final AlgDataTypeField lastField = Iterables.getLast( fields );
            if ( lastField.getName().equals( "_extra" ) ) {
                return new AlgDataTypeFieldImpl( lastField.getId(), fieldName, -1, lastField.getType() );
            }
        }

        // a dynamic * field will match any field name.
        for ( AlgDataTypeField field : fields ) {
            if ( field.isDynamicStar() ) {
                // the requested field could be in the unresolved star
                return field;
            }
        }

        return null;
    }


    private static void getFieldRecurse( List<Slot> slots, AlgDataType type, int depth, String fieldName, boolean caseSensitive ) {
        while ( slots.size() <= depth ) {
            slots.add( new Slot() );
        }
        final Slot slot = slots.get( depth );
        for ( AlgDataTypeField field : type.getFields() ) {
            if ( Util.matches( caseSensitive, field.getName(), fieldName ) ) {
                slot.count++;
                slot.field = field;
            }
        }
        // No point looking to depth + 1 if there is a hit at depth.
        if ( slot.count == 0 ) {
            for ( AlgDataTypeField field : type.getFields() ) {
                if ( field.getType().isStruct() ) {
                    getFieldRecurse( slots, field.getType(), depth + 1, fieldName, caseSensitive );
                }
            }
        }
    }


    @Override
    public List<AlgDataTypeField> getFields() {
        assert isStruct();
        return fields;
    }


    @Override
    public List<String> getFieldNames() {
        return fields.stream().map( AlgDataTypeField::getName ).collect( Collectors.toList() );
    }


    @Override
    public List<Long> getFieldIds() {
        return ids;
    }


    public List<String> getPhysicalFieldNames() {
        // TODO MV: Is there a more efficient way for doing this?
        List<String> l = new ArrayList<>();
        fields.forEach( f -> l.add( f.getPhysicalName() ) );
        return l;
    }


    @Override
    public int getFieldCount() {
        assert isStruct() : this;
        return fields.size();
    }


    @Override
    public StructKind getStructKind() {
        return isStruct() ? StructKind.FULLY_QUALIFIED : StructKind.NONE;
    }


    @Override
    public AlgDataType getComponentType() {
        // this is not a collection type
        return null;
    }


    @Override
    public boolean isStruct() {
        return fields != null;
    }


    @Override
    public boolean equals( Object obj ) {
        if ( obj instanceof AlgDataTypeImpl ) {
            final AlgDataTypeImpl that = (AlgDataTypeImpl) obj;
            return this.digest.equals( that.digest );
        }
        return false;
    }


    @Override
    public int hashCode() {
        return digest.hashCode();
    }


    @Override
    public String getFullTypeString() {
        return digest;
    }


    @Override
    public boolean isNullable() {
        return false;
    }


    @Override
    public Charset getCharset() {
        return null;
    }


    @Override
    public Collation getCollation() {
        return null;
    }


    @Override
    public IntervalQualifier getIntervalQualifier() {
        return null;
    }


    @Override
    public int getPrecision() {
        return PRECISION_NOT_SPECIFIED;
    }


    @Override
    public int getRawPrecision() {
        return PRECISION_NOT_SPECIFIED;
    }


    @Override
    public int getScale() {
        return SCALE_NOT_SPECIFIED;
    }


    @Override
    public PolyType getPolyType() {
        return null;
    }


    @Override
    public AlgDataTypeFamily getFamily() {
        // by default, put each type into its own family
        return this;
    }


    /**
     * Generates a string representation of this type.
     *
     * @param sb StringBuilder into which to generate the string
     * @param withDetail when true, all detail information needed to compute a unique digest (and return from getFullTypeString) should be included;
     */
    protected abstract void generateTypeString( StringBuilder sb, boolean withDetail );


    /**
     * Computes the digest field. This should be called in every non-abstract subclass constructor once the type is fully defined.
     */
    protected void computeDigest() {
        StringBuilder sb = new StringBuilder();
        generateTypeString( sb, true );
        if ( !isNullable() ) {
            sb.append( " NOT NULL" );
        }
        digest = sb.toString();
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        generateTypeString( sb, false );
        return sb.toString();
    }


    @Override
    public AlgDataTypePrecedenceList getPrecedenceList() {
        // By default, make each type have a precedence list containing only other types in the same family
        return new AlgDataTypePrecedenceList() {
            @Override
            public boolean containsType( AlgDataType type ) {
                return getFamily() == type.getFamily();
            }


            @Override
            public int compareTypePrecedence( AlgDataType type1, AlgDataType type2 ) {
                assert containsType( type1 );
                assert containsType( type2 );
                return 0;
            }
        };
    }


    @Override
    public AlgDataTypeComparability getComparability() {
        return AlgDataTypeComparability.ALL;
    }


    /**
     * Returns an implementation of {@link AlgProtoDataType} that copies a given type using the given type factory.
     */
    public static AlgProtoDataType proto( final AlgDataType protoType ) {
        assert protoType != null;
        return (AlgProtoDataType & Serializable) typeFactory -> typeFactory.copyType( protoType );
    }


    /**
     * Returns a {@link AlgProtoDataType} that will create a type {@code typeName}.
     * <p>
     * For example, {@code proto(PolyType.DATE), false} will create {@code DATE NOT NULL}.
     *
     * @param typeName Type name
     * @param nullable Whether nullable
     * @return Proto data type
     */
    public static AlgProtoDataType proto( final PolyType typeName, final boolean nullable ) {
        assert typeName != null;
        return typeFactory -> {
            final AlgDataType type = typeFactory.createPolyType( typeName );
            return typeFactory.createTypeWithNullability( type, nullable );
        };
    }


    /**
     * Returns a {@link AlgProtoDataType} that will create a type {@code typeName(precision)}.
     * <p>
     * For example, {@code proto(PolyType.VARCHAR, 100, false)} will create {@code VARCHAR(100) NOT NULL}.
     *
     * @param typeName Type name
     * @param precision Precision
     * @param nullable Whether nullable
     * @return Proto data type
     */
    public static AlgProtoDataType proto( final PolyType typeName, final int precision, final boolean nullable ) {
        assert typeName != null;
        return typeFactory -> {
            final AlgDataType type = typeFactory.createPolyType( typeName, precision );
            return typeFactory.createTypeWithNullability( type, nullable );
        };
    }


    /**
     * Returns a {@link AlgProtoDataType} that will create a type {@code typeName(precision, scale)}.
     * <p>
     * For example, {@code proto(PolyType.DECIMAL, 7, 2, false)} will create {@code DECIMAL(7, 2) NOT NULL}.
     *
     * @param typeName Type name
     * @param precision Precision
     * @param scale Scale
     * @param nullable Whether nullable
     * @return Proto data type
     */
    public static AlgProtoDataType proto( final PolyType typeName, final int precision, final int scale, final boolean nullable ) {
        return typeFactory -> {
            final AlgDataType type = typeFactory.createPolyType( typeName, precision, scale );
            return typeFactory.createTypeWithNullability( type, nullable );
        };
    }


    /**
     * Returns the "extra" field in a row type whose presence signals that fields will come into existence just by asking for them.
     *
     * @param rowType Row type
     * @return The "extra" field, or null
     */
    public static AlgDataTypeField extra( AlgDataType rowType ) {
        // Even in a case-insensitive connection, the name must be precisely "_extra".
        return rowType.getField( "_extra", true, false );
    }


    @Override
    public boolean isDynamicStruct() {
        return false;
    }


    /**
     * Work space for {@link AlgDataTypeImpl#getFieldRecurse}.
     */
    private static class Slot {

        int count;
        AlgDataTypeField field;

    }

}

