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

package org.polypheny.db.type;


import java.util.List;
import java.util.Objects;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.util.Util;


/**
 * SqlTypeTransforms defines a number of reusable instances of {@link PolyTypeTransform}.
 * <p>
 * NOTE: avoid anonymous inner classes here except for unique, non-generalizable strategies; anything else belongs in a
 * reusable top-level class. If you find yourself copying and pasting an existing strategy's anonymous inner class,
 * you're making a mistake.
 */
public abstract class PolyTypeTransforms {

    /**
     * Parameter type-inference transform strategy where a derived type is transformed into the same type but nullable
     * if any of a calls operands is nullable
     */
    public static final PolyTypeTransform TO_NULLABLE =
            ( opBinding, typeToTransform ) ->
                    PolyTypeUtil.makeNullableIfOperandsAre(
                            opBinding.getTypeFactory(),
                            opBinding.collectOperandTypes(),
                            Objects.requireNonNull( typeToTransform ) );

    /**
     * Parameter type-inference transform strategy where a derived type is transformed into the same type, but nullable if
     * and only if all of a call's operands are nullable.
     */
    public static final PolyTypeTransform TO_NULLABLE_ALL = ( opBinding, type ) -> {
        final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
        return typeFactory.createTypeWithNullability( type, PolyTypeUtil.allNullable( opBinding.collectOperandTypes() ) );
    };

    /**
     * Parameter type-inference transform strategy where a derived type is transformed into the same type but not nullable.
     */
    public static final PolyTypeTransform TO_NOT_NULLABLE =
            ( opBinding, typeToTransform ) ->
                    opBinding.getTypeFactory().createTypeWithNullability(
                            Objects.requireNonNull( typeToTransform ),
                            false );

    /**
     * Parameter type-inference transform strategy where a derived type is transformed into the same type with nulls allowed.
     */
    public static final PolyTypeTransform FORCE_NULLABLE =
            ( opBinding, typeToTransform ) ->
                    opBinding.getTypeFactory().createTypeWithNullability(
                            Objects.requireNonNull( typeToTransform ),
                            true );

    /**
     * Type-inference strategy whereby the result is NOT NULL if any of the arguments is NOT NULL; otherwise the type is unchanged.
     */
    public static final PolyTypeTransform LEAST_NULLABLE =
            ( opBinding, typeToTransform ) -> {
                for ( AlgDataType type : opBinding.collectOperandTypes() ) {
                    if ( !type.isNullable() ) {
                        return opBinding.getTypeFactory().createTypeWithNullability( typeToTransform, false );
                    }
                }
                return typeToTransform;
            };

    /**
     * Type-inference strategy whereby the result type of a call is VARYING the type given. The length returned is the same
     * as length of the first argument. Return type will have same nullability as input type nullability. First Arg must be
     * of string type.
     */
    public static final PolyTypeTransform TO_VARYING =
            new PolyTypeTransform() {
                @Override
                public AlgDataType transformType( OperatorBinding opBinding, AlgDataType typeToTransform ) {
                    switch ( typeToTransform.getPolyType() ) {
                        case VARCHAR:
                        case VARBINARY:
                            return typeToTransform;
                    }

                    PolyType retTypeName = toVar( typeToTransform );

                    AlgDataType ret = opBinding.getTypeFactory().createPolyType( retTypeName, typeToTransform.getPrecision() );
                    if ( PolyTypeUtil.inCharFamily( typeToTransform ) ) {
                        ret = opBinding.getTypeFactory()
                                .createTypeWithCharsetAndCollation(
                                        ret,
                                        typeToTransform.getCharset(),
                                        typeToTransform.getCollation() );
                    }
                    return opBinding.getTypeFactory().createTypeWithNullability( ret, typeToTransform.isNullable() );
                }


                private PolyType toVar( AlgDataType type ) {
                    final PolyType polyType = type.getPolyType();
                    switch ( polyType ) {
                        case CHAR:
                            return PolyType.VARCHAR;
                        case BINARY:
                            return PolyType.VARBINARY;
                        case ANY:
                            return PolyType.ANY;
                        default:
                            throw Util.unexpected( polyType );
                    }
                }
            };

    /**
     * Parameter type-inference transform strategy where a derived type must be a multiset type and the returned type
     * is the multiset's element type.
     *
     * @see MultisetPolyType#getComponentType
     */
    public static final PolyTypeTransform TO_MULTISET_ELEMENT_TYPE = ( opBinding, typeToTransform ) -> typeToTransform.getComponentType();

    /**
     * Parameter type-inference transform strategy that wraps a given type in a multiset.
     *
     * @see AlgDataTypeFactory#createMultisetType(AlgDataType, long)
     */
    public static final PolyTypeTransform TO_MULTISET = ( opBinding, typeToTransform ) -> opBinding.getTypeFactory().createMultisetType( typeToTransform, -1 );

    /**
     * Parameter type-inference transform strategy where a derived type must be a struct type with precisely one field and
     * the returned type is the type of that field.
     */
    public static final PolyTypeTransform ONLY_COLUMN =
            ( opBinding, typeToTransform ) -> {
                final List<AlgDataTypeField> fields = typeToTransform.getFields();
                assert fields.size() == 1;
                return fields.get( 0 ).getType();
            };

}

