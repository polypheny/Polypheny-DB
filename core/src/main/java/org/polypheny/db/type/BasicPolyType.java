/*
 * Copyright 2019-2021 The Polypheny Project
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


import com.google.common.base.Preconditions;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import lombok.Getter;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.SerializableCharset;


/**
 * BasicPolyType represents a standard atomic type (excluding interval types).
 * <p>
 * Instances of this class are immutable.
 */
public class BasicPolyType extends AbstractPolyType {

    private final int precision;
    private final int scale;
    private final AlgDataTypeSystem typeSystem;
    @Getter
    private final Collation collation;
    private final SerializableCharset wrappedCharset;


    /**
     * Constructs a type with no parameters. This should only be called from a factory method.
     *
     * @param typeSystem Type system
     * @param typeName Type name
     */
    public BasicPolyType( AlgDataTypeSystem typeSystem, PolyType typeName ) {
        this( typeSystem, typeName, false, PRECISION_NOT_SPECIFIED, SCALE_NOT_SPECIFIED, null, null );
        checkPrecScale( typeName, false, false );
    }


    /**
     * Throws if {@code typeName} does not allow the given combination of precision and scale.
     */
    protected static void checkPrecScale( PolyType typeName, boolean precisionSpecified, boolean scaleSpecified ) {
        if ( !typeName.allowsPrecScale( precisionSpecified, scaleSpecified ) ) {
            throw new AssertionError( "typeName.allowsPrecScale(" + precisionSpecified + ", " + scaleSpecified + "): " + typeName );
        }
    }


    /**
     * Constructs a type with precision/length but no scale.
     *
     * @param typeSystem Type system
     * @param typeName Type name
     * @param precision Precision (called length for some types)
     */
    public BasicPolyType( AlgDataTypeSystem typeSystem, PolyType typeName, int precision ) {
        this( typeSystem, typeName, false, precision, SCALE_NOT_SPECIFIED, null, null );
        checkPrecScale( typeName, true, false );
    }


    /**
     * Constructs a type with precision/length and scale.
     *
     * @param typeSystem Type system
     * @param typeName Type name
     * @param precision Precision (called length for some types)
     * @param scale Scale
     */
    public BasicPolyType( AlgDataTypeSystem typeSystem, PolyType typeName, int precision, int scale ) {
        this( typeSystem, typeName, false, precision, scale, null, null );
        checkPrecScale( typeName, true, true );
    }


    /**
     * Internal constructor.
     */
    private BasicPolyType(
            AlgDataTypeSystem typeSystem,
            PolyType typeName,
            boolean nullable,
            int precision,
            int scale,
            Collation collation,
            SerializableCharset wrappedCharset ) {
        super( typeName, nullable, null );
        this.typeSystem = Objects.requireNonNull( typeSystem );
        this.precision = precision;
        this.scale = scale;

        if ( typeName == PolyType.JSON ) {
            this.collation = Collation.IMPLICIT;
            this.wrappedCharset = SerializableCharset.forCharset( StandardCharsets.ISO_8859_1 );
        } else {
            this.collation = collation;
            this.wrappedCharset = wrappedCharset;
        }

        computeDigest();
    }


    /**
     * Constructs a type with nullability.
     */
    public BasicPolyType createWithNullability( boolean nullable ) {
        if ( nullable == this.isNullable ) {
            return this;
        }
        return new BasicPolyType(
                this.typeSystem,
                this.typeName,
                nullable,
                this.precision,
                this.scale,
                this.collation,
                this.wrappedCharset );
    }


    /**
     * Constructs a type with charset and collation.
     * <p>
     * This must be a character type.
     */
    BasicPolyType createWithCharsetAndCollation( Charset charset, Collation collation ) {
        Preconditions.checkArgument( PolyTypeUtil.inCharFamily( this ) );
        return new BasicPolyType(
                this.typeSystem,
                this.typeName,
                this.isNullable,
                this.precision,
                this.scale,
                collation,
                SerializableCharset.forCharset( charset ) );
    }


    @Override
    public int getPrecision() {
        if ( precision == PRECISION_NOT_SPECIFIED ) {
            return typeSystem.getDefaultPrecision( typeName );
        }
        return precision;
    }


    @Override
    public int getRawPrecision() {
        return precision;
    }


    @Override
    public int getScale() {
        if ( scale == SCALE_NOT_SPECIFIED ) {
            switch ( typeName ) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case DECIMAL:
                    return 0;
                default:
                    // fall through
            }
        }
        return scale;
    }


    @Override
    public Charset getCharset() {
        return wrappedCharset == null ? null : wrappedCharset.getCharset();
    }


    // implement RelDataTypeImpl
    @Override
    protected void generateTypeString( StringBuilder sb, boolean withDetail ) {
        // Called to make the digest, which equals() compares; so equivalent data types must produce identical type strings.

        sb.append( typeName.name() );
        boolean printPrecision = precision != PRECISION_NOT_SPECIFIED;
        boolean printScale = scale != SCALE_NOT_SPECIFIED;

        // for the digest, print the precision when defaulted, since (for instance) TIME is equivalent to TIME(0).
        if ( withDetail ) {
            // -1 means there is no default value for precision
            if ( typeName.allowsPrec() && typeSystem.getDefaultPrecision( typeName ) > -1 ) {
                printPrecision = true;
            }
            if ( typeName.getDefaultScale() > -1 ) {
                printScale = true;
            }
        }

        if ( printPrecision ) {
            sb.append( '(' );
            sb.append( getPrecision() );
            if ( printScale ) {
                sb.append( ", " );
                sb.append( getScale() );
            }
            sb.append( ')' );
        }
        if ( !withDetail ) {
            return;
        }
        if ( wrappedCharset != null && !Collation.IMPLICIT.getCharset().equals( wrappedCharset.getCharset() ) ) {
            sb.append( " CHARACTER SET \"" );
            sb.append( wrappedCharset.getCharset().name() );
            sb.append( "\"" );
        }
        if ( collation != null && collation != Collation.IMPLICIT && collation != Collation.COERCIBLE ) {
            sb.append( " COLLATE \"" );
            sb.append( collation.getCollationName() );
            sb.append( "\"" );
        }
    }


    /**
     * Returns a value which is a limit for this type.
     *
     * For example,
     *
     * <table border="1">
     * <caption>Limits</caption>
     * <tr>
     * <th>Datatype</th>
     * <th>sign</th>
     * <th>limit</th>
     * <th>beyond</th>
     * <th>precision</th>
     * <th>scale</th>
     * <th>Returns</th>
     * </tr>
     * <tr>
     * <td>Integer</td>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>2147483647 (2 ^ 31 -1 = MAXINT)</td>
     * </tr>
     * <tr>
     * <td>Integer</td>
     * <td>true</td>
     * <td>true</td>
     * <td>true</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>2147483648 (2 ^ 31 = MAXINT + 1)</td>
     * </tr>
     * <tr>
     * <td>Integer</td>
     * <td>false</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>-2147483648 (-2 ^ 31 = MININT)</td>
     * </tr>
     * <tr>
     * <td>Boolean</td>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>TRUE</td>
     * </tr>
     * <tr>
     * <td>Varchar</td>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>10</td>
     * <td>-1</td>
     * <td>'ZZZZZZZZZZ'</td>
     * </tr>
     * </table>
     *
     * @param sign If true, returns upper limit, otherwise lower limit
     * @param limit If true, returns value at or near to overflow; otherwise value at or near to underflow
     * @param beyond If true, returns the value just beyond the limit, otherwise the value at the limit
     * @return Limit value
     */
    public Object getLimit( boolean sign, PolyType.Limit limit, boolean beyond ) {
        int precision = typeName.allowsPrec() ? this.getPrecision() : -1;
        int scale = typeName.allowsScale() ? this.getScale() : -1;
        return typeName.getLimit( sign, limit, beyond, precision, scale );
    }

}

