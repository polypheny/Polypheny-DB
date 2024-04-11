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

package org.polypheny.db.rex;


import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.util.Pair;


/**
 * Variable which references a field of an input relational expression.
 *
 * Fields of the input are 0-based. If there is more than one input, they are numbered consecutively. For example, if the inputs to a join are.
 *
 * <ul>
 * <li>Input #0: EMP(EMPNO, ENAME, DEPTNO) and</li>
 * <li>Input #1: DEPT(DEPTNO AS DEPTNO2, DNAME)</li>
 * </ul>
 *
 * then the fields are:
 *
 * <ul>
 * <li>Field #0: EMPNO</li>
 * <li>Field #1: ENAME</li>
 * <li>Field #2: DEPTNO (from EMP)</li>
 * <li>Field #3: DEPTNO2 (from DEPT)</li>
 * <li>Field #4: DNAME</li>
 * </ul>
 *
 * So <code>RexInputRef(3, Integer)</code> is the correct reference for the field DEPTNO2.
 */
public class RexIndexRef extends RexSlot {

    // list of common names, to reduce memory allocations
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private static final List<String> NAMES = new SelfPopulatingList( "$", 30 );


    /**
     * Creates an input variable.
     *
     * @param index Index of the field in the underlying row-type
     * @param type Type of the column
     */
    public RexIndexRef( int index, AlgDataType type ) {
        super( createName( index ), index, type );
    }


    @Override
    public boolean equals( Object obj ) {
        return this == obj
                || obj instanceof RexIndexRef
                && index == ((RexIndexRef) obj).index;
    }


    @Override
    public int hashCode() {
        return index;
    }


    /**
     * Creates a reference to a given field in a row type.
     */
    public static RexIndexRef of( int index, AlgDataType rowType ) {
        return of( index, rowType.getFields() );
    }


    public static RexIndexRef of( int index, DocumentType rowType ) {
        return new RexIndexRef( index, rowType );
    }


    /**
     * Creates a reference to a given field in a list of fields.
     */
    public static RexIndexRef of( int index, List<AlgDataTypeField> fields ) {
        return new RexIndexRef( index, fields.get( index ).getType() );
    }


    /**
     * Creates a reference to a given field in a list of fields.
     */
    public static Pair<RexNode, String> of2( int index, List<AlgDataTypeField> fields ) {
        final AlgDataTypeField field = fields.get( index );
        return Pair.of( new RexIndexRef( index, field.getType() ), field.getName() );
    }


    @Override
    public Kind getKind() {
        return Kind.INPUT_REF;
    }


    @Override
    public <R> R accept( RexVisitor<R> visitor ) {
        return visitor.visitIndexRef( this );
    }


    @Override
    public <R, P> R accept( RexBiVisitor<R, P> visitor, P arg ) {
        return visitor.visitInputRef( this, arg );
    }


    /**
     * Creates a name for an input reference, of the form "$index". If the index is low, uses a cache of common names, to reduce gc.
     */
    public static String createName( int index ) {
        return NAMES.get( index );
    }

}

