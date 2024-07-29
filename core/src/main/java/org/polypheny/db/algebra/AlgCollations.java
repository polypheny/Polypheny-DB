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

package org.polypheny.db.algebra;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.mapping.Mappings.TargetMapping;


/**
 * Utilities concerning {@link AlgCollation} and {@link AlgFieldCollation}.
 */
public class AlgCollations {

    /**
     * A collation indicating that a relation is not sorted. Ordering by no columns.
     */
    public static final AlgCollation EMPTY = AlgCollationTraitDef.INSTANCE.canonize( new AlgCollationImpl( ImmutableList.of() ) );


    private AlgCollations() {
    }


    public static AlgCollation of( AlgFieldCollation... fieldCollations ) {
        return of( ImmutableList.copyOf( fieldCollations ) );
    }


    public static AlgCollation of( List<AlgFieldCollation> fieldCollations ) {
        if ( Util.isDistinct( ordinals( fieldCollations ) ) ) {
            return new AlgCollationImpl( ImmutableList.copyOf( fieldCollations ) );
        }
        // Remove field collations whose field has already been seen
        final ImmutableList.Builder<AlgFieldCollation> builder = ImmutableList.builder();
        final Set<Integer> set = new HashSet<>();
        for ( AlgFieldCollation fieldCollation : fieldCollations ) {
            if ( set.add( fieldCollation.getFieldIndex() ) ) {
                builder.add( fieldCollation );
            }
        }
        return new AlgCollationImpl( builder.build() );
    }


    /**
     * Creates a collation containing one field.
     */
    public static AlgCollation of( int fieldIndex ) {
        return of( new AlgFieldCollation( fieldIndex ) );
    }


    /**
     * Creates a list containing one collation containing one field.
     */
    public static List<AlgCollation> createSingleton( int fieldIndex ) {
        return ImmutableList.of( of( fieldIndex ) );
    }


    /**
     * Checks that a collection of collations is valid.
     *
     * @param rowType Row type of the relational expression
     * @param collationList List of collations
     * @param fail Whether to fail if invalid
     * @return Whether valid
     */
    public static boolean isValid( AlgDataType rowType, List<AlgCollation> collationList, boolean fail ) {
        final int fieldCount = rowType.getFieldCount();
        for ( AlgCollation collation : collationList ) {
            for ( AlgFieldCollation fieldCollation : collation.getFieldCollations() ) {
                final int index = fieldCollation.getFieldIndex();
                if ( index < 0 || index >= fieldCount ) {
                    assert !fail;
                    return false;
                }
            }
        }
        return true;
    }


    public static boolean equal( List<AlgCollation> collationList1, List<AlgCollation> collationList2 ) {
        return collationList1.equals( collationList2 );
    }


    /**
     * Returns the indexes of the field collations in a given collation.
     */
    public static List<Integer> ordinals( AlgCollation collation ) {
        return ordinals( collation.getFieldCollations() );
    }


    /**
     * Returns the indexes of the fields in a list of field collations.
     */
    public static List<Integer> ordinals(
            List<AlgFieldCollation> fieldCollations ) {
        return Lists.transform( fieldCollations, AlgFieldCollation::getFieldIndex );
    }


    /**
     * Returns whether a collation indicates that the collection is sorted on a given list of keys.
     *
     * @param collation Collation
     * @param keys List of keys
     * @return Whether the collection is sorted on the given keys
     */
    public static boolean contains( AlgCollation collation, Iterable<Integer> keys ) {
        return contains( collation, Util.distinctList( keys ) );
    }


    private static boolean contains( AlgCollation collation, List<Integer> keys ) {
        final int n = collation.getFieldCollations().size();
        final Iterator<Integer> iterator = keys.iterator();
        for ( int i = 0; i < n; i++ ) {
            final AlgFieldCollation fieldCollation =
                    collation.getFieldCollations().get( i );
            if ( !iterator.hasNext() ) {
                return true;
            }
            if ( fieldCollation.getFieldIndex() != iterator.next() ) {
                return false;
            }
        }
        return !iterator.hasNext();
    }


    /**
     * Returns whether one of a list of collations indicates that the collection is sorted on the given list of keys.
     */
    public static boolean contains( List<AlgCollation> collations, ImmutableList<Integer> keys ) {
        final List<Integer> distinctKeys = Util.distinctList( keys );
        for ( AlgCollation collation : collations ) {
            if ( contains( collation, distinctKeys ) ) {
                return true;
            }
        }
        return false;
    }


    public static AlgCollation shift( AlgCollation collation, int offset ) {
        if ( offset == 0 ) {
            return collation; // save some effort
        }
        final ImmutableList.Builder<AlgFieldCollation> fieldCollations =
                ImmutableList.builder();
        for ( AlgFieldCollation fc : collation.getFieldCollations() ) {
            fieldCollations.add( fc.shift( offset ) );
        }
        return new AlgCollationImpl( fieldCollations.build() );
    }


    /**
     * Creates a copy of this collation that changes the ordinals of input fields.
     */
    public static AlgCollation permute( AlgCollation collation, Map<Integer, Integer> mapping ) {
        return of( Util.transform( collation.getFieldCollations(), fc -> fc.copy( mapping.get( fc.getFieldIndex() ) ) ) );
    }


    /**
     * Creates a copy of this collation that changes the ordinals of input fields.
     */
    public static AlgCollation permute( AlgCollation collation, TargetMapping mapping ) {
        return of( Util.transform( collation.getFieldCollations(), fc -> fc.copy( mapping.getTarget( fc.getFieldIndex() ) ) ) );
    }

}
