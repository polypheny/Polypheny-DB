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


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import java.util.Iterator;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.polypheny.db.plan.AlgMultipleTrait;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.runtime.Utilities;
import org.polypheny.db.util.Util;


/**
 * Simple implementation of {@link AlgCollation}.
 */
@Getter
public class AlgCollationImpl implements AlgCollation {

    private final ImmutableList<AlgFieldCollation> fieldCollations;


    protected AlgCollationImpl( ImmutableList<AlgFieldCollation> fieldCollations ) {
        this.fieldCollations = fieldCollations;
        Preconditions.checkArgument( Util.isDistinct( AlgCollations.ordinals( fieldCollations ) ), "fields must be distinct" );
    }


    @Override
    public AlgCollationTraitDef getTraitDef() {
        return AlgCollationTraitDef.INSTANCE;
    }


    public int hashCode() {
        return fieldCollations.hashCode();
    }


    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( obj instanceof AlgCollationImpl that ) {
            return this.fieldCollations.equals( that.fieldCollations );
        }
        return false;
    }


    @Override
    public boolean isTop() {
        return fieldCollations.isEmpty();
    }


    @Override
    public int compareTo( @Nonnull AlgMultipleTrait o ) {
        final AlgCollationImpl that = (AlgCollationImpl) o;
        final UnmodifiableIterator<AlgFieldCollation> iterator = that.fieldCollations.iterator();
        for ( AlgFieldCollation f : fieldCollations ) {
            if ( !iterator.hasNext() ) {
                return 1;
            }
            final AlgFieldCollation f2 = iterator.next();
            int c = Utilities.compare( f.getFieldIndex(), f2.getFieldIndex() );
            if ( c != 0 ) {
                return c;
            }
        }
        return iterator.hasNext() ? -1 : 0;
    }


    @Override
    public void register( AlgPlanner planner ) {
    }


    @Override
    public boolean satisfies( AlgTrait<?> trait ) {
        return this == trait
                || trait instanceof AlgCollationImpl
                && Util.startsWith(
                fieldCollations,
                ((AlgCollationImpl) trait).fieldCollations );
    }


    /**
     * Returns a string representation of this collation, suitably terse given that it will appear in plan traces.
     * Examples: "[]", "[2]", "[0 DESC, 1]", "[0 DESC, 1 ASC NULLS LAST]".
     */
    public String toString() {
        Iterator<AlgFieldCollation> it = fieldCollations.iterator();
        if ( !it.hasNext() ) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder();
        sb.append( '[' );
        for ( ; ; ) {
            AlgFieldCollation e = it.next();
            sb.append( e.getFieldIndex() );
            if ( e.direction != AlgFieldCollation.Direction.ASCENDING || e.nullDirection != e.direction.defaultNullDirection() ) {
                sb.append( ' ' ).append( e.shortString() );
            }
            if ( !it.hasNext() ) {
                return sb.append( ']' ).toString();
            }
            sb.append( ',' ).append( ' ' );
        }
    }


}

