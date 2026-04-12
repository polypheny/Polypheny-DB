/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.type.entity;

import org.apache.calcite.linq4j.tree.Expression;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class PolyFloatList<E extends PolyFloat> extends PolyList<E> {


    public PolyFloatList( PolyType type ) {
        super( type );
    }


    @Override
    public int size() {
        return 0;
    }


    @Override
    public boolean isEmpty() {
        return false;
    }


    @Override
    public boolean contains( Object o ) {
        return false;
    }


    @Override
    public @NotNull Iterator<E> iterator() {
        return null;
    }


    @Override
    public @NotNull Object[] toArray() {
        return new Object[0];
    }


    @Override
    public @NotNull <T> T[] toArray( @NotNull T[] a ) {
        return null;
    }


    @Override
    public boolean add( E e ) {
        return false;
    }


    @Override
    public boolean remove( Object o ) {
        return false;
    }


    @Override
    public boolean containsAll( @NotNull Collection<?> c ) {
        return false;
    }


    @Override
    public boolean addAll( @NotNull Collection<? extends E> c ) {
        return false;
    }


    @Override
    public boolean addAll( int index, @NotNull Collection<? extends E> c ) {
        return false;
    }


    @Override
    public boolean removeAll( @NotNull Collection<?> c ) {
        return false;
    }


    @Override
    public boolean retainAll( @NotNull Collection<?> c ) {
        return false;
    }


    @Override
    public void clear() {

    }


    @Override
    public E get( int index ) {
        return null;
    }


    @Override
    public E set( int index, E element ) {
        return null;
    }


    @Override
    public void add( int index, E element ) {

    }


    @Override
    public E remove( int index ) {
        return null;
    }


    @Override
    public int indexOf( Object o ) {
        return 0;
    }


    @Override
    public int lastIndexOf( Object o ) {
        return 0;
    }


    @Override
    public @NotNull ListIterator<E> listIterator() {
        return null;
    }


    @Override
    public @NotNull ListIterator<E> listIterator( int index ) {
        return null;
    }


    @Override
    public @NotNull List<E> subList( int fromIndex, int toIndex ) {
        return List.of();
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return 0L;
    }


    @Override
    public Object toJava() {
        return null;
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        return 0;
    }


    @Override
    public Expression asExpression() {
        return null;
    }


    @Override
    public PolySerializable copy() {
        return null;
    }

}
