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

import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.def.SimpleSerializerDef;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * <p>Special list type for {@code PolyList<PolyFloat>}. </p>
 * <p>This list is specifically used where performance matters.</p>
 * @param <E> PolyFloat
 */
@EqualsAndHashCode(callSuper = false)
public class PolyFloatList<E extends PolyFloat> extends PolyList<E> {

    @Getter
    private float[] value;

    public PolyFloatList( ) {
        super( PolyType.ARRAY );
        this.value = new float[0];
    }


    public PolyFloatList( float[] value ) {
        super( PolyType.ARRAY );
        this.value = value;
    }


    @Override
    public int size() {
        return value.length;
    }


    @Override
    public boolean isEmpty() {
        return value.length == 0;
    }


    @Override
    public boolean contains( Object o ) {
        if ( o instanceof PolyFloat pf ) {
            for ( float f : value ) {
                if ( pf.equals( PolyFloat.of( f ) ) ) return true;
            }
        } else if ( o instanceof Float fl ) {
            for ( float f : value ) {
                if ( fl.equals( f ) ) return true;
            }
        }
        return false;
    }


    @Override
    public @NotNull Iterator<E> iterator() {
        return new Iterator<>() {
            private int cursor = 0;

            @Override
            public boolean hasNext() {
                return cursor < value.length;
            }

            @Override
            @SuppressWarnings("unchecked")
            public E next() {
                if ( !hasNext() ) throw new NoSuchElementException();
                return (E) PolyFloat.of( value[cursor++] );
            }
        };
    }


    @Override
    public @NotNull Object[] toArray() {
        Object[] arr = new Object[value.length];
        for ( int i = 0; i < value.length; i++ ) {
            arr[i] = PolyFloat.of( value[i] );
        }
        return arr;
    }


    @Override
    @SuppressWarnings("unchecked")
    public @NotNull <T> T[] toArray( @NotNull T[] a ) {
        int size = size();
        if ( a.length < size ) {
            return (T[]) Arrays.copyOf( toArray(), size, a.getClass() );
        }
        System.arraycopy( toArray(), 0, a, 0, size );
        if ( a.length > size ) {
            a[size] = null;
        }
        return a;
    }


    @Override
    public boolean add( E e ) {
        float[] newArray = Arrays.copyOf( value, value.length + 1 );
        newArray[value.length] = e.floatValue();
        value = newArray;
        return true;
    }


    @Override
    public boolean remove( Object o ) {
        int index = indexOf( o );
        if ( index >= 0 ) {
            remove( index );
            return true;
        }
        return false;
    }


    @Override
    public boolean containsAll( @NotNull Collection<?> c ) {
        for ( Object e : c ) {
            if ( !contains( e ) ) return false;
        }
        return true;
    }


    @Override
    public boolean addAll( @NotNull Collection<? extends E> c ) {
        if ( c.isEmpty() ) return false;
        float[] newArray = Arrays.copyOf( value, value.length + c.size() );
        int i = value.length;
        for ( E e : c ) {
            newArray[i++] = e.floatValue();
        }
        value = newArray;
        return true;
    }


    @Override
    public boolean addAll( int index, @NotNull Collection<? extends E> c ) {
        if ( index < 0 || index > value.length ) throw new IndexOutOfBoundsException();
        if ( c.isEmpty() ) return false;

        float[] newArray = new float[value.length + c.size()];
        System.arraycopy( value, 0, newArray, 0, index );

        int i = index;
        for ( E e : c ) {
            newArray[i++] = e.floatValue();
        }

        System.arraycopy( value, index, newArray, index + c.size(), value.length - index );
        value = newArray;
        return true;
    }


    @Override
    public boolean removeAll( @NotNull Collection<?> c ) {
        boolean modified = false;
        for ( Object e : c ) {
            while ( remove( e ) ) {
                modified = true;
            }
        }
        return modified;
    }


    @Override
    public boolean retainAll( @NotNull Collection<?> c ) {
        boolean modified = false;
        for ( int i = value.length - 1; i >= 0; --i ) {
            if ( !c.contains( get( i ) ) ) {
                remove( i );
                modified = true;
            }
        }
        return modified;
    }


    @Override
    public void clear() {
        value = new float[0];
    }


    @Override
    @SuppressWarnings("unchecked")
    public E get( int index ) {
        if ( index < 0 || index >= value.length ) throw new IndexOutOfBoundsException();
        return (E) PolyFloat.of( value[index] );
    }


    @Override
    public E set( int index, E element ) {
        if ( index < 0 || index >= value.length ) throw new IndexOutOfBoundsException();
        E old = get( index );
        value[index] = element.floatValue();
        return old;
    }


    @Override
    public void add( int index, E element ) {
        if ( index < 0 || index > value.length ) throw new IndexOutOfBoundsException();
        float[] newArray = new float[value.length + 1];
        System.arraycopy( value, 0, newArray, 0, index );
        newArray[index] = element.floatValue();
        System.arraycopy( value, index, newArray, index + 1, value.length - index );
        value = newArray;
    }


    @Override
    public E remove( int index ) {
        if ( index < 0 || index >= value.length ) throw new IndexOutOfBoundsException();
        E old = get( index );
        float[] newArray = new float[value.length - 1];
        System.arraycopy( value, 0, newArray, 0, index );
        System.arraycopy( value, index + 1, newArray, index, value.length - index - 1 );
        value = newArray;
        return old;
    }


    @Override
    public int indexOf( Object o ) {
        for ( int i = 0; i < value.length; ++i ) {
            if ( get( i ).equals( o ) ) return i;
        }
        return -1;
    }


    @Override
    public int lastIndexOf( Object o ) {
        for ( int i = value.length - 1; i >= 0; --i ) {
            if ( get( i ).equals( o ) ) return i;
        }
        return -1;
    }


    @Override
    public @NotNull ListIterator<E> listIterator() {
        return listIterator( 0 );
    }


    @Override
    public @NotNull ListIterator<E> listIterator( int index ) {
        List<E> list = new ArrayList<>();
        for ( int i = 0; i < size(); i++ ) {
            list.add( get( i ) );
        }
        return list.listIterator( index );
    }


    @Override
    public @NotNull List<E> subList( int fromIndex, int toIndex ) {
        if ( fromIndex < 0 || toIndex > value.length || fromIndex > toIndex ) {
            throw new IndexOutOfBoundsException();
        }
        List<E> sublist = new ArrayList<>( toIndex - fromIndex );
        for ( int i = fromIndex; i < toIndex; ++i ) {
            sublist.add( get( i ) );
        }
        return sublist;
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return (long) value.length * Float.BYTES;
    }


    @Override
    public Object toJava() {
        return Arrays.asList( toArray() );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !o.isList() ) return -1;
        PolyList<?> other = o.asList();
        int minSize = Math.min( this.size(), other.size() );
        for ( int i = 0; i < minSize; i++ ) {
            int cmp = this.get( i ).compareTo( other.get( i ) );
            if ( cmp != 0 ) return cmp;
        }
        return Integer.compare( this.size(), other.size() );
    }


    @Override
    public Expression asExpression() {
        List<Expression> list = new ArrayList<>();
        for ( float f : value ) {
            list.add( PolyFloat.of( f ).asExpression() );
        }
        return Expressions.new_( PolyFloatList.class, Expressions.newArrayInit( float.class, list) );
    }


    @Override
    public PolySerializable copy() {
        return new PolyFloatList<>( Arrays.copyOf( value, value.length ) );
    }


    public float getRaw( int i ) {
        return value[i];
    }


    public float[] getRawValue() {
        return value;
    }


    public static class PolyFloatListSerializerDef extends SimpleSerializerDef<PolyFloatList<?>> {

        @Override
        protected BinarySerializer<PolyFloatList<?>> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {

                // size, value[0], value[1], ..., value[n], with n = item.size()
                @Override
                public void encode( BinaryOutput out, PolyFloatList<?> item ) {
                    if ( item == null ) {
                        out.writeInt( -1 );
                        return;
                    }
                    out.writeInt( item.size() );
                    for ( int i = 0; i < item.size(); ++i ) {
                        out.writeFloat( item.getRaw( i ) );
                    }
                }

                @Override
                public PolyFloatList<?> decode( BinaryInput in ) throws CorruptedDataException {
                    int size = in.readInt();
                    if ( size == -1 ) return null;
                    float[] arr = new float[size];
                    for ( int i = 0; i < size; ++i ) {
                        arr[i] = in.readFloat();
                    }
                    return new PolyFloatList<>( arr );
                }
            };
        }
    }

}
