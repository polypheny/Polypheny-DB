/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.util;


import com.google.common.collect.Ordering;
import java.util.Collections;
import java.util.Comparator;


/**
 * Compares arrays.
 */
public class ArrayComparator implements Comparator<Object[]> {

    private final Comparator[] comparators;


    public ArrayComparator( Comparator... comparators ) {
        this.comparators = comparators;
    }


    public ArrayComparator( boolean... descendings ) {
        this.comparators = comparators( descendings );
    }


    private static Comparator[] comparators( boolean[] descendings ) {
        Comparator[] comparators = new Comparator[descendings.length];
        for ( int i = 0; i < descendings.length; i++ ) {
            boolean descending = descendings[i];
            comparators[i] =
                    descending
                            ? Collections.reverseOrder()
                            : Ordering.natural();
        }
        return comparators;
    }


    @Override
    public int compare( Object[] o1, Object[] o2 ) {
        for ( int i = 0; i < comparators.length; i++ ) {
            Comparator comparator = comparators[i];
            int c = comparator.compare( o1[i], o2[i] );
            if ( c != 0 ) {
                return c;
            }
        }
        return 0;
    }
}

