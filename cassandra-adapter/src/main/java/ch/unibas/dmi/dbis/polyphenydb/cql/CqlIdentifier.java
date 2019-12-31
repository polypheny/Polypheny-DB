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

package ch.unibas.dmi.dbis.polyphenydb.cql;


import ch.unibas.dmi.dbis.polyphenydb.cql.parser.CqlParserPos;
import java.util.Objects;


public class CqlIdentifier extends CqlNode {

    private String keyspace;
    private String name;
    private boolean quoted;


    /**
     * Creates a node.
     *
     * @param pos Parser position, must not be null.
     */
    CqlIdentifier( String name, String keyspace, boolean quoted, CqlParserPos pos ) {
        super( pos );
        this.name = Objects.requireNonNull(name);
        this.keyspace = keyspace;
        this.quoted = quoted;
    }


    @Override
    public String toString() {
        if ( this.keyspace != null ) {
            return this.keyspace + "." + this.name;
        } else {
            return this.name;
        }
    }


    @Override
    public CqlNode clone( CqlParserPos pos ) {
        return new CqlIdentifier( this.name, this.keyspace, this.quoted , pos );
    }

    @Override
    public CqlKind getKind() {
        return CqlKind.IDENTIFIER;
    }
}
