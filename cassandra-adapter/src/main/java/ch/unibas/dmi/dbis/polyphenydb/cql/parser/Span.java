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

package ch.unibas.dmi.dbis.polyphenydb.cql.parser;


import ch.unibas.dmi.dbis.polyphenydb.cql.CqlNode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Builder for {@link CqlParserPos}.
 *
 * Because it is mutable, it is convenient for keeping track of the positions of the tokens that go into a non-terminal. It can be passed into methods, which can add the positions of tokens consumed to it.
 *
 * Some patterns:
 *
 * <ul>
 * <li>{@code final Span s;} declaration of a Span at the top of a production</li>
 * <li>{@code s = span();} initializes s to a Span that includes the token we just saw; very often occurs immediately after the first token in the production</li>
 * <li>{@code s.end(this);} adds the most recent token to span s and evaluates to a CqlParserPosition that spans from beginning to end; commonly used when making a call to a function</li>
 * <li>{@code s.pos()} returns a position spanning all tokens in the list</li>
 * <li>{@code s.add(node);} adds a CqlNode's parser position to a span</li>
 * <li>{@code s.addAll(nodeList);} adds several CqlNodes' parser positions to a span</li>
 * <li>{@code s = Span.of();} initializes s to an empty Span, not even including the most recent token; rarely used</li>
 * </ul>
 */
public final class Span {

    private final List<CqlParserPos> posList = new ArrayList<>();


    /**
     * Use one of the {@link #of} methods.
     */
    private Span() {
    }


    /**
     * Creates an empty Span.
     */
    public static Span of() {
        return new Span();
    }


    /**
     * Creates a Span with one position.
     */
    public static Span of( CqlParserPos p ) {
        return new Span().add( p );
    }


    /**
     * Creates a Span of one node.
     */
    public static Span of( CqlNode n ) {
        return new Span().add( n );
    }


    /**
     * Creates a Span between two nodes.
     */
    public static Span of( CqlNode n0, CqlNode n1 ) {
        return new Span().add( n0 ).add( n1 );
    }


    /**
     * Creates a Span of a list of nodes.
     */
    public static Span of( Collection<? extends CqlNode> nodes ) {
        return new Span().addAll( nodes );
    }


    /**
     * Adds a node's position to the list, and returns this Span.
     */
    public Span add( CqlNode n ) {
        return add( n.getParserPosition() );
    }


    /**
     * Adds a node's position to the list if the node is not null, and returns this Span.
     */
    public Span addIf( CqlNode n ) {
        return n == null ? this : add( n );
    }


    /**
     * Adds a position to the list, and returns this Span.
     */
    public Span add( CqlParserPos pos ) {
        posList.add( pos );
        return this;
    }


    /**
     * Adds the positions of a collection of nodes to the list, and returns this Span.
     */
    public Span addAll( Iterable<? extends CqlNode> nodes ) {
        for ( CqlNode node : nodes ) {
            add( node );
        }
        return this;
    }


    /**
     * Adds the position of the last token emitted by a parser to the list, and returns this Span.
     */
    public Span add( CqlAbstractParserImpl parser ) {
        try {
            final CqlParserPos pos = parser.getPos();
            return add( pos );
        } catch ( Exception e ) {
            // getPos does not really throw an exception
            throw new AssertionError( e );
        }
    }


    /**
     * Returns a position spanning the earliest position to the latest.
     * Does not assume that the positions are sorted.
     * Throws if the list is empty.
     */
    public CqlParserPos pos() {
        switch ( posList.size() ) {
            case 0:
                throw new AssertionError();
            case 1:
                return posList.get( 0 );
            default:
                return CqlParserPos.sum( posList );
        }
    }


    /**
     * Adds the position of the last token emitted by a parser to the list, and returns a position that covers the whole range.
     */
    public CqlParserPos end( CqlAbstractParserImpl parser ) {
        return add( parser ).pos();
    }


    /**
     * Adds a node's position to the list, and returns a position that covers the whole range.
     */
    public CqlParserPos end( CqlNode n ) {
        return add( n ).pos();
    }


    /**
     * Clears the contents of this Span, and returns this Span.
     */
    public Span clear() {
        posList.clear();
        return this;
    }
}

