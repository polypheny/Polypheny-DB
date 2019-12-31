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
import ch.unibas.dmi.dbis.polyphenydb.util.Static;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;


/**
 * SqlParserPos represents the position of a parsed token within SQL statement text.
 */
public class CqlParserPos implements Serializable {

    /**
     * SqlParserPos representing line one, character one. Use this if the node doesn't correspond to a position in piece of SQL text.
     */
    public static final CqlParserPos ZERO = new CqlParserPos( 0, 0 );

    private static final long serialVersionUID = 1L;

    private final int lineNumber;
    private final int columnNumber;
    private final int endLineNumber;
    private final int endColumnNumber;


    /**
     * Creates a new parser position.
     */
    public CqlParserPos( int lineNumber, int columnNumber ) {
        this( lineNumber, columnNumber, lineNumber, columnNumber );
    }


    /**
     * Creates a new parser range.
     */
    public CqlParserPos( int startLineNumber, int startColumnNumber, int endLineNumber, int endColumnNumber ) {
        this.lineNumber = startLineNumber;
        this.columnNumber = startColumnNumber;
        this.endLineNumber = endLineNumber;
        this.endColumnNumber = endColumnNumber;
        assert startLineNumber < endLineNumber
                || startLineNumber == endLineNumber
                && startColumnNumber <= endColumnNumber;
    }


    public int hashCode() {
        return Objects.hash( lineNumber, columnNumber, endLineNumber, endColumnNumber );
    }


    public boolean equals( Object o ) {
        return o == this
                || o instanceof CqlParserPos
                && this.lineNumber == ((CqlParserPos) o).lineNumber
                && this.columnNumber == ((CqlParserPos) o).columnNumber
                && this.endLineNumber == ((CqlParserPos) o).endLineNumber
                && this.endColumnNumber == ((CqlParserPos) o).endColumnNumber;
    }


    /**
     * @return 1-based starting line number
     */
    public int getLineNum() {
        return lineNumber;
    }


    /**
     * @return 1-based starting column number
     */
    public int getColumnNum() {
        return columnNumber;
    }


    /**
     * @return 1-based end line number (same as starting line number if the ParserPos is a point, not a range)
     */
    public int getEndLineNum() {
        return endLineNumber;
    }


    /**
     * @return 1-based end column number (same as starting column number if the ParserPos is a point, not a range)
     */
    public int getEndColumnNum() {
        return endColumnNumber;
    }


    @Override
    public String toString() {
        return Static.RESOURCE.parserContext( lineNumber, columnNumber ).str();
    }


    /**
     * Combines this parser position with another to create a position that spans from the first point in the first to the last point in the other.
     */
    public CqlParserPos plus( CqlParserPos pos ) {
        return new CqlParserPos( getLineNum(), getColumnNum(), pos.getEndLineNum(), pos.getEndColumnNum() );
    }


    /**
     * Combines this parser position with an array of positions to create a position that spans from the first point in the first to the last point in the other.
     */
    public CqlParserPos plusAll( CqlNode[] nodes ) {
        return plusAll( Arrays.asList( nodes ) );
    }


    /**
     * Combines this parser position with a list of positions.
     */
    public CqlParserPos plusAll( Collection<CqlNode> nodeList ) {
        int line = getLineNum();
        int column = getColumnNum();
        int endLine = getEndLineNum();
        int endColumn = getEndColumnNum();
        return sum( toPos( nodeList ), line, column, endLine, endColumn );
    }


    /**
     * Combines the parser positions of an array of nodes to create a position which spans from the beginning of the first to the end of the last.
     */
    public static CqlParserPos sum( final CqlNode[] nodes ) {
        return sum( toPos( nodes ) );
    }


    private static List<CqlParserPos> toPos( final CqlNode[] nodes ) {
        return new AbstractList<CqlParserPos>() {
            @Override
            public CqlParserPos get( int index ) {
                return nodes[index].getParserPosition();
            }


            @Override
            public int size() {
                return nodes.length;
            }
        };
    }


    private static Iterable<CqlParserPos> toPos( Iterable<CqlNode> nodes ) {
        return Iterables.transform( nodes, CqlNode::getParserPosition );
    }


    /**
     * Combines the parser positions of a list of nodes to create a position which spans from the beginning of the first to the end of the last.
     */
    public static CqlParserPos sum( final List<? extends CqlNode> nodes ) {
        return sum( Lists.transform( nodes, CqlNode::getParserPosition ) );
    }


    /**
     * Combines an iterable of parser positions to create a position which spans from the beginning of the first to the end of the last.
     */
    public static CqlParserPos sum( Iterable<CqlParserPos> poses ) {
        final List<CqlParserPos> list =
                poses instanceof List
                        ? (List<CqlParserPos>) poses
                        : Lists.newArrayList( poses );
        return sum_( list );
    }


    /**
     * Combines a list of parser positions to create a position which spans from the beginning of the first to the end of the last.
     */
    private static CqlParserPos sum_( final List<CqlParserPos> positions ) {
        switch ( positions.size() ) {
            case 0:
                throw new AssertionError();
            case 1:
                return positions.get( 0 );
            default:
                final List<CqlParserPos> poses = new AbstractList<CqlParserPos>() {
                    @Override
                    public CqlParserPos get( int index ) {
                        return positions.get( index + 1 );
                    }


                    @Override
                    public int size() {
                        return positions.size() - 1;
                    }
                };
                final CqlParserPos p = positions.get( 0 );
                return sum( poses, p.lineNumber, p.columnNumber, p.endLineNumber, p.endColumnNumber );
        }
    }


    /**
     * Computes the parser position which is the sum of an array of parser positions and of a parser position represented by (line, column, endLine, endColumn).
     *
     * @param poses Array of parser positions
     * @param line Start line
     * @param column Start column
     * @param endLine End line
     * @param endColumn End column
     * @return Sum of parser positions
     */
    private static CqlParserPos sum( Iterable<CqlParserPos> poses, int line, int column, int endLine, int endColumn ) {
        int testLine;
        int testColumn;
        for ( CqlParserPos pos : poses ) {
            if ( pos == null || pos.equals( CqlParserPos.ZERO ) ) {
                continue;
            }
            testLine = pos.getLineNum();
            testColumn = pos.getColumnNum();
            if ( testLine < line || testLine == line && testColumn < column ) {
                line = testLine;
                column = testColumn;
            }

            testLine = pos.getEndLineNum();
            testColumn = pos.getEndColumnNum();
            if ( testLine > endLine || testLine == endLine && testColumn > endColumn ) {
                endLine = testLine;
                endColumn = testColumn;
            }
        }
        return new CqlParserPos( line, column, endLine, endColumn );
    }


    public boolean overlaps( CqlParserPos pos ) {
        return startsBefore( pos ) && endsAfter( pos )
                || pos.startsBefore( this ) && pos.endsAfter( this );
    }


    private boolean startsBefore( CqlParserPos pos ) {
        return lineNumber < pos.lineNumber
                || lineNumber == pos.lineNumber
                && columnNumber <= pos.columnNumber;
    }


    private boolean endsAfter( CqlParserPos pos ) {
        return endLineNumber > pos.endLineNumber
                || endLineNumber == pos.endLineNumber
                && endColumnNumber >= pos.endColumnNumber;
    }


    public boolean startsAt( CqlParserPos pos ) {
        return lineNumber == pos.lineNumber
                && columnNumber == pos.columnNumber;
    }
}
