/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.mql.parser;


import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.mql.MqlNode;
import org.polypheny.db.util.Static;


/**
 * SqlParserPos represents the position of a parsed token within SQL statement text.
 */
public class MqlParserPos implements Serializable {

    /**
     * SqlParserPos representing line one, character one. Use this if the node doesn't correspond to a position in piece of SQL text.
     */
    public static final MqlParserPos ZERO = new MqlParserPos( 0, 0 );

    private static final long serialVersionUID = 1L;

    private final int lineNumber;
    private final int columnNumber;
    private final int endLineNumber;
    private final int endColumnNumber;


    /**
     * Creates a new parser position.
     */
    public MqlParserPos( int lineNumber, int columnNumber ) {
        this( lineNumber, columnNumber, lineNumber, columnNumber );
    }


    /**
     * Creates a new parser range.
     */
    public MqlParserPos( int startLineNumber, int startColumnNumber, int endLineNumber, int endColumnNumber ) {
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
                || o instanceof MqlParserPos
                && this.lineNumber == ((MqlParserPos) o).lineNumber
                && this.columnNumber == ((MqlParserPos) o).columnNumber
                && this.endLineNumber == ((MqlParserPos) o).endLineNumber
                && this.endColumnNumber == ((MqlParserPos) o).endColumnNumber;
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
    public MqlParserPos plus( MqlParserPos pos ) {
        return new MqlParserPos( getLineNum(), getColumnNum(), pos.getEndLineNum(), pos.getEndColumnNum() );
    }


    /**
     * Combines this parser position with an array of positions to create a position that spans from the first point in the first to the last point in the other.
     */
    public MqlParserPos plusAll( MqlNode[] nodes ) {
        return plusAll( Arrays.asList( nodes ) );
    }


    /**
     * Combines this parser position with a list of positions.
     */
    public MqlParserPos plusAll( Collection<MqlNode> nodeList ) {
        int line = getLineNum();
        int column = getColumnNum();
        int endLine = getEndLineNum();
        int endColumn = getEndColumnNum();
        return sum( toPos( nodeList ), line, column, endLine, endColumn );
    }


    /**
     * Combines the parser positions of an array of nodes to create a position which spans from the beginning of the first to the end of the last.
     */
    public static MqlParserPos sum( final MqlNode[] nodes ) {
        return sum( toPos( nodes ) );
    }


    private static List<MqlParserPos> toPos( final MqlNode[] nodes ) {
        return new AbstractList<MqlParserPos>() {
            @Override
            public MqlParserPos get( int index ) {
                return nodes[index].getParserPosition();
            }


            @Override
            public int size() {
                return nodes.length;
            }
        };
    }


    private static Iterable<MqlParserPos> toPos( Iterable<MqlNode> nodes ) {
        return Iterables.transform( nodes, MqlNode::getParserPosition );
    }


    /**
     * Combines the parser positions of a list of nodes to create a position which spans from the beginning of the first to the end of the last.
     */
    public static MqlParserPos sum( final List<? extends MqlNode> nodes ) {
        return sum( Lists.transform( nodes, MqlNode::getParserPosition ) );
    }


    /**
     * Combines an iterable of parser positions to create a position which spans from the beginning of the first to the end of the last.
     */
    public static MqlParserPos sum( Iterable<MqlParserPos> poses ) {
        final List<MqlParserPos> list =
                poses instanceof List
                        ? (List<MqlParserPos>) poses
                        : Lists.newArrayList( poses );
        return sum_( list );
    }


    /**
     * Combines a list of parser positions to create a position which spans from the beginning of the first to the end of the last.
     */
    private static MqlParserPos sum_( final List<MqlParserPos> positions ) {
        switch ( positions.size() ) {
            case 0:
                throw new AssertionError();
            case 1:
                return positions.get( 0 );
            default:
                final List<MqlParserPos> poses = new AbstractList<MqlParserPos>() {
                    @Override
                    public MqlParserPos get( int index ) {
                        return positions.get( index + 1 );
                    }


                    @Override
                    public int size() {
                        return positions.size() - 1;
                    }
                };
                final MqlParserPos p = positions.get( 0 );
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
    private static MqlParserPos sum( Iterable<MqlParserPos> poses, int line, int column, int endLine, int endColumn ) {
        int testLine;
        int testColumn;
        for ( MqlParserPos pos : poses ) {
            if ( pos == null || pos.equals( MqlParserPos.ZERO ) ) {
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
        return new MqlParserPos( line, column, endLine, endColumn );
    }


    public boolean overlaps( MqlParserPos pos ) {
        return startsBefore( pos ) && endsAfter( pos )
                || pos.startsBefore( this ) && pos.endsAfter( this );
    }


    private boolean startsBefore( MqlParserPos pos ) {
        return lineNumber < pos.lineNumber
                || lineNumber == pos.lineNumber
                && columnNumber <= pos.columnNumber;
    }


    private boolean endsAfter( MqlParserPos pos ) {
        return endLineNumber > pos.endLineNumber
                || endLineNumber == pos.endLineNumber
                && endColumnNumber >= pos.endColumnNumber;
    }


    public boolean startsAt( MqlParserPos pos ) {
        return lineNumber == pos.lineNumber
                && columnNumber == pos.columnNumber;
    }

}
