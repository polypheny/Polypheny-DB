/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
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
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.externalize;


import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelWriter;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import com.google.common.collect.ImmutableList;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.avatica.util.Spacer;
import org.apache.calcite.linq4j.Ord;


/**
 * Implementation of {@link RelWriter}.
 */
public class RelWriterImpl implements RelWriter {

    protected final PrintWriter pw;
    private final SqlExplainLevel detailLevel;
    private final boolean withIdPrefix;
    protected final Spacer spacer = new Spacer();
    private final boolean insertFieldNames;
    private final List<Pair<String, Object>> values = new ArrayList<>();


    public RelWriterImpl( PrintWriter pw ) {
        this( pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES, true );
    }


    public RelWriterImpl( PrintWriter pw, SqlExplainLevel detailLevel, boolean withIdPrefix ) {
        this( pw, detailLevel, withIdPrefix, RuntimeConfig.REL_WRITER_INSERT_FIELD_NAMES.getBoolean() );
    }


    public RelWriterImpl( PrintWriter pw, SqlExplainLevel detailLevel, boolean withIdPrefix, boolean insertFieldNames ) {
        this.pw = pw;
        this.detailLevel = detailLevel;
        this.withIdPrefix = withIdPrefix;
        this.insertFieldNames = insertFieldNames;
    }


    protected void explain_( RelNode rel, List<Pair<String, Object>> values ) {
        List<RelNode> inputs = rel.getInputs();
        final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
        if ( !mq.isVisibleInExplain( rel, detailLevel ) ) {
            // render children in place of this, at same level
            explainInputs( inputs );
            return;
        }

        StringBuilder s = new StringBuilder();
        spacer.spaces( s );
        if ( withIdPrefix ) {
            s.append( rel.getId() ).append( ":" );
        }
        s.append( rel.getRelTypeName() );
        if ( detailLevel != SqlExplainLevel.NO_ATTRIBUTES ) {
            int j = 0;
            for ( Pair<String, Object> value : values ) {
                if ( value.right instanceof RelNode ) {
                    continue;
                }
                if ( j++ == 0 ) {
                    s.append( "(" );
                } else {
                    s.append( ", " );
                }
                s.append( value.left )
                        .append( "=[" )
                        .append( insertFieldNames ? insertFieldNames( rel, value.right ) : value.right )
                        .append( "]" );
            }
            if ( j > 0 ) {
                s.append( ")" );
            }
        }
        switch ( detailLevel ) {
            case ALL_ATTRIBUTES:
                s.append( ": rowcount = " )
                        .append( mq.getRowCount( rel ) )
                        .append( ", cumulative cost = " )
                        .append( mq.getCumulativeCost( rel ) );
        }
        switch ( detailLevel ) {
            case NON_COST_ATTRIBUTES:
            case ALL_ATTRIBUTES:
                if ( !withIdPrefix ) {
                    // If we didn't print the rel id at the start of the line, print it at the end.
                    s.append( ", id = " ).append( rel.getId() );
                }
                break;
        }
        pw.println( s );
        spacer.add( 2 );
        explainInputs( inputs );
        spacer.subtract( 2 );
    }


    // TODO MV: This is not a nice solution
    private String insertFieldNames( RelNode rel, Object right ) {
        String str = right.toString();
        if ( str.contains( "$" ) ) {
            int offset = 0;
            for ( RelNode input : rel.getInputs() ) {
                for ( RelDataTypeField field : input.getRowType().getFieldList() ) {
                    String searchStr = "$" + (offset + field.getIndex());
                    int position = str.indexOf( searchStr );
                    if ( position >= 0
                            && ((str.length() == position + searchStr.length())
                            || (str.length() > position + searchStr.length() && str.charAt( position + searchStr.length() ) != '{')) ) {
                        str = str.replace( searchStr, searchStr + "{" + field.getName() + "}" );
                    }
                }
                offset = input.getRowType().getFieldList().size();
            }
        }
        return str;
    }


    private void explainInputs( List<RelNode> inputs ) {
        for ( RelNode input : inputs ) {
            input.explain( this );
        }
    }


    @Override
    public final void explain( RelNode rel, List<Pair<String, Object>> valueList ) {
        explain_( rel, valueList );
    }


    @Override
    public SqlExplainLevel getDetailLevel() {
        return detailLevel;
    }


    @Override
    public RelWriter input( String term, RelNode input ) {
        values.add( Pair.of( term, (Object) input ) );
        return this;
    }


    @Override
    public RelWriter item( String term, Object value ) {
        values.add( Pair.of( term, value ) );
        return this;
    }


    @Override
    public RelWriter itemIf( String term, Object value, boolean condition ) {
        if ( condition ) {
            item( term, value );
        }
        return this;
    }


    @Override
    public RelWriter done( RelNode node ) {
        assert checkInputsPresentInExplain( node );
        final List<Pair<String, Object>> valuesCopy = ImmutableList.copyOf( values );
        values.clear();
        explain_( node, valuesCopy );
        pw.flush();
        return this;
    }


    private boolean checkInputsPresentInExplain( RelNode node ) {
        int i = 0;
        if ( values.size() > 0 && values.get( 0 ).left.equals( "subset" ) ) {
            ++i;
        }
        for ( RelNode input : node.getInputs() ) {
            assert values.get( i ).right == input;
            ++i;
        }
        return true;
    }


    @Override
    public boolean nest() {
        return false;
    }


    /**
     * Converts the collected terms and values to a string. Does not write to the parent writer.
     */
    public String simple() {
        final StringBuilder buf = new StringBuilder( "(" );
        for ( Ord<Pair<String, Object>> ord : Ord.zip( values ) ) {
            if ( ord.i > 0 ) {
                buf.append( ", " );
            }
            buf.append( ord.e.left ).append( "=[" ).append( ord.e.right ).append( "]" );
        }
        buf.append( ")" );
        return buf.toString();
    }
}

