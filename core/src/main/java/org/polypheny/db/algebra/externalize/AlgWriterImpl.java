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

package org.polypheny.db.algebra.externalize;


import com.google.common.collect.ImmutableList;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.avatica.util.Spacer;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.util.Pair;


/**
 * Implementation of {@link AlgWriter}.
 */
public class AlgWriterImpl implements AlgWriter {

    protected final PrintWriter pw;
    private final ExplainLevel detailLevel;
    private final boolean withIdPrefix;
    protected final Spacer spacer = new Spacer();
    private final boolean insertFieldNames;
    private final List<Pair<String, Object>> values = new ArrayList<>();


    public AlgWriterImpl( PrintWriter pw ) {
        this( pw, ExplainLevel.EXPPLAN_ATTRIBUTES, true );
    }


    public AlgWriterImpl( PrintWriter pw, ExplainLevel detailLevel, boolean withIdPrefix ) {
        this( pw, detailLevel, withIdPrefix, RuntimeConfig.REL_WRITER_INSERT_FIELD_NAMES.getBoolean() );
    }


    public AlgWriterImpl( PrintWriter pw, ExplainLevel detailLevel, boolean withIdPrefix, boolean insertFieldNames ) {
        this.pw = pw;
        this.detailLevel = detailLevel;
        this.withIdPrefix = withIdPrefix;
        this.insertFieldNames = insertFieldNames;
    }


    protected void explain_( AlgNode alg, List<Pair<String, Object>> values ) {
        List<AlgNode> inputs = alg.getInputs();
        final AlgMetadataQuery mq = alg.getCluster().getMetadataQuery();
        if ( !mq.isVisibleInExplain( alg, detailLevel ) ) {
            // render children in place of this, at same level
            explainInputs( inputs );
            return;
        }

        StringBuilder s = new StringBuilder();
        spacer.spaces( s );
        if ( withIdPrefix ) {
            s.append( alg.getId() ).append( ":" );
        }
        s.append( alg.getAlgTypeName() );
        if ( detailLevel != ExplainLevel.NO_ATTRIBUTES ) {
            int j = 0;
            for ( Pair<String, Object> value : values ) {
                if ( value.right instanceof AlgNode ) {
                    continue;
                }
                if ( j++ == 0 ) {
                    s.append( "(" );
                } else {
                    s.append( ", " );
                }
                s.append( value.left )
                        .append( "=[" )
                        .append( insertFieldNames ? insertFieldNames( alg, value.right ) : value.right )
                        .append( "]" );
            }
            if ( j > 0 ) {
                s.append( ")" );
            }
        }
        switch ( detailLevel ) {
            case ALL_ATTRIBUTES:
                s.append( ": rowcount = " )
                        .append( mq.getTupleCount( alg ) )
                        .append( ", cumulative cost = " )
                        .append( mq.getCumulativeCost( alg ) );
        }
        switch ( detailLevel ) {
            case NON_COST_ATTRIBUTES:
            case ALL_ATTRIBUTES:
                if ( !withIdPrefix ) {
                    // If we didn't print the alg id at the start of the line, print it at the end.
                    s.append( ", id = " ).append( alg.getId() );
                }
                break;
        }
        pw.println( s );
        spacer.add( 2 );
        explainInputs( inputs );
        spacer.subtract( 2 );
    }


    // TODO MV: This is not a nice solution
    private String insertFieldNames( AlgNode alg, Object right ) {
        String str = right.toString();
        if ( str.contains( "$" ) ) {
            int offset = 0;
            for ( AlgNode input : alg.getInputs() ) {
                for ( AlgDataTypeField field : input.getTupleType().getFields() ) {
                    String searchStr = "$" + (offset + field.getIndex());
                    int position = str.indexOf( searchStr );
                    if ( position >= 0
                            && ((str.length() == position + searchStr.length())
                            || (str.length() > position + searchStr.length() && str.charAt( position + searchStr.length() ) != '{')) ) {
                        str = str.replace( searchStr, searchStr + "{" + field.getName() + "}" );
                    }
                }
                offset = input.getTupleType().getFields().size();
            }
        }
        return str;
    }


    private void explainInputs( List<AlgNode> inputs ) {
        for ( AlgNode input : inputs ) {
            input.explain( this );
        }
    }


    @Override
    public final void explain( AlgNode alg, List<Pair<String, Object>> valueList ) {
        explain_( alg, valueList );
    }


    @Override
    public ExplainLevel getDetailLevel() {
        return detailLevel;
    }


    @Override
    public AlgWriter input( String term, AlgNode input ) {
        values.add( Pair.of( term, (Object) input ) );
        return this;
    }


    @Override
    public AlgWriter item( String term, Object value ) {
        values.add( Pair.of( term, value ) );
        return this;
    }


    @Override
    public AlgWriter itemIf( String term, Object value, boolean condition ) {
        if ( condition ) {
            item( term, value );
        }
        return this;
    }


    @Override
    public AlgWriter done( AlgNode node ) {
        assert checkInputsPresentInExplain( node );
        final List<Pair<String, Object>> valuesCopy = ImmutableList.copyOf( values );
        values.clear();
        explain_( node, valuesCopy );
        pw.flush();
        return this;
    }


    private boolean checkInputsPresentInExplain( AlgNode node ) {
        int i = 0;
        if ( values.size() > i && values.get( i ).left.equals( "subset" ) ) {
            ++i;
        }

        if ( values.size() > i && (values.get( i ).left.equals( "model" )) ) {
            ++i;
        }

        for ( AlgNode input : node.getInputs() ) {
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

