/*
 * Copyright 2019-2021 The Polypheny Project
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


import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.XmlOutput;


/**
 * Callback for a relational expression to dump in XML format.
 */
public class AlgXmlWriter extends AlgWriterImpl {

    private final XmlOutput xmlOutput;
    boolean generic = true;

    // TODO jvs 23-Dec-2005:  honor detail level.  The current inheritance structure makes this difficult without duplication; need to factor out the filtering of attributes before rendering.


    public AlgXmlWriter( PrintWriter pw, ExplainLevel detailLevel ) {
        super( pw, detailLevel, true );
        xmlOutput = new XmlOutput( pw );
        xmlOutput.setGlob( true );
        xmlOutput.setCompact( false );
    }


    @Override
    protected void explain_( AlgNode alg, List<Pair<String, Object>> values ) {
        if ( generic ) {
            explainGeneric( alg, values );
        } else {
            explainSpecific( alg, values );
        }
    }


    /**
     * Generates generic XML (sometimes called 'element-oriented XML'). Like this:
     *
     * <blockquote>
     * <code>
     * &lt;{@link AlgNode} id="1" type="Join"&gt;<br>
     * &nbsp;&nbsp;&lt;Property name="condition"&gt;EMP.DEPTNO =
     * DEPT.DEPTNO&lt;/Property&gt;<br>
     * &nbsp;&nbsp;&lt;Inputs&gt;<br>
     * &nbsp;&nbsp;&nbsp;&nbsp;&lt;{@link AlgNode} id="2" type="Project"&gt;<br>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;Property name="expr1"&gt;x +
     * y&lt;/Property&gt;<br>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;Property
     * name="expr2"&gt;45&lt;/Property&gt;<br>
     * &nbsp;&nbsp;&nbsp;&nbsp;&lt;/AlgNode&gt;<br>
     * &nbsp;&nbsp;&nbsp;&nbsp;&lt;{@link AlgNode} id="3" type="TableAccess"&gt;<br>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;Property
     * name="table"&gt;SALES.EMP&lt;/Property&gt;<br>
     * &nbsp;&nbsp;&nbsp;&nbsp;&lt;/AlgNode&gt;<br>
     * &nbsp;&nbsp;&lt;/Inputs&gt;<br>
     * &lt;/AlgNode&gt;</code>
     * </blockquote>
     *
     * @param alg Relational expression
     * @param values List of term-value pairs
     */
    private void explainGeneric( AlgNode alg, List<Pair<String, Object>> values ) {
        String algType = alg.getAlgTypeName();
        xmlOutput.beginBeginTag( "AlgNode" );
        xmlOutput.attribute( "type", algType );

        xmlOutput.endBeginTag( "AlgNode" );

        final List<AlgNode> inputs = new ArrayList<>();
        for ( Pair<String, Object> pair : values ) {
            if ( pair.right instanceof AlgNode ) {
                inputs.add( (AlgNode) pair.right );
                continue;
            }
            if ( pair.right == null ) {
                continue;
            }
            xmlOutput.beginBeginTag( "Property" );
            xmlOutput.attribute( "name", pair.left );
            xmlOutput.endBeginTag( "Property" );
            xmlOutput.cdata( pair.right.toString() );
            xmlOutput.endTag( "Property" );
        }
        xmlOutput.beginTag( "Inputs", null );
        spacer.add( 2 );
        for ( AlgNode input : inputs ) {
            input.explain( this );
        }
        spacer.subtract( 2 );
        xmlOutput.endTag( "Inputs" );
        xmlOutput.endTag( "AlgNode" );
    }


    /**
     * Generates specific XML (sometimes called 'attribute-oriented XML'). Like this:
     *
     * <blockquote><pre>
     * &lt;Join condition="EMP.DEPTNO = DEPT.DEPTNO"&gt;
     *   &lt;Project expr1="x + y" expr2="42"&gt;
     *   &lt;TableAccess table="SALES.EMPS"&gt;
     * &lt;/Join&gt;
     * </pre></blockquote>
     *
     * @param alg Relational expression
     * @param values List of term-value pairs
     */
    private void explainSpecific( AlgNode alg, List<Pair<String, Object>> values ) {
        String tagName = alg.getAlgTypeName();
        xmlOutput.beginBeginTag( tagName );
        xmlOutput.attribute( "id", alg.getId() + "" );

        for ( Pair<String, Object> value : values ) {
            if ( value.right instanceof AlgNode ) {
                continue;
            }
            xmlOutput.attribute( value.left, value.right.toString() );
        }
        xmlOutput.endBeginTag( tagName );
        spacer.add( 2 );
        for ( AlgNode input : alg.getInputs() ) {
            input.explain( this );
        }
        spacer.subtract( 2 );
    }

}

