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
 */

package org.polypheny.db.algebra.core.document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;


public abstract class DocumentProject extends SingleAlg implements DocumentAlg {


    public Map<String, ? extends RexNode> includes;
    public List<String> excludes;


    /**
     * Creates a {@link DocumentProject}.
     * {@link ModelTrait#DOCUMENT} native node of a project.
     */
    protected DocumentProject( AlgCluster cluster, AlgTraitSet traits, AlgNode input, @NotNull Map<String, ? extends RexNode> includes, @NotNull List<String> excludes ) {
        super( cluster, traits, input );
        this.includes = includes;
        this.excludes = excludes;
        this.rowType = DocumentType.ofIncludes( includes ).ofExcludes( excludes );
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() + "$" + includes.hashCode() + "$" + excludes.hashCode() + getInput().algCompareString();
    }


    @Override
    public DocType getDocType() {
        return DocType.PROJECT;
    }


    @Override
    public AlgNode accept( RexShuttle shuttle ) {
        List<RexNode> exp = this.includes.values().stream().map( p -> (RexNode) p ).collect( Collectors.toList() );
        List<RexNode> exps = shuttle.apply( exp );
        if ( exp == exps ) {
            return this;
        }
        return copy( traitSet, List.of( input ) );
    }


    public RexNode asSingleProject() {
        RexBuilder builder = getCluster().getRexBuilder();
        RexNode doc = RexIndexRef.of( 0, DocumentType.ofId() );
        List<RexNode> nodes = new ArrayList<>();
        nodes.add( doc );
        // null key is replaceRoot
        nodes.add(
                builder.makeLiteral(
                        PolyList.copyOf( includes.keySet().stream().filter( Objects::nonNull ).map( v -> PolyList.copyOf( Arrays.stream( v.split( "\\." ) ).map( PolyString::of ).collect( Collectors.toList() ) ) )
                                .collect( Collectors.toList() ) ),
                        builder.getTypeFactory().createArrayType( builder.getTypeFactory().createPolyType( PolyType.CHAR, 255 ), -1 ), PolyType.ARRAY ) );
        nodes.addAll( includes.entrySet().stream().filter( o -> Objects.nonNull( o.getKey() ) ).map( Entry::getValue ).toList() );

        if ( !includes.isEmpty() ) {
            doc = builder.makeCall( getTupleType(), OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_MERGE ), nodes );

            List<Entry<String, ? extends RexNode>> root = includes.entrySet().stream().filter( obj -> Objects.isNull( obj.getKey() ) ).collect( Collectors.toList() );
            if ( !root.isEmpty() ) {
                return builder.makeCall( getTupleType(), OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_REPLACE_ROOT ), root.get( 0 ).getValue() );
            }
        }

        if ( !excludes.isEmpty() ) {
            doc = builder.makeCall(
                    getTupleType(),
                    OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_REMOVE ),
                    doc,
                    builder.makeArray(
                            builder.getTypeFactory().createArrayType(
                                    builder.getTypeFactory().createArrayType( builder.getTypeFactory().createPolyType( PolyType.CHAR, 255 ), -1 ),
                                    -1 ),
                            excludes.stream().map( value -> PolyList.of( Arrays.stream( value.split( "\\." ) ).map( PolyString::of ).collect( Collectors.toList() ) ) ).collect( Collectors.toList() ) ) );
        }

        return doc;
    }

}
