/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adapter.neo4j.util;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.linq4j.function.Function1;
import org.polypheny.db.adapter.neo4j.NeoRelationalImplementor;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.type.PolyType;

public class Translator extends RexVisitorImpl<String> {


    private final List<AlgDataTypeField> afterFields;
    private final Map<String, String> mapping;
    private final NeoRelationalImplementor implementor;
    private final List<AlgDataTypeField> beforeFields;
    private final String mappingLabel;


    public Translator( AlgDataType afterRowType, AlgDataType beforeRowType, Map<String, String> mapping, NeoRelationalImplementor implementor, @Nullable String mappingLabel ) {
        super( true );
        this.afterFields = afterRowType.getFieldList();
        this.beforeFields = beforeRowType.getFieldList();
        this.mapping = mapping;
        this.implementor = implementor;
        this.mappingLabel = mappingLabel;
    }


    @Override
    public String visitLiteral( RexLiteral literal ) {
        return NeoUtil.rexAsString( literal, mappingLabel, true );
    }


    @Override
    public String visitInputRef( RexInputRef inputRef ) {
        String name = beforeFields.get( inputRef.getIndex() ).getName();
        if ( mapping.containsKey( name ) ) {
            return mapping.get( name );
        }
        name = adjustGraph( inputRef.getType(), name );
        return name;
    }


    private String adjustGraph( AlgDataType type, String name ) {
        switch ( type.getPolyType() ) {
            case NODE:
                return String.format( "(%s)", name );
            case EDGE:
                return String.format( "-[%s]-", name );
        }
        return name;
    }


    @Override
    public String visitLocalRef( RexLocalRef localRef ) {
        String name = afterFields.get( localRef.getIndex() ).getName();
        if ( mapping.containsKey( name ) ) {
            return mapping.get( name );
        }
        return name;
    }


    @Override
    public String visitCorrelVariable( RexCorrelVariable correlVariable ) {
        String name = afterFields.get( correlVariable.id.getId() ).getName();
        if ( mapping.containsKey( name ) ) {
            return mapping.get( name );
        }
        PolyType type = afterFields.get( correlVariable.id.getId() ).getType().getPolyType();
        if ( type == PolyType.NODE ) {
            name = String.format( "(%s)", name );
        }

        return name;
    }


    @Override
    public String visitDynamicParam( RexDynamicParam dynamicParam ) {
        if ( implementor != null ) {
            implementor.addPreparedType( dynamicParam );
            return NeoUtil.asParameter( dynamicParam.getIndex(), true );
        }
        throw new UnsupportedOperationException( "Prepared parameter is not possible without a implementor." );
    }


    @Override
    public String visitCall( RexCall call ) {
        List<String> ops = call.operands.stream().map( o -> o.accept( this ) ).collect( Collectors.toList() );

        Function1<List<String>, String> getter = NeoUtil.getOpAsNeo( call.op.getOperatorName() );
        assert getter != null : "Function is not supported by the Neo4j adapter.";
        return "(" + getter.apply( ops ) + ")";
    }

}
