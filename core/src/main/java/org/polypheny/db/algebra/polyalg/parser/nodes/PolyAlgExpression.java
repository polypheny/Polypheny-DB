/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.algebra.polyalg.parser.nodes;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamTag;
import org.polypheny.db.algebra.polyalg.PolyAlgUtils;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgExpressionExtension.ExtensionType;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Operator;

/**
 * One-size-fits-all class for any RexNodes or even literals not wrapped in a RexNode
 */

@Getter
public class PolyAlgExpression extends PolyAlgNode {

    private final List<PolyAlgLiteral> literals;
    private final List<PolyAlgExpression> childExps;
    private final PolyAlgDataType dataType; // the normal data type specification which is indicated by : after literals
    private final Map<ExtensionType, PolyAlgExpressionExtension> extensions;
    @Setter
    private PolyAlgDataType cast; // unlike this.type, this.cast appears in the form CAST(field as INTEGER)


    public PolyAlgExpression(
            @NonNull List<PolyAlgLiteral> literals, List<PolyAlgExpression> childExps, PolyAlgDataType dataType,
            @NonNull List<PolyAlgExpressionExtension> extensions, ParserPos pos ) {
        super( pos );
        if ( literals.isEmpty() ) {
            throw new GenericRuntimeException( "Expression must have at least one literal" );
        }

        this.literals = literals;
        this.childExps = childExps;
        this.dataType = dataType;
        this.extensions = new HashMap<>();
        for ( PolyAlgExpressionExtension ext : extensions ) {
            this.extensions.put( ext.getType(), ext );
        }

    }


    public boolean isCall() {
        // if childExps is an empty list, we have a call with 0 arguments
        return childExps != null;
    }


    public boolean isElementRef() {
        return isCall() && getLiteralsAsString().equals( PolyAlgUtils.ELEMENT_REF_PREFIX );
    }


    public boolean isSingleLiteral() {
        return !isCall() && !hasExtensions() && literals.size() == 1;
    }


    public boolean hasDataType() {
        return dataType != null;
    }


    public boolean hasExtensions() {
        return !extensions.isEmpty();
    }


    public PolyAlgExpressionExtension getExtension( ExtensionType type ) {
        return extensions.get( type );
    }


    /**
     * Return the explicitly stated AlgDataType for this PolyAlgExpression or {@code null} if it has none.
     *
     * @return the stated AlgDataType of this expression
     */
    public AlgDataType getAlgDataType() {
        return dataType == null ? null : dataType.toAlgDataType();
    }


    /**
     * Return the explicitly stated AlgDataType to be used in the CAST operator.
     *
     * @return the stated AlgDataType of this expression
     */
    public AlgDataType getAlgDataTypeForCast() {
        PolyAlgDataType cast = getOnlyChild().getCast();
        if ( cast == null ) {
            throw new GenericRuntimeException( "No AlgDataType to cast to was specified" );
        }
        return cast.toAlgDataType();
    }


    public int toInt( Set<ParamTag> constraints ) {
        if ( !isSingleLiteral() ) {
            throw new GenericRuntimeException( "Not a valid integer: " + this );
        }
        int i = literals.get( 0 ).toInt();
        if ( i < 0 && constraints.contains( ParamTag.NON_NEGATIVE ) ) {
            throw new GenericRuntimeException( "Integer value must not be negative!" );
        }
        return i;
    }


    public Double toDouble( Set<ParamTag> constraints ) {
        if ( !isSingleLiteral() ) {
            throw new GenericRuntimeException( "Not a valid double: " + this );
        }
        double d = literals.get( 0 ).toNumber().doubleValue();
        if ( d < 0 && constraints.contains( ParamTag.NON_NEGATIVE ) ) {
            throw new GenericRuntimeException( "Double value must not be negative!" );
        }
        return d;
    }


    public Number toNumber() {
        if ( !isSingleLiteral() ) {
            throw new GenericRuntimeException( "Not a valid number: " + this );
        }
        return literals.get( 0 ).toNumber();
    }


    public boolean toBoolean() {
        if ( !isSingleLiteral() ) {
            throw new GenericRuntimeException( "Not a valid integer: " + this );
        }
        return literals.get( 0 ).toBoolean();
    }


    public <T extends Enum<T>> T toEnum( Class<T> enumClass ) {
        if ( !isSingleLiteral() ) {
            throw new GenericRuntimeException( "Not a valid enum: " + this );
        }
        return Enum.valueOf( enumClass, literals.get( 0 ).toString() );
    }


    public String toIdentifier() {
        if ( !isSingleLiteral() ) {
            throw new GenericRuntimeException( "Not a valid identifier: " + this );
        }
        return literals.get( 0 ).toUnquotedString();
    }


    public List<String> getLiteralsAsStrings() {
        return literals.stream().map( PolyAlgLiteral::toString ).toList();
    }


    public String getLiteralsAsString() {
        return String.join( " ", getLiteralsAsStrings() );
    }


    public PolyAlgLiteral getLastLiteral() {
        return literals.get( literals.size() - 1 );
    }


    public PolyAlgExpression getOnlyChild() {
        if ( childExps.size() != 1 ) {
            throw new GenericRuntimeException( "Unexpected number of child expressions: " + childExps.size() );
        }
        return childExps.get( 0 );
    }


    public String getDefaultAlias() {
        if ( isCastOperator() ) {
            return getOnlyChild().getDefaultAlias();
        }
        return toString();
    }


    public boolean isCastOperator() {
        if ( isCall() ) {
            try {
                return getOperator( DataModel.RELATIONAL ).getOperatorName() == OperatorName.CAST;
            } catch ( GenericRuntimeException ignored ) {
            }
        }
        return false;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for ( PolyAlgLiteral literal : literals ) {
            sb.append( literal.toUnquotedString() );
        }
        if ( isCall() ) {
            StringJoiner joiner = new StringJoiner( ", " );
            for ( PolyAlgExpression e : childExps ) {
                joiner.add( e.toString() );
            }
            sb.append( "(" ).append( joiner ).append( ")" );
        }
        if ( hasDataType() ) {
            sb.append( ":" ).append( dataType );
        }
        if ( hasExtensions() ) {
            StringJoiner joiner = new StringJoiner( " " );
            for ( PolyAlgExpressionExtension extension : extensions.values() ) {
                joiner.add( extension.toString() );
            }
            sb.append( joiner );
        }
        return sb.toString();
    }


    public Operator getOperator( DataModel model ) {
        String str = getLiteralsAsString();
        String upper = str.toUpperCase( Locale.ROOT );

        Operator op = OperatorRegistry.getFromUniqueName( model, upper );
        if ( op == null ) {
            if ( model == DataModel.RELATIONAL ) {
                // We might have a common operator -> also test GRAPH and DOCUMENT
                op = OperatorRegistry.getFromUniqueName( DataModel.GRAPH, upper );
                if ( op == null ) {
                    op = OperatorRegistry.getFromUniqueName( DataModel.DOCUMENT, upper );
                }
            } else {
                op = OperatorRegistry.getFromUniqueName( DataModel.RELATIONAL, upper ); // generic operator fallback
            }
            if ( op == null ) {
                throw new GenericRuntimeException( "Operator '" + str + "' is not yet supported" );
            }
        }
        return op;
    }


    public AggFunction getAggFunction() {
        String str = getLiteralsAsString();
        OperatorName opName = OperatorName.valueOf( str.toUpperCase( Locale.ROOT ) );
        if ( opName.getClazz() != AggFunction.class ) {
            throw new GenericRuntimeException( "Operator '" + str + "' is not a valid aggregate function" );
        }
        return OperatorRegistry.getAgg( opName );
    }


}

