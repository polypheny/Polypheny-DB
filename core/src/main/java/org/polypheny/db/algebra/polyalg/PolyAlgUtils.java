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

package org.polypheny.db.algebra.polyalg;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgMetadata.GlobalStats;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArg;
import org.polypheny.db.algebra.polyalg.arguments.RexArg;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexDigestIncludeType;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexElementRef;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexFieldCollation;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexOver;
import org.polypheny.db.rex.RexPatternFieldRef;
import org.polypheny.db.rex.RexRangeRef;
import org.polypheny.db.rex.RexSubQuery;
import org.polypheny.db.rex.RexTableIndexRef;
import org.polypheny.db.rex.RexVisitor;
import org.polypheny.db.rex.RexWindow;
import org.polypheny.db.rex.RexWindowBound;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Quadruple;
import org.polypheny.db.util.ValidatorUtil;

@Slf4j
public class PolyAlgUtils {

    private static final Pattern CAST_PATTERN;
    public static final String ELEMENT_REF_PREFIX = "$elem";


    static {
        // matches group "my_field" in "CAST(my_field AS INTEGER)"
        CAST_PATTERN = Pattern.compile( "^CAST\\(([^ )]+) AS.+\\)$", Pattern.CASE_INSENSITIVE );
    }


    public static String appendAlias( String exp, String alias ) {
        if ( alias == null || alias.equals( exp ) || isCastWithSameName( exp, alias ) ) {
            return exp;
        }
        String sanitized = sanitizeIdentifier( alias );
        if ( sanitized.equals( exp ) ) {
            return exp;
        }
        return exp + " AS " + sanitized;
    }


    public static String sanitizeIdentifier( String alias ) {
        if ( (alias.startsWith( "'" ) && alias.endsWith( "'" )) || (alias.startsWith( "\"" ) && alias.endsWith( "\"" )) ) {
            return alias;
        }
        if ( alias.matches( "[a-zA-Z#$@öÖäÄüÜàÀçÇáÁèÈíÍîÎóÓòôÔÒíÍëËâÂïÏéÉñÑß.\\d]*" ) ) {
            return alias;
        }
        return "\"" + alias + "\"";
    }


    /**
     * Each element in exps is compared with the corresponding element in aliases.
     * If they differ (and not just by a CAST expression), the alias is appended to the element, separated by the keyword {@code AS}.
     * For example {@code AVG(age) AS average}.
     *
     * @param exps List of strings to be assigned an alias
     * @param aliases List with each element being the alias for the corresponding value in exps
     * @return Copy of the list exps with aliases appended where values differ
     */
    public static List<String> appendAliases( List<String> exps, List<String> aliases ) {
        assert exps.size() == aliases.size();
        List<String> list = new ArrayList<>();
        for ( int i = 0; i < exps.size(); i++ ) {
            list.add( appendAlias( exps.get( i ), aliases.get( i ) ) );
        }
        return list;
    }


    private static boolean isCastWithSameName( String exp, String alias ) {
        Matcher m = CAST_PATTERN.matcher( exp );
        return m.find() && m.group( 1 ).equals( alias );
    }


    /**
     * Joins the values for a multivalued attribute into a single string.
     * If values contains more than one element, the returned string is surrounded with brackets to represent a list.
     * An empty list is indicated with {@code "[]"}.
     *
     * @param values the values to be joined
     * @param omitBrackets whether the surrounding brackets in the case of multiple values should be omitted
     * @return a string either representing a list containing all entries of values or a single value if values is of size 1
     */
    public static String joinMultiValued( List<String> values, boolean omitBrackets ) {
        if ( values.isEmpty() ) {
            return "[]";
        }
        String str = String.join( ", ", values );
        return (omitBrackets || values.size() <= 1) ? str : "[" + str + "]";
    }


    public static String joinMultiValuedWithBrackets( List<String> values ) {
        String str = String.join( ", ", values );
        return "[" + str + "]";
    }


    /**
     * Returns a ListArg (with unpackValues = false) corresponding to the projects argument of an implicit PROJECT operator required to rename the fieldNames
     * of child to the corresponding fieldNames in inputFieldNames.
     * If the projection is non-trivial, the returned ListArg will contain {@code child.getTupleType().getFieldCount()} entries.
     *
     * @param child the child whose implicit projections should be generated
     * @param inputFieldNames the names of the fields of all children after renaming
     * @param startIndex index of the first field of child in inputFieldNames
     * @return ListArg representing the projects argument or null if no projections are required.
     */
    public static ListArg<RexArg> getAuxProjections( AlgNode child, List<String> inputFieldNames, int startIndex ) {
        List<RexNode> from = new ArrayList<>();
        List<String> to = new ArrayList<>();
        List<String> names = child.getTupleType().getFieldNames();
        boolean isTrivial = true;

        for ( int i = 0; i < names.size(); i++ ) {
            String name = names.get( i );
            String uniqueName = inputFieldNames.get( startIndex + i );
            from.add( RexIndexRef.of( i, child.getTupleType() ) );
            to.add( uniqueName );
            if ( name != null && !name.equals( uniqueName ) ) {
                isTrivial = false;
            }
        }
        if ( isTrivial ) {
            return null;
        }
        return new ListArg<>( from, RexArg::new, to, false );
    }


    public static List<String> getInputFieldNamesList( AlgNode context ) {
        if ( context == null ) {
            return List.of();
        }
        return context.getInputs().stream()
                .flatMap( node -> node.getTupleType().getFieldNames().stream() )
                .toList();
    }


    public static List<String> uniquifiedInputFieldNames( AlgNode context ) {
        List<String> names = getInputFieldNamesList( context );
        return ValidatorUtil.uniquify( names, ValidatorUtil.ATTEMPT_SUGGESTER, true );
    }


    public static <T extends PolyAlgArg> List<List<T>> getNestedListArgAsList( ListArg<ListArg> outerListArg ) {
        List<List<T>> outerList = new ArrayList<>();
        for ( List<T> list : outerListArg.map( ListArg::getArgs ) ) {
            if ( list.isEmpty() ) {
                // empty inner lists are not supported
            } else {
                outerList.add( list );
            }
        }
        return outerList;
    }


    public static <T extends PolyAlgArg, E> List<List<E>> getNestedListArgAsList( ListArg<ListArg> outerListArg, Function<T, E> mapper ) {
        List<List<E>> outerList = new ArrayList<>();
        for ( List<T> list : outerListArg.map( ListArg::getArgs ) ) {
            if ( list.isEmpty() ) {
                outerList.add( List.of() );
            } else {
                outerList.add( list.stream().map( mapper ).toList() );
            }
        }
        return outerList;
    }


    public static <T> ImmutableList<ImmutableList<T>> toImmutableNestedList( List<List<T>> nestedList ) {
        ImmutableList.Builder<ImmutableList<T>> builder = ImmutableList.builder();

        for ( List<T> innerList : nestedList ) {
            builder.add( ImmutableList.copyOf( innerList ) );
        }

        return builder.build();
    }


    public static String digestWithNames( RexNode expr, List<String> inputFieldNames ) {
        return expr.accept( new NameReplacer( inputFieldNames ) );
    }


    public static ObjectNode wrapInRename( AlgNode child, ListArg<RexArg> projections, AlgNode context, List<String> inputFieldNames, ObjectMapper mapper, GlobalStats gs ) {
        ObjectNode node = mapper.createObjectNode();
        PolyAlgDeclaration decl = PolyAlgRegistry.getDeclaration( LogicalRelProject.class );
        node.put( "opName", decl.opName );

        ObjectNode argNode = mapper.createObjectNode();
        argNode.put( "type", ParamType.LIST.name() );
        argNode.set( "value", projections.serialize( context, inputFieldNames, mapper ) );

        node.set( "arguments", mapper.createObjectNode().set( decl.getPos( 0 ).getName(), argNode ) );
        node.set( "metadata", PolyAlgMetadata.getMetadataForAuxiliaryNode( mapper ) );

        node.set( "inputs", mapper.createArrayNode().add( child.serializePolyAlgebra( mapper, gs ) ) );
        return node;
    }


    public static PolyPath buildPolyPath( List<PolyNode> nodes, List<Quadruple<PolyDictionary, List<PolyString>, PolyString, EdgeDirection>> edgeArgs ) {
        List<PolyEdge> edges = new ArrayList<>();
        for ( int i = 0; i < edgeArgs.size(); i++ ) {
            Quadruple<PolyDictionary, List<PolyString>, PolyString, EdgeDirection> e = edgeArgs.get( 0 );
            PolyNode source = nodes.get( i );
            PolyNode target = nodes.get( i + 1 );
            edges.add( new PolyEdge( e.a, e.b, source.id, target.id, e.d, e.c ) );
        }
        return PolyPath.create( nodes.stream().map( n -> Pair.of( n.variableName, n ) ).toList(),
                edges.stream().map( e -> Pair.of( e.variableName, e ) ).toList() );
    }


    public static class NameReplacer implements RexVisitor<String> {

        private final List<String> names;


        public NameReplacer( List<String> names ) {
            this.names = names;
        }


        @Override
        public String visitIndexRef( RexIndexRef inputRef ) {
            return sanitizeIdentifier( names.get( inputRef.getIndex() ) );
        }


        @Override
        public String visitLocalRef( RexLocalRef localRef ) {
            String type = localRef.getType().toString();
            if ( type.contains( "[" ) ) {
                // DocumentTypes should probably be handled better
                type = localRef.getType().getPolyType().getTypeName();
            }
            return RexLocalRef.PREFIX + localRef.getIndex() + ":" + type;
        }


        @Override
        public String visitLiteral( RexLiteral literal ) {
            return visitLiteral( literal, RexDigestIncludeType.OPTIONAL );
        }


        @Override
        public String visitCall( RexCall call ) {
            // This code follows call.toString(), but uses the visitor for nested RexNodes

            boolean withType = call.isA( Kind.CAST ) || call.isA( Kind.NEW_SPECIFICATION );
            final StringBuilder sb = new StringBuilder( OperatorRegistry.getUniqueName( call.op ) );
            if ( (!call.operands.isEmpty()) && (call.op.getSyntax() == Syntax.FUNCTION_ID) ) {
                // Don't print params for empty arg list. For example, we want "SYSTEM_USER", not "SYSTEM_USER()".
            } else {
                sb.append( "(" );
                appendOperands( call, sb );
                if ( withType ) {
                    sb.append( " AS " ); // this is different to the syntax of type specification for literals to be closer to SQL syntax
                    sb.append( call.type.getFullTypeString() );
                }
                sb.append( ")" );
            }
            return sb.toString();
        }


        @Override
        public String visitOver( RexOver over ) {
            log.warn( "Serialization is not yet correctly implemented for RexWindow." );
            boolean withType = over.isA( Kind.CAST ) || over.isA( Kind.NEW_SPECIFICATION );
            final StringBuilder sb = new StringBuilder( OperatorRegistry.getUniqueName( over.op ) );
            sb.append( "(" );
            if ( over.isDistinct() ) {
                sb.append( "DISTINCT " );
            }
            appendOperands( over, sb );
            sb.append( ")" );
            if ( withType ) {
                sb.append( ":" );
                sb.append( over.type.getFullTypeString() );
            }
            sb.append( " OVER (" )
                    .append( visitRexWindow( over.getWindow() ) )
                    .append( ")" );
            return sb.toString();
        }


        @Override
        public String visitCorrelVariable( RexCorrelVariable correlVariable ) {
            return correlVariable.getName();
        }


        @Override
        public String visitDynamicParam( RexDynamicParam dynamicParam ) {
            String type = dynamicParam.type.toString();
            if ( type.contains( "[" ) ) {
                // DocumentTypes should probably be handled better
                type = dynamicParam.type.getPolyType().getTypeName();
            }
            return "?" + dynamicParam.getIndex() + ":" + type;
        }


        @Override
        public String visitRangeRef( RexRangeRef rangeRef ) {
            // Regular RexNode trees do not contain this construct
            return rangeRef.toString();
        }


        @Override
        public String visitFieldAccess( RexFieldAccess fieldAccess ) {
            return fieldAccess.getReferenceExpr().accept( this ) + "." + fieldAccess.getField().getName();
        }


        @Override
        public String visitSubQuery( RexSubQuery subQuery ) {
            /* final StringBuilder sb = new StringBuilder( OperatorRegistry.getUniqueName( subQuery.op ) );
            sb.append( "(" );
            for ( RexNode operand : subQuery.operands ) {
                sb.append( operand );
                sb.append( ", " );
            }
            sb.append( "{\n" );
            subQuery.alg.buildPolyAlgebra( sb );
            sb.append( "})" );
            return "subQuery: " + sb; */
            throw new NotImplementedException( "RexSubQuery can not yet be serialized to PolyAlgebra" );
        }


        @Override
        public String visitTableInputRef( RexTableIndexRef fieldRef ) {
            throw new NotImplementedException( "tableInputRef can not yet be serialized to PolyAlgebra" );
        }


        @Override
        public String visitPatternFieldRef( RexPatternFieldRef fieldRef ) {
            throw new NotImplementedException( "patternFieldRef can not yet be serialized to PolyAlgebra" );
        }


        @Override
        public String visitNameRef( RexNameRef nameRef ) {
            String names = String.join( ".", nameRef.getNames() );
            if ( nameRef.getIndex().isPresent() ) {
                return names + "@" + nameRef.getIndex().get();
            }
            return names;
        }


        @Override
        public String visitElementRef( RexElementRef elemRef ) {
            if ( elemRef.type.getPolyType() != PolyType.DOCUMENT ) {
                throw new NotImplementedException( "PolyAlg for RexElementRef is currently only supported for DocumentType" );
            }
            return ELEMENT_REF_PREFIX + "(" + elemRef.getCollectionRef().accept( this ) + ")";
        }


        private void appendOperands( RexCall call, StringBuilder sb ) {
            for ( int i = 0; i < call.operands.size(); i++ ) {
                if ( i > 0 ) {
                    sb.append( ", " );
                }
                RexNode operand = call.operands.get( i );
                if ( !(operand instanceof RexLiteral) ) {
                    sb.append( operand.accept( this ) );
                    continue;
                }
                // Type information might be omitted in certain cases to improve readability
                // For instance, AND/OR arguments should be BOOLEAN, so AND(true, null) is better than AND(true, null:BOOLEAN), and we keep the same info +($0, 2) is better than +($0, 2:BIGINT). Note: if $0 has BIGINT,
                // then 2 is expected to be of BIGINT type as well.
                RexDigestIncludeType includeType = RexDigestIncludeType.OPTIONAL;
                if ( (call.isA( Kind.AND ) || call.isA( Kind.OR )) && operand.getType().getPolyType() == PolyType.BOOLEAN ) {
                    includeType = RexDigestIncludeType.NO_TYPE;
                }
                if ( RexCall.SIMPLE_BINARY_OPS.contains( call.getKind() ) ) {
                    RexNode otherArg = call.operands.get( 1 - i );
                    if ( (!(otherArg instanceof RexLiteral) || ((RexLiteral) otherArg).digestIncludesType() == RexDigestIncludeType.NO_TYPE) && RexCall.equalSansNullability( operand.getType(), otherArg.getType() ) ) {
                        includeType = RexDigestIncludeType.NO_TYPE;
                    }
                }
                sb.append( visitLiteral( (RexLiteral) operand, includeType ) );
            }
        }


        private String visitRexWindow( RexWindow window ) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter( sw );
            int clauseCount = 0;
            if ( !window.partitionKeys.isEmpty() ) {
                clauseCount++;
                pw.print( "PARTITION BY " );
                for ( int i = 0; i < window.partitionKeys.size(); i++ ) {
                    if ( i > 0 ) {
                        pw.print( ", " );
                    }
                    RexNode partitionKey = window.partitionKeys.get( i );
                    pw.print( partitionKey.accept( this ) );
                }
            }
            if ( window.orderKeys.size() > 0 ) {
                if ( clauseCount++ > 0 ) {
                    pw.print( ' ' );
                }
                pw.print( "ORDER BY " );
                for ( int i = 0; i < window.orderKeys.size(); i++ ) {
                    if ( i > 0 ) {
                        pw.print( ", " );
                    }
                    RexFieldCollation orderKey = window.orderKeys.get( i );
                    pw.print( orderKey.toString( this ) );
                }
            }
            if ( window.getLowerBound() == null ) {
                // No ROWS or RANGE clause
            } else if ( window.getUpperBound() == null ) {
                if ( clauseCount++ > 0 ) {
                    pw.print( ' ' );
                }
                if ( window.isRows() ) {
                    pw.print( "ROWS " );
                } else {
                    pw.print( "RANGE " );
                }
                pw.print( visitRexWindowBound( window.getLowerBound() ) );
            } else {
                if ( clauseCount++ > 0 ) {
                    pw.print( ' ' );
                }
                if ( window.isRows() ) {
                    pw.print( "ROWS BETWEEN " );
                } else {
                    pw.print( "RANGE BETWEEN " );
                }
                pw.print( visitRexWindowBound( window.getLowerBound() ) );
                pw.print( " AND " );
                pw.print( visitRexWindowBound( window.getUpperBound() ) );
            }
            return sw.toString();
        }


        private String visitRexWindowBound( RexWindowBound bound ) {
            // at this point it is simply much easier to rely on the toString method of the RexWindowBound subclasses.
            return bound.toString( this );
        }


        private String visitLiteral( RexLiteral literal, RexDigestIncludeType includeType ) {
            PolyValue value = literal.value;
            String str = visitPolyValue( value );
            if ( str == null ) {
                str = literal.computeDigest( includeType );
            }
            return str;
        }


        private String visitPolyValue( PolyValue value ) {
            if ( value.isNode() ) {
                return visitPolyNode( value.asNode(), true );
            } else if ( value.isPath() ) {
                return visitPolyPath( value.asPath(), true );
            } else if ( value.isEdge() ) {
                return visitPolyEdge( value.asEdge(), true );
            } else if ( value.isList() ) {
                return visitPolyList( value.asList() );
            } else if ( value.isDocument() ) {
                return visitPolyDocument( value.asDocument() );
            }
            return null;
        }


        private String visitPolyDocument( PolyDocument document ) {
            return "PolyDocument " + document.toJson();
        }


        private String visitPolyList( PolyList<? extends PolyValue> list ) {
            return "PolyList " + list.toJson();
        }


        private String visitPolyNode( PolyNode node, boolean withPrefix ) {

            String prefix = withPrefix ? "PolyNode " : "";
            return prefix + "(" + visitGraphLabelProps( node.labels, node.properties, node.variableName ) + ")";

        }


        private String visitPolyPath( PolyPath path, boolean withPrefix ) {
            StringBuilder sb = new StringBuilder( withPrefix ? "PolyPath " : "" );
            for ( GraphPropertyHolder holder : path.getPath() ) {
                if ( holder.isNode() ) {
                    sb.append( visitPolyNode( (PolyNode) holder, false ) );
                } else if ( holder.isEdge() ) {
                    sb.append( visitPolyEdge( (PolyEdge) holder, false ) );
                }
            }
            return sb.toString();
        }


        private String visitPolyEdge( PolyEdge edge, boolean withPrefix ) {

            String left = "-", right = "-";
            switch ( edge.direction ) {
                case LEFT_TO_RIGHT -> right += ">";
                case RIGHT_TO_LEFT -> left = "<" + left;
                case NONE -> {
                }
            }

            StringBuilder sb = new StringBuilder();
            if ( withPrefix ) {
                sb.append( "PolyEdge (" ).append( edge.left ).append( ")" );
            }
            sb.append( left );
            String lp = visitGraphLabelProps( edge.labels, edge.properties, edge.variableName );
            if ( !lp.isEmpty() ) {
                sb.append( "[" )
                        .append( lp )
                        .append( "]" );
            }
            sb.append( right );
            if ( withPrefix ) {
                sb.append( "(" ).append( edge.right ).append( ")" );
            }
            return sb.toString();
        }


        private String visitGraphLabelProps( PolyList<PolyString> lbls, PolyDictionary props, PolyString varName ) {
            String name = (varName == null || varName.isNull()) ? "" : varName.toString();
            String labels = String.join( ":", lbls.stream().map( PolyString::toString ).toList() );
            String properties = visitPolyDictionary( props );
            if ( properties.equals( "{}" ) ) {
                properties = "";
            }
            String s = name;

            if ( !labels.isEmpty() ) {
                s += ":" + labels;
            }
            if ( !s.isEmpty() && !properties.isEmpty() ) {
                s += " ";
            }
            s += properties;
            return s;

        }


        private String visitPolyDictionary( PolyDictionary dict ) {
            List<String> propsList = new ArrayList<>();
            for ( Entry<PolyString, PolyValue> entry : dict.map.entrySet() ) {
                PolyValue value = entry.getValue();
                String valueStr = visitPolyValue( value );
                if ( valueStr == null ) {
                    valueStr = switch ( value.type ) {
                        case VARCHAR, CHAR, TEXT -> value.asString().toTypedString( false );
                        default -> value.toString();
                    };
                }
                propsList.add( entry.getKey().toString() + "=" + valueStr );
            }
            return "{" + String.join( ", ", propsList ) + "}";
        }

    }

}
