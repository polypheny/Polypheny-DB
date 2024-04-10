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

package org.polypheny.db.algebra.polyalg;


import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Syntax;
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
import org.polypheny.db.util.ValidatorUtil;

public class PolyAlgUtils {

    private static final Pattern CAST_PATTERN;


    static {
        StringJoiner joiner = new StringJoiner( "|", "CAST\\(([^)]+)\\):(", ")" );
        for ( PolyType type : PolyType.values() ) {
            joiner.add( type.getName() );
        }
        CAST_PATTERN = Pattern.compile( joiner.toString() ); // matches group "my_field" in "CAST(my_field):INTEGER"
    }


    public static String appendAlias( String exp, String alias ) {
        if ( alias == null || alias.equals( exp ) || isCastWithSameName( exp, alias ) ) {
            return exp;
        }
        return exp + " AS " + alias;
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
     *
     * @param values the values to be joined
     * @param omitBrackets whether the surrounding brackets in the case of multiple values should be omitted
     * @return a string either representing a list containing all entries of values or a single value if values is of size 1
     */
    public static String joinMultiValued( List<String> values, boolean omitBrackets ) {
        String str = String.join( ", ", values );
        return (omitBrackets || values.size() <= 1) ? str : "[" + str + "]";
    }


    public static List<String> getAuxProjections( AlgNode child, List<String> inputFieldNames, int startIndex ) {
        List<String> projections = new ArrayList<>();
        List<String> names = child.getTupleType().getFieldNames();
        for ( int i = 0; i < names.size(); i++ ) {
            String name = names.get( i );
            String uniqueName = inputFieldNames.get( startIndex + i );
            if ( !name.equals( uniqueName ) ) {
                projections.add( name + " AS " + uniqueName );
            }
        }
        return projections;
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


    public static String digestWithNames( RexNode expr, List<String> inputFieldNames ) {
        return expr.accept( new NameReplacer( inputFieldNames ) );
    }


    public static class NameReplacer implements RexVisitor<String> {

        private final List<String> names;


        public NameReplacer( List<String> names ) {
            this.names = names;
        }


        @Override
        public String visitIndexRef( RexIndexRef inputRef ) {
            return names.get( inputRef.getIndex() );
        }


        @Override
        public String visitLocalRef( RexLocalRef localRef ) {
            return "===LocalRef=== " + localRef;
        }


        @Override
        public String visitLiteral( RexLiteral literal ) {
            return literal.computeDigest( RexDigestIncludeType.OPTIONAL );
        }


        @Override
        public String visitCall( RexCall call ) {
            // This code closely follows call.toString(), but uses the visitor for nested RexNodes

            boolean withType = call.isA( Kind.CAST ) || call.isA( Kind.NEW_SPECIFICATION );
            final StringBuilder sb = new StringBuilder( call.op.getName() );
            if ( (!call.operands.isEmpty()) && (call.op.getSyntax() == Syntax.FUNCTION_ID) ) {
                // Don't print params for empty arg list. For example, we want "SYSTEM_USER", not "SYSTEM_USER()".
            } else {
                sb.append( "(" );
                appendOperands( call, sb );
                sb.append( ")" );
            }
            if ( withType ) {
                sb.append( ":" );
                // NOTE jvs 16-Jan-2005:  for digests, it is very important to use the full type string.
                sb.append( call.type.getFullTypeString() );
            }
            return sb.toString();
        }


        @Override
        public String visitOver( RexOver over ) {
            boolean withType = over.isA( Kind.CAST ) || over.isA( Kind.NEW_SPECIFICATION );
            final StringBuilder sb = new StringBuilder( over.op.getName() );
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
            return "===dynamicParam=== " + dynamicParam;
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
            /* TODO: make sure subquery is parsed correctly
             */
            final StringBuilder sb = new StringBuilder( subQuery.op.getName() );
            sb.append( "(" );
            for ( RexNode operand : subQuery.operands ) {
                sb.append( operand );
                sb.append( ", " );
            }
            sb.append( "{\n" );
            subQuery.alg.buildPolyAlgebra( sb );
            sb.append( "})" );
            return "subQuery: " + sb;
        }


        @Override
        public String visitTableInputRef( RexTableIndexRef fieldRef ) {
            return "===tableInputRef=== " + fieldRef;
        }


        @Override
        public String visitPatternFieldRef( RexPatternFieldRef fieldRef ) {
            return "===patternFieldRef=== " + fieldRef;
        }


        @Override
        public String visitNameRef( RexNameRef nameRef ) {
            return "===nameRef=== " + nameRef;
        }


        @Override
        public String visitElementRef( RexElementRef rexElementRef ) {
            return "===elementRef=== " + rexElementRef;
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
                sb.append( ((RexLiteral) operand).computeDigest( includeType ) );
            }
        }


        private String visitRexWindow( RexWindow window ) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter( sw );
            int clauseCount = 0;
            if ( !window.partitionKeys.isEmpty() ) {
                if ( clauseCount++ > 0 ) {
                    pw.print( ' ' );
                }
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

    }

}
