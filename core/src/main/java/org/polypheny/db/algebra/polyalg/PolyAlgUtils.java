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


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexElementRef;
import org.polypheny.db.rex.RexFieldAccess;
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

public class PolyAlgUtils {


    public static String getFieldNameFromIndex( AlgNode alg, int idx ) {
        for ( AlgNode input : alg.getInputs() ) {
            List<AlgDataTypeField> fields = input.getTupleType().getFields();
            if ( idx < fields.size() ) {
                return fields.get( idx ).getName();
            }
            idx += fields.size();
        }
        return Integer.toString( idx );
    }


    public static String appendAlias( String exp, String alias ) {
        if ( alias == null || alias.equals( exp ) ) {
            return exp;
        }
        return exp + " AS " + alias;
    }


    /**
     * Each element in exps is compared with the corresponding element in aliases.
     * If they differ, the alias is appended to the element, separated by the keyword {@code AS}.
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


    public static List<String> getFieldNames( AlgNode context ) {
        if ( context == null ) {
            return List.of();
        }
        return context.getInputs().stream()
                .flatMap( node -> node.getTupleType().getFields().stream() )
                .map( AlgDataTypeField::getName )
                .toList();
    }


    public static String digestWithNames( RexNode expr, AlgNode context ) {
        return expr.accept( new NameReplacer( getFieldNames( context ) ) );
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
            return "LocalRef: " + localRef;
        }


        @Override
        public String visitLiteral( RexLiteral literal ) {
            return literal.toString();
        }


        @Override
        public String visitCall( RexCall call ) {
            return call.toString( this );
        }


        @Override
        public String visitOver( RexOver over ) {
            return over.toString( this );
        }


        @Override
        public String visitCorrelVariable( RexCorrelVariable correlVariable ) {
            return "correlVariable: " + correlVariable;
        }


        @Override
        public String visitDynamicParam( RexDynamicParam dynamicParam ) {
            return "dynamicParam: " + dynamicParam;
        }


        @Override
        public String visitRangeRef( RexRangeRef rangeRef ) {
            return "rangeRef: " + rangeRef;
        }


        @Override
        public String visitFieldAccess( RexFieldAccess fieldAccess ) {
            return "fieldAccess: " + fieldAccess;
        }


        @Override
        public String visitSubQuery( RexSubQuery subQuery ) {
            /* TODO: handling subquery when constructing PolyAlg representation
                in method computeDigest( boolean withType, RexVisitor<String> visitor ),
                sb.append( AlgOptUtil.toString( alg ) );
                */
            return "subQuery: " + subQuery;
        }


        @Override
        public String visitTableInputRef( RexTableIndexRef fieldRef ) {
            return "tableInputRef: " + fieldRef;
        }


        @Override
        public String visitPatternFieldRef( RexPatternFieldRef fieldRef ) {
            return "patternFieldRef: " + fieldRef;
        }


        @Override
        public String visitNameRef( RexNameRef nameRef ) {
            return "nameRef: " + nameRef;
        }


        @Override
        public String visitElementRef( RexElementRef rexElementRef ) {
            return "elementRef: " + rexElementRef;
        }

    }

}
