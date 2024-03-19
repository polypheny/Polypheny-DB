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
import org.polypheny.db.rex.RexNode;

public class PolyAlgUtils {

    /**
     * Replaces all occurrences of '$' followed by some number corresponding to a field in the inputs of alg
     * with the name of that field.
     *
     * @param alg the AlgNode whose inputs contain the field names
     * @param str String whose occurrences of '$' + field number should be replaced
     * @return str with field numbers replaced with their names.
     */
    public static String replaceWithFieldNames( AlgNode alg, String str ) {
        if ( str.contains( "$" ) ) {
            int offset = 0;
            for ( AlgNode input : alg.getInputs() ) {
                for ( AlgDataTypeField field : input.getTupleType().getFields() ) {
                    String searchStr = "$" + (offset + field.getIndex());
                    int position = str.indexOf( searchStr );
                    if ( position >= 0 && (str.length() >= position + searchStr.length()) ) {
                        str = str.replace( searchStr, field.getName() );
                    }
                }
                offset = input.getTupleType().getFields().size();
            }
        }
        return str;
    }


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


    public static List<String> replaceWithFieldNames( AlgNode alg, List<?> exps ) {
        return exps.stream()
                .map( exp -> replaceWithFieldNames( alg, exp.toString() ) )
                .toList();
    }


    public static String appendAlias( String exp, String alias ) {
        if ( !alias.equals( exp ) ) {
            exp += " AS " + alias;
        }
        return exp;
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
     * @return a string either representing a list containing or values or a single value if values is of size 1.
     */
    public static String joinMultiValued( List<String> values ) {
        if ( values.isEmpty() ) {
            return "";
        }
        if ( values.size() == 1 ) {
            return values.get( 0 );
        }

        return "[" + String.join( ", ", values ) + "]";
    }

}
