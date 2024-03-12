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

package org.polypheny.db.sql.language.validate;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.sql.language.SqlMatchRecognize;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.util.NameMatcher;
import org.polypheny.db.util.NameMatchers;


/**
 * Scope for expressions in a {@code MATCH_RECOGNIZE} clause.
 *
 * Defines variables and uses them as prefix of columns reference.
 */
public class MatchRecognizeScope extends ListScope {

    private static final String STAR = "*";

    private final SqlMatchRecognize matchRecognize;
    private final Set<String> patternVars;


    /**
     * Creates a MatchRecognizeScope.
     */
    public MatchRecognizeScope( SqlValidatorScope parent, SqlMatchRecognize matchRecognize ) {
        super( parent );
        this.matchRecognize = matchRecognize;
        patternVars = NameMatchers.withCaseSensitive( false ).createSet();
        patternVars.add( STAR );
    }


    @Override
    public SqlNode getNode() {
        return matchRecognize;
    }


    public SqlMatchRecognize getMatchRecognize() {
        return matchRecognize;
    }


    public Set<String> getPatternVars() {
        return patternVars;
    }


    public void addPatternVar( String str ) {
        patternVars.add( str );
    }


    @Override
    public Map<String, ScopeChild>
    findQualifyingEntityNames( String columnName, SqlNode ctx, NameMatcher nameMatcher ) {
        final Map<String, ScopeChild> map = new HashMap<>();
        for ( ScopeChild child : children ) {
            final AlgDataType rowType = child.namespace.getTupleType();
            if ( nameMatcher.field( rowType, columnName ) != null ) {
                map.put( STAR, child );
            }
        }
        if ( map.isEmpty() ) {
            return parent.findQualifyingEntityNames( columnName, ctx, nameMatcher );
        }
        return map;
    }


    @Override
    public void resolve( List<String> names, boolean deep, Resolved resolved ) {
        super.resolve( names, deep, resolved );
    }

}
