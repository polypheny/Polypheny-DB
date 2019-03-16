/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.validate;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.StructKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlMatchRecognize;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


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
        patternVars = validator.getCatalogReader().nameMatcher().createSet();
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
    findQualifyingTableNames( String columnName, SqlNode ctx, SqlNameMatcher nameMatcher ) {
        final Map<String, ScopeChild> map = new HashMap<>();
        for ( ScopeChild child : children ) {
            final RelDataType rowType = child.namespace.getRowType();
            if ( nameMatcher.field( rowType, columnName ) != null ) {
                map.put( STAR, child );
            }
        }
        switch ( map.size() ) {
            case 0:
                return parent.findQualifyingTableNames( columnName, ctx, nameMatcher );
            default:
                return map;
        }
    }


    @Override
    public void resolve( List<String> names, SqlNameMatcher nameMatcher, boolean deep, Resolved resolved ) {
        if ( patternVars.contains( names.get( 0 ) ) ) {
            final Step path = new EmptyPath().plus( null, 0, null, StructKind.FULLY_QUALIFIED );
            final ScopeChild child = children.get( 0 );
            resolved.found( child.namespace, child.nullable, this, path, names );
            if ( resolved.count() > 0 ) {
                return;
            }
        }
        super.resolve( names, nameMatcher, deep, resolved );
    }

}
