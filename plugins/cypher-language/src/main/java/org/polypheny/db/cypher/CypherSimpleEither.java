/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.cypher;

import lombok.Getter;
import org.polypheny.db.languages.ParserPos;

// <Map<String, CypherExpression>, CypherParameter>
@Getter
public class CypherSimpleEither<T, P> extends CypherNode {

    private final T left;
    private final P right;


    protected CypherSimpleEither( ParserPos pos, T left, P right ) {
        super( pos );
        this.left = left;
        this.right = right;
    }


    public static <T, P> CypherSimpleEither<T, P> left( T left ) {
        return new CypherSimpleEither<>( ParserPos.ZERO, left, null );
    }


    public static <T, P> CypherSimpleEither<T, P> right( P param ) {
        return new CypherSimpleEither<>( ParserPos.ZERO, null, param );
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.EITHER;
    }

}
