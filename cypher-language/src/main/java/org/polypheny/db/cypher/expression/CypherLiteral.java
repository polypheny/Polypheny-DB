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

package org.polypheny.db.cypher.expression;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.languages.ParserPos;

@Getter
public class CypherLiteral extends CypherExpression {

    private final Literal literalType;
    private List<StringPos> keys;
    private List<CypherExpression> values;
    private List<CypherExpression> list;
    private String image;
    private boolean negated;
    private String string;


    public CypherLiteral( ParserPos pos, Literal literalType ) {
        super( pos );
        this.literalType = literalType;
    }


    public CypherLiteral( ParserPos pos, Literal literalType, List<CypherExpression> list ) {
        super( pos );
        this.literalType = literalType;
        this.list = list;
    }


    public CypherLiteral( ParserPos pos, Literal literalType, String string ) {
        super( pos );
        this.literalType = literalType;
        this.string = string;
    }


    public CypherLiteral( ParserPos pos, Literal literalType, List<StringPos> keys, List<CypherExpression> values ) {
        super( pos );
        this.literalType = literalType;
        this.keys = keys;
        this.values = values;
    }


    public CypherLiteral( ParserPos pos, Literal literalType, String image, boolean negated ) {
        super( pos );
        this.literalType = literalType;
        this.image = image;
        this.negated = negated;
    }


    public enum Literal {
        TRUE, FALSE, NULL, LIST, MAP, STRING, DOUBLE, DECIMAL, HEX, OCTAL, STAR
    }

}
