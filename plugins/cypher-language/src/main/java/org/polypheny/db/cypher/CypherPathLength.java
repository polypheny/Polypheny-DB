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

@Getter
public class CypherPathLength extends CypherNode {


    private ParserPos fromPos;
    private ParserPos toPos;
    private String from;
    private String to;


    public CypherPathLength( ParserPos pos, ParserPos fromPos, ParserPos toPos, String from, String to ) {
        super( pos );
        this.fromPos = fromPos;
        this.toPos = toPos;
        this.from = from;
        this.to = to;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.PATH_LENGTH;
    }

}
