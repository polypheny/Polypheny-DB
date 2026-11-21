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

package org.polypheny.db.cypher.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.polypheny.db.cypher.CypherParameter;
import org.polypheny.db.cypher.CypherStatement;
import org.polypheny.db.languages.ParserImpl;
import org.polypheny.db.type.entity.PolyValue;

public abstract class CypherAbstractParserImpl implements ParserImpl {

    private final List<CypherParameter> parameters = new ArrayList<>();
    private Map<String, PolyValue> bindings = null;


    List<CypherParameter> getParameters() {
        return List.copyOf( parameters );
    }


    void setBindings( Map<String, PolyValue> bindings ) {
        this.bindings = bindings;
    }


    protected CypherParameter registerParameter( CypherParameter parameter ) {
        parameters.add( parameter );
        return bindings == null ? parameter : parameter.withValue( bindings.get( parameter.getName() ) );
    }


    public abstract List<CypherStatement> parseExpressionsEof() throws Exception;

}
