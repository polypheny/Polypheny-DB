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

package org.polypheny.db.cypher.ddl;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.cypher.CypherSimpleEither;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.expression.CypherProperty;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.languages.ParserPos;


@Getter
public class CypherCreateIndex extends CypherSchemaCommand {

    private final IndexType type;
    private final boolean replace;
    private final boolean ifNotExists;
    private final boolean isNode;
    private final String indexName;
    private final CypherVariable variable;
    private final StringPos funcName;
    private final CypherVariable funcParam;
    private final List<StringPos> labels;
    private final List<CypherProperty> properties;
    private final CypherSimpleEither options;


    public CypherCreateIndex(
            ParserPos pos,
            IndexType type,
            boolean replace,
            boolean ifNotExists,
            boolean isNode,
            String indexName,
            CypherVariable variable,
            List<StringPos> labels,
            List<CypherProperty> properties,
            CypherSimpleEither options ) {
        super( pos );
        this.type = type;
        this.replace = replace;
        this.ifNotExists = ifNotExists;
        this.isNode = isNode;
        this.indexName = indexName;
        this.variable = variable;
        this.labels = labels;
        this.properties = properties;
        this.options = options;

        this.funcName = null;
        this.funcParam = null;
    }


    public CypherCreateIndex(
            ParserPos pos,
            IndexType type,
            boolean replace,
            boolean ifNotExists,
            boolean isNode,
            String indexName,
            CypherVariable variable,
            StringPos funcName,
            CypherVariable funcParam,
            CypherSimpleEither options ) {
        super( pos );
        this.type = type;
        this.replace = replace;
        this.ifNotExists = ifNotExists;
        this.isNode = isNode;
        this.indexName = indexName;
        this.variable = variable;
        this.funcName = funcName;
        this.funcParam = funcParam;
        this.options = options;

        this.labels = null;
        this.properties = null;
    }


    public CypherCreateIndex(
            ParserPos pos,
            IndexType type,
            StringPos stringPos,
            List<StringPos> properties ) {
        super( pos );
        this.type = type;
        this.indexName = stringPos.getImage();
        this.properties = properties.stream()
                .map( p -> new CypherProperty( new CypherExpression( p.getPos() ), p ) )
                .toList();

        this.replace = false;
        this.variable = null;
        this.ifNotExists = false;
        this.isNode = false;
        this.funcParam = null;
        this.labels = null;
        this.options = null;
        this.funcName = null;
    }


    public <E> CypherCreateIndex(
            ParserPos pos,
            IndexType type,
            boolean replace,
            boolean ifNotExists,
            boolean isNode,
            String indexName,
            CypherVariable variable,
            ImmutableList<StringPos> labels,
            List<CypherProperty> properties,
            CypherSimpleEither options ) {
        super( pos );
        this.type = type;
        this.replace = replace;
        this.ifNotExists = ifNotExists;
        this.isNode = isNode;
        this.indexName = indexName;
        this.variable = variable;
        this.labels = labels;
        this.properties = properties;
        this.options = options;

        this.funcName = null;
        this.funcParam = null;
    }


    public enum IndexType {
        LOOKUP, OLD_SYNTAX, FULL_TEXT, RANGE, TEXT, POINT, DEFAULT, BTREE
    }

}
