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

package org.polypheny.db.cypher.ddl;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.languages.ParserPos;


@Getter
public class CypherDropIndex extends CypherSchemaCommand {

    private final String name;
    private final List<StringPos> properties;
    private final boolean ifExists;
    private final ParserPos namePos;


    public CypherDropIndex( ParserPos pos, StringPos namePos, List<StringPos> properties, boolean ifExists ) {
        super( pos );
        this.name = namePos.getImage();
        this.namePos = namePos.getPos();
        this.properties = properties;
        this.ifExists = ifExists;
    }


    public CypherDropIndex( ParserPos pos, String name, boolean ifExists ) {
        super( pos );
        this.name = name;
        this.ifExists = ifExists;
        this.namePos = null;
        this.properties = ImmutableList.of();
    }

}
