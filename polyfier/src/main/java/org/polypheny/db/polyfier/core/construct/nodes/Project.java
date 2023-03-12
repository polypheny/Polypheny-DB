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

package org.polypheny.db.polyfier.core.construct.nodes;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.polyfier.core.construct.model.Column;
import org.polypheny.db.polyfier.core.construct.model.Result;

import java.util.LinkedList;

@Getter
@Setter(AccessLevel.PROTECTED)
public class Project extends Unary {
    private static int idx = 0;

    LinkedList<Column> projectFields;

    protected Project() {
        super( idx++ );
    }

    public static Project project( LinkedList<Column> projectFields ) {
        Project project = new Project();

        project.setOperatorType( OperatorType.PROJECT );

        project.setProjectFields( projectFields );
        project.setTarget( projectFields.get( 0 ).getResult().getNode() );

        Result result = Result.from( project, projectFields );

        project.setResult( result );

        return project;
    }

}
