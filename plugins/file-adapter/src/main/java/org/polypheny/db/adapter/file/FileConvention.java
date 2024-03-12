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

package org.polypheny.db.adapter.file;


import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.file.algebra.FileRules;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.Convention;


@Getter
public class FileConvention extends Convention.Impl {

    private final Expression fileSchemaExpression;
    private final FileSchema fileSchema;
    /**
     * Whether the query is a modification (insert, update, delete) or a select query.
     * Needed for the org.polypheny.db.adapter.file.alg.FileRules.FileUnionRule
     */
    @Setter
    private boolean isModification = false;


    public FileConvention( String name, Expression fileSchemaExpression, FileSchema fileSchema ) {
        super( "FileConvention." + name, FileAlg.class );
        this.fileSchemaExpression = fileSchemaExpression;
        this.fileSchema = fileSchema;
    }


    @Override
    public void register( AlgPlanner planner ) {
        for ( AlgOptRule rule : FileRules.rules( this, FileMethod.EXECUTE.method, fileSchema ) ) {
            planner.addRuleDuringRuntime( rule );
        }
    }

}
