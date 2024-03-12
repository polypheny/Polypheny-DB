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

package org.polypheny.db.adapter.file.source;


import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.file.FileConvention;
import org.polypheny.db.adapter.file.FileMethod;
import org.polypheny.db.adapter.file.FileSchema;
import org.polypheny.db.adapter.file.algebra.FileRules;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgPlanner;


public class QfsConvention extends FileConvention {

    @Getter
    private final Expression fileSchemaExpression;


    public QfsConvention( String name, Expression fileSchemaExpression, FileSchema fileSchema ) {
        super( "QfsConvention." + name, fileSchemaExpression, fileSchema );
        this.fileSchemaExpression = fileSchemaExpression;
    }


    @Override
    public void register( AlgPlanner planner ) {
        for ( AlgOptRule rule : FileRules.rules( this, FileMethod.EXECUTE_QFS.method, getFileSchema() ) ) {
            planner.addRule( rule );
        }
    }

}
