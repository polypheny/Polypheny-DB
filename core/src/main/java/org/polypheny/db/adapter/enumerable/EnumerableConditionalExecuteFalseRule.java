/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.enumerable;


import lombok.SneakyThrows;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.convert.ConverterRule;
import org.polypheny.db.rel.core.ConditionalExecute;
import org.polypheny.db.rel.core.ConditionalExecute.Condition;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.logical.LogicalConditionalExecute;


public class EnumerableConditionalExecuteFalseRule extends ConverterRule {

    public EnumerableConditionalExecuteFalseRule() {
        super( LogicalConditionalExecute.class,
                lce -> lce.getCondition() == Condition.FALSE,
                Convention.NONE, EnumerableConvention.INSTANCE,
                RelFactories.LOGICAL_BUILDER, "EnumerableConditionalExecuteFalseRule" );
    }


    @SneakyThrows
    @Override
    public RelNode convert( RelNode rel ) {
        ConditionalExecute ce = (ConditionalExecute) rel;
        throw ce.getExceptionClass().getConstructor( String.class ).newInstance( ce.getExceptionMessage() );
    }
}