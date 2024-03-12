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

package org.polypheny.db.processing;

import java.util.List;
import java.util.function.Function;
import lombok.Value;
import org.polypheny.db.type.entity.PolyValue;

@Value
public class LanguageContext {

    Function<QueryContext, List<QueryContext>> splitter;
    Function<QueryContext, ImplementationContext> toIterator;
    Function<ImplementationContext, List<List<PolyValue>>> toPolyValue;

}
