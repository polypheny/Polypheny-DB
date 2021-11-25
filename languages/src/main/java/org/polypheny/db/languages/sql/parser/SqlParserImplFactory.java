/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.languages.sql.parser;


import java.io.Reader;
import org.polypheny.db.languages.ParserImpl;
import org.polypheny.db.languages.ParserFactory;
import org.polypheny.db.tools.Planner;


/**
 * Factory for {@link SqlAbstractParserImpl} objects.
 *
 * A parser factory allows you to include a custom parser in {@link Planner} created through {@code Frameworks}.
 */
public interface SqlParserImplFactory extends ParserFactory {

    /**
     * Get the underlying parser implementation.
     *
     * @return {@link SqlAbstractParserImpl} object.
     */
    ParserImpl getParser( Reader stream );

}

