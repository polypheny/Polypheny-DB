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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.languages.mql.parser;

import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.util.PolyphenyDbParserException;


/**
 * {@link MqlParseException} defines a checked exception corresponding to {@link Parser}.
 */
public class MqlParseException extends NodeParseException implements PolyphenyDbParserException {


    /**
     * Creates a SqlParseException.
     *
     * @param message Message
     * @param pos Position
     * @param expectedTokenSequences Token sequences
     * @param tokenImages Token images
     * @param parserException Parser exception
     */
    public MqlParseException( String message, ParserPos pos, int[][] expectedTokenSequences, String[] tokenImages, Throwable parserException ) {
        super( message, pos, expectedTokenSequences, tokenImages, parserException );
    }

}
