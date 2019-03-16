/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.pretty;


/**
 * Data structure to hold options for {@link SqlPrettyWriter#setFormatOptions(SqlFormatOptions)}.
 */
public class SqlFormatOptions {

    private boolean alwaysUseParentheses = false;
    private boolean caseClausesOnNewLines = false;
    private boolean clauseStartsLine = true;
    private boolean keywordsLowercase = false;
    private boolean quoteAllIdentifiers = true;
    private boolean selectListItemsOnSeparateLines = false;
    private boolean whereListItemsOnSeparateLines = false;
    private boolean windowDeclarationStartsLine = true;
    private boolean windowListItemsOnSeparateLines = true;
    private int indentation = 4;
    private int lineLength = 0;


    /**
     * Constructs a set of default SQL format options.
     */
    public SqlFormatOptions() {
        super();
    }


    /**
     * Constructs a complete set of SQL format options.
     *
     * @param alwaysUseParentheses Always use parentheses
     * @param caseClausesOnNewLines Case clauses on new lines
     * @param clauseStartsLine Clause starts line
     * @param keywordsLowercase Keywords in lower case
     * @param quoteAllIdentifiers Quote all identifiers
     * @param selectListItemsOnSeparateLines Select items on separate lines
     * @param whereListItemsOnSeparateLines Where items on separate lines
     * @param windowDeclarationStartsLine Window declaration starts line
     * @param windowListItemsOnSeparateLines Window list items on separate lines
     * @param indentation Indentation
     * @param lineLength Line length
     */
    public SqlFormatOptions(
            boolean alwaysUseParentheses,
            boolean caseClausesOnNewLines,
            boolean clauseStartsLine,
            boolean keywordsLowercase,
            boolean quoteAllIdentifiers,
            boolean selectListItemsOnSeparateLines,
            boolean whereListItemsOnSeparateLines,
            boolean windowDeclarationStartsLine,
            boolean windowListItemsOnSeparateLines,
            int indentation,
            int lineLength ) {
        this();
        this.alwaysUseParentheses = alwaysUseParentheses;
        this.caseClausesOnNewLines = caseClausesOnNewLines;
        this.clauseStartsLine = clauseStartsLine;
        this.keywordsLowercase = keywordsLowercase;
        this.quoteAllIdentifiers = quoteAllIdentifiers;
        this.selectListItemsOnSeparateLines = selectListItemsOnSeparateLines;
        this.whereListItemsOnSeparateLines = whereListItemsOnSeparateLines;
        this.windowDeclarationStartsLine = windowDeclarationStartsLine;
        this.windowListItemsOnSeparateLines = windowListItemsOnSeparateLines;
        this.indentation = indentation;
        this.lineLength = lineLength;
    }


    public boolean isAlwaysUseParentheses() {
        return alwaysUseParentheses;
    }


    public void setAlwaysUseParentheses( boolean alwaysUseParentheses ) {
        this.alwaysUseParentheses = alwaysUseParentheses;
    }


    public boolean isCaseClausesOnNewLines() {
        return caseClausesOnNewLines;
    }


    public void setCaseClausesOnNewLines( boolean caseClausesOnNewLines ) {
        this.caseClausesOnNewLines = caseClausesOnNewLines;
    }


    public boolean isClauseStartsLine() {
        return clauseStartsLine;
    }


    public void setClauseStartsLine( boolean clauseStartsLine ) {
        this.clauseStartsLine = clauseStartsLine;
    }


    public boolean isKeywordsLowercase() {
        return keywordsLowercase;
    }


    public void setKeywordsLowercase( boolean keywordsLowercase ) {
        this.keywordsLowercase = keywordsLowercase;
    }


    public boolean isQuoteAllIdentifiers() {
        return quoteAllIdentifiers;
    }


    public void setQuoteAllIdentifiers( boolean quoteAllIdentifiers ) {
        this.quoteAllIdentifiers = quoteAllIdentifiers;
    }


    public boolean isSelectListItemsOnSeparateLines() {
        return selectListItemsOnSeparateLines;
    }


    public void setSelectListItemsOnSeparateLines( boolean selectListItemsOnSeparateLines ) {
        this.selectListItemsOnSeparateLines = selectListItemsOnSeparateLines;
    }


    public boolean isWhereListItemsOnSeparateLines() {
        return whereListItemsOnSeparateLines;
    }


    public void setWhereListItemsOnSeparateLines( boolean whereListItemsOnSeparateLines ) {
        this.whereListItemsOnSeparateLines = whereListItemsOnSeparateLines;
    }


    public boolean isWindowDeclarationStartsLine() {
        return windowDeclarationStartsLine;
    }


    public void setWindowDeclarationStartsLine( boolean windowDeclarationStartsLine ) {
        this.windowDeclarationStartsLine = windowDeclarationStartsLine;
    }


    public boolean isWindowListItemsOnSeparateLines() {
        return windowListItemsOnSeparateLines;
    }


    public void setWindowListItemsOnSeparateLines( boolean windowListItemsOnSeparateLines ) {
        this.windowListItemsOnSeparateLines = windowListItemsOnSeparateLines;
    }


    public int getLineLength() {
        return lineLength;
    }


    public void setLineLength( int lineLength ) {
        this.lineLength = lineLength;
    }


    public int getIndentation() {
        return indentation;
    }


    public void setIndentation( int indentation ) {
        this.indentation = indentation;
    }
}

