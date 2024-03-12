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

package org.polypheny.db.sql.language.advise;


import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.parser.SqlAbstractParserImpl;
import org.polypheny.db.sql.language.parser.SqlParser;
import org.polypheny.db.sql.language.validate.SqlValidatorWithHints;
import org.polypheny.db.util.Advisor;
import org.polypheny.db.util.SourceStringReader;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * An assistant which offers hints and corrections to a partially-formed SQL statement. It is used in the SQL editor user-interface.
 */
public class SqlAdvisor implements Advisor {

    public static final Logger LOGGER = PolyphenyDbTrace.PARSER_LOGGER;

    // Flags indicating precision/scale combinations
    private final SqlValidatorWithHints validator;
    private final ParserConfig parserConfig;


    /**
     * Creates a SqlAdvisor with a validator instance and given parser configuration
     *
     * @param validator Validator
     * @param parserConfig parser config
     */
    public SqlAdvisor( SqlValidatorWithHints validator, ParserConfig parserConfig ) {
        this.validator = validator;
        this.parserConfig = parserConfig;
    }


    /**
     * Attempts to parse and validate a SQL statement. Throws the first exception encountered. The error message of this exception is to be displayed on the UI
     *
     * @param sql A user-input sql statement to be validated
     * @return a List of ValidateErrorInfo (null if sql is valid)
     */
    public List<ValidateErrorInfo> validate( String sql ) {
        SqlNode sqlNode;
        List<ValidateErrorInfo> errorList = new ArrayList<>();

        sqlNode = collectParserError( sql, errorList );
        if ( !errorList.isEmpty() ) {
            return errorList;
        }
        try {
            validator.validateSql( sqlNode );
        } catch ( PolyphenyDbContextException e ) {
            ValidateErrorInfo errInfo = new ValidateErrorInfo( e );

            // validator only returns 1 exception now
            errorList.add( errInfo );
            return errorList;
        } catch ( Exception e ) {
            ValidateErrorInfo errInfo =
                    new ValidateErrorInfo(
                            1,
                            1,
                            1,
                            sql.length(),
                            e.getMessage() );

            // parser only returns 1 exception now
            errorList.add( errInfo );
            return errorList;
        }
        return null;
    }



    /**
     * Wrapper function to parse a SQL query (SELECT or VALUES, but not INSERT, UPDATE, DELETE, CREATE, DROP etc.), throwing a {@link NodeParseException} if the statement is not syntactically valid.
     *
     * @param sql SQL statement
     * @return parse tree
     * @throws NodeParseException if not syntactically valid
     */
    protected SqlNode parseQuery( String sql ) throws NodeParseException {
        SqlAbstractParserImpl parserImpl = (SqlAbstractParserImpl) parserConfig.parserFactory().getParser( new SourceStringReader( sql ) );
        Parser parser = new SqlParser( parserImpl, parserConfig );
        return (SqlNode) parser.parseStmt();
    }


    /**
     * Attempts to parse a SQL statement and adds to the errorList if any syntax error is found. This implementation uses {@link SqlParser}. Subclass can re-implement this with a different parser implementation
     *
     * @param sql A user-input sql statement to be parsed
     * @param errorList A {@link List} of error to be added to
     * @return {@link SqlNode } that is root of the parse tree, null if the sql is not valid
     */
    protected SqlNode collectParserError( String sql, List<ValidateErrorInfo> errorList ) {
        try {
            return parseQuery( sql );
        } catch ( NodeParseException e ) {
            ValidateErrorInfo errInfo = new ValidateErrorInfo( e.getPos(), e.getMessage() );

            // parser only returns 1 exception now
            errorList.add( errInfo );
            return null;
        }
    }


    /**
     * An inner class that represents error message text and position info of a validator or parser exception
     */
    public static class ValidateErrorInfo {

        /**
         * 1-based starting line number
         */
        @Getter
        private final int startLineNum;
        /**
         * 1-based starting column number
         */
        @Getter
        private final int startColumnNum;
        /**
         * 1-based end line number
         */
        @Getter
        private final int endLineNum;
        /**
         * 1-based end column number
         */
        @Getter
        private final int endColumnNum;
        private final String errorMsg;


        /**
         * Creates a new ValidateErrorInfo with the position coordinates and an error string.
         *
         * @param startLineNum Start line number
         * @param startColumnNum Start column number
         * @param endLineNum End line number
         * @param endColumnNum End column number
         * @param errorMsg Error message
         */
        public ValidateErrorInfo( int startLineNum, int startColumnNum, int endLineNum, int endColumnNum, String errorMsg ) {
            this.startLineNum = startLineNum;
            this.startColumnNum = startColumnNum;
            this.endLineNum = endLineNum;
            this.endColumnNum = endColumnNum;
            this.errorMsg = errorMsg;
        }


        /**
         * Creates a new ValidateErrorInfo with an PolyphenyDbContextException.
         *
         * @param e Exception
         */
        public ValidateErrorInfo( PolyphenyDbContextException e ) {
            this.startLineNum = e.getPosLine();
            this.startColumnNum = e.getPosColumn();
            this.endLineNum = e.getEndPosLine();
            this.endColumnNum = e.getEndPosColumn();
            this.errorMsg = e.getCause().getMessage();
        }


        /**
         * Creates a new ValidateErrorInfo with a SqlParserPos and an error string.
         *
         * @param pos Error position
         * @param errorMsg Error message
         */
        public ValidateErrorInfo( ParserPos pos, String errorMsg ) {
            this.startLineNum = pos.getLineNum();
            this.startColumnNum = pos.getColumnNum();
            this.endLineNum = pos.getEndLineNum();
            this.endColumnNum = pos.getEndColumnNum();
            this.errorMsg = errorMsg;
        }


        /**
         * @return error message
         */
        public String getMessage() {
            return errorMsg;
        }

    }

}

