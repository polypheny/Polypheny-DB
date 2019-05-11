/*
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
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.webui;


import java.util.regex.Matcher;
import java.util.regex.Pattern;


class MyResponse {

    public static ResponseCreator ok( final String body, final String path ) {
        return ( req, res ) -> {
            res.status( 200 );
            res.type( getResponseType( path ) );

            return body;
        };
    }

    public static ResponseCreator badRequest( final String body ) {
        return ( req, res ) -> {
            res.status( 400 );
            res.type( "text/html" );

            return body;
        };
    }

    private static String getResponseType( final String file ) {
        Pattern pattern = Pattern.compile( "\\.(\\w+)$" );
        Matcher m = pattern.matcher( file );

        m.find();
        String g = m.group( 1 );
        switch ( g ) {
            case "svg":
                return "image/svg+xml";
            case "ttf":
                return "application/octet-stream";

            //todo woff mime type
            //https://stackoverflow.com/questions/5128069/what-is-the-mime-type-for-ttf-files
            case "woff2":
            case "woff":
                //return "font/" + g;
                //return "application/x-font-" + g;
                //return "application/octet-stream";
                return "application/font-" + g;
            case "ico":
                return "image/x-icon";
            case "js":
            case "css":
                return "text/" + g;
            default:
                System.err.println( "Response Type for " + g + " not handled yet." );
                return "text/" + g;
        }
    }

}
