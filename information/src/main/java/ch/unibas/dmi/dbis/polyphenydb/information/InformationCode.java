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

package ch.unibas.dmi.dbis.polyphenydb.information;


import java.util.UUID;


/**
 * An Information object containing code that will be rendered in an ace editor in the UI
 */
public class InformationCode extends Information {

    @SuppressWarnings("unused")
    private String code;
    @SuppressWarnings("unused")
    private String language;


    /**
     * Constructor
     *
     * @param groupId The group this information element belongs to
     * @param code Code that should be rendered in an ace editor in the UI
     */
    public InformationCode( final String groupId, final String code ) {
        this( UUID.randomUUID().toString(), groupId, code, "java" );
    }


    /**
     * Constructor
     *
     * @param group The group this information element belongs to
     * @param code Code that should be rendered in an ace editor in the UI
     */
    public InformationCode( final InformationGroup group, final String code ) {
        this( UUID.randomUUID().toString(), group.getId(), code, "java" );
    }


    /**
     * Constructor
     *
     * @param group The group this information element belongs to
     * @param code Code that should be rendered in an ace editor in the UI
     * @param language The language for the ace syntax highlighting
     */
    public InformationCode( final InformationGroup group, final String code, final String language ) {
        this( UUID.randomUUID().toString(), group.getId(), code, language );
    }


    /**
     * Constructor
     *
     * @param id The id of this information element
     * @param groupId The group this information element belongs to
     * @param code Code that should be rendered in an ace editor in the UI
     * @param language The language for the ace syntax highlighting
     */
    public InformationCode( final String id, final String groupId, final String code, final String language ) {
        super( id, groupId );
        this.code = code;
        this.language = language;
    }


    /**
     * Update the content of an InformationCode object.
     *
     * @param code The code to set for this element
     */
    public void updateCode( final String code ) {
        this.code = code;
        notifyManager();
    }

}
