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

package ch.unibas.dmi.dbis.polyphenydb.webui.models.requests;


import ch.unibas.dmi.dbis.polyphenydb.webui.models.SortState;
import java.util.Map;


/**
 * Required to parse a request coming from the UI using Gson
 */
public class UIRequest {

    /**
     * The name of the table the data should be fetched from
     */
    public String tableId;

    /**
     * Information about the pagination,
     * what current page should be loaded
     */
    public int currentPage;

    /**
     * Data that should be inserted
     */
    public Map<String, String> data;

    /**
     * For each column: If it should be filtered empty string if it should not be filtered
     */
    public Map<String, String> filter;

    /**
     * For each column: If and how it should be sorted
     */
    public Map<String, SortState> sortState;

}
