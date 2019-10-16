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
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.schema;


import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * A Schema representing a bookstore.
 *
 * It contains a single table with various levels/types of nesting, and is used mainly for testing parts of code that rely on nested structures.
 *
 * New authors can be added but attention should be made to update appropriately tests that might fail.
 *
 * The Schema is meant to be used with {@link ch.unibas.dmi.dbis.polyphenydb.adapter.java.ReflectiveSchema} thus all fields, and methods, should be public.
 */
public final class BookstoreSchema {

    public final Author[] authors = {
            new Author( 1,
                    "Victor Hugo",
                    new Place(
                            new Coordinate( BigDecimal.valueOf( 47.24 ), BigDecimal.valueOf( 6.02 ) ),
                            "Besançon",
                            "France" ),
                    Collections.singletonList(
                            new Book( "Les Misérables",
                                    1862,
                                    Collections.singletonList( new Page( 1, "Contents" ) ) ) ) ),
            new Author( 2,
                    "Nikos Kazantzakis",
                    new Place(
                            new Coordinate( BigDecimal.valueOf( 35.3387 ), BigDecimal.valueOf( 25.1442 ) ),
                            "Heraklion",
                            "Greece" ),
                    Arrays.asList(
                            new Book(
                                    "Zorba the Greek",
                                    1946,
                                    Arrays.asList( new Page( 1, "Contents" ), new Page( 2, "Acknowledgements" ) ) ),
                            new Book(
                                    "The Last Temptation of Christ",
                                    1955,
                                    Collections.singletonList( new Page( 1, "Contents" ) ) ) ) ),
            new Author( 3,
                    "Homer",
                    new Place( null, "Ionia", "Greece" ),
                    Collections.emptyList() )
    };


    /**
     *
     */
    public static class Author {

        public final int aid;
        public final String name;
        public final Place birthPlace;
        @ch.unibas.dmi.dbis.polyphenydb.adapter.java.Array(component = Book.class)
        public final List<Book> books;


        public Author( int aid, String name, Place birthPlace, List<Book> books ) {
            this.aid = aid;
            this.name = name;
            this.birthPlace = birthPlace;
            this.books = books;
        }
    }


    /**
     *
     */
    public static class Place {

        public final Coordinate coords;
        public final String city;
        public final String country;


        public Place( Coordinate coords, String city, String country ) {
            this.coords = coords;
            this.city = city;
            this.country = country;
        }

    }


    /**
     *
     */
    public static class Coordinate {

        public final BigDecimal latitude;
        public final BigDecimal longtitude;


        public Coordinate( BigDecimal latitude, BigDecimal longtitude ) {
            this.latitude = latitude;
            this.longtitude = longtitude;
        }
    }


    /**
     *
     */
    public static class Book {

        public final String title;
        public final int publishYear;
        @ch.unibas.dmi.dbis.polyphenydb.adapter.java.Array(component = Page.class)
        public final List<Page> pages;


        public Book( String title, int publishYear, List<Page> pages ) {
            this.title = title;
            this.publishYear = publishYear;
            this.pages = pages;
        }
    }


    /**
     *
     */
    public static class Page {

        public final int pageNo;
        public final String contentType;


        public Page( int pageNo, String contentType ) {
            this.pageNo = pageNo;
            this.contentType = contentType;
        }
    }
}
