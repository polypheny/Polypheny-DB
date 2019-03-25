/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
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

package ch.unibas.dmi.dbis.polyphenydb.webui;

import java.util.ArrayList;

public class FormGenerator {
    private String title;
    private String description;
    private ArrayList<FormGroup> groups = new ArrayList<FormGroup>();

    FormGenerator(String title, String description) {
        this.title = title;
        this.description = description;
    }

    FormGenerator withGroup(FormGroup g){
        this.groups.add( g );
        return this;
    }

    public static void main( String[] args ) {
        FormGenerator g = new FormGenerator( "Generator", "Long description" );
        FormGroup g1 = new FormGroup( "Group 1" ).withItem( new FormItem<String>( "firstName", "vorname", "John", "text" ) )
                .withItem( new FormItem<String>( "lastName", "name", "Miller", "text" ) );
        FormGroup g2 = new FormGroup( "Group 2" ).withItem( new FormItem<Integer>( "age", "age", 3, "number" ) )
                .withItem( new FormItem<Integer>( "birthday", "bday", 2019, "umber" ) );
        g.withGroup( g1 ).withGroup( g2 );
        System.out.println(g.groups.size());
    }
}

class FormGroup {
    private String title;
    private ArrayList<FormItem> items = new ArrayList<FormItem>();

    FormGroup(String title){
        this.title = title;
    }

    FormGroup withItem(FormItem f){
        this.items.add( f );
        return this;
    }
}

//todo abstract (toString) / with ConfigValue
class FormItem<T> {
    private String label;
    private T value;
    private String type;//todo enum
    private String key;
    private String validation;//todo enum

    FormItem(String key, String label, T value, String type){
        this.key = key;
        this.label = label;
        this.value = value;
        this.type = type;
    }
}
