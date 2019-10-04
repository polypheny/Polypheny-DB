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

package ch.unibas.dmi.dbis.polyphenydb.schema;


import lombok.AllArgsConstructor;


public class FoodmartSchema {

    public final SalesFact[] sales_fact_1997 = {
            new SalesFact( 12321, 1, 1, 22, 12, 1, 67, 1 ),
    };

    public final Product[] product = {
            new Product( "soap dispenser", 1, 12321, "The fancy soap company", "Dispenser 101", 21, 89, 1020, 950, true, false, 1, 42, 12, 10, 5 ),
    };

    public final Customer[] customer = {
            new Customer( 1, 1221, "Mayer", "Hans-Rudi", "Jacob", "Im Gässli 2", "", "", "", "Basel", "Basel-Stadt", 4001, "Switzerland", 24, "053151662", "07151562", "3.6.1981", 1, 180000, 0, 2, 2, 3, "5.1.2018", true, 4, true, 2 ),
    };

    public final ProductClass[] product_class = {
            new ProductClass( 1, 1, 3, 1, 1 ),
    };

    public final Store[] store = {
            new Store( 1, 3, 2, "Basel Mega Store", 1, "Mega Store Strasse 1", "Basel", "Basel-Stadt", 4001, "Switzerland", 1, "0622625512", "05236462", "1.1.2005", "1.1.2017", 1500, 800, 200, 500, true, true, true, true, true ),
    };

    public final Department[] department = {
            new Department( 1, "IT" ),
    };

    public final Employee[] employee = {
            new Employee( 1, "Hans Müller", "Hans", "Müller", 23, "Head of something", 1, 1, "23.6.1979", "5.9.1999", "", 6500, 2, 3, 1, 0, "Mgn" ),
    };


    @AllArgsConstructor
    public static class SalesFact {

        public final int product_id;
        public final int time_id;
        public final int customer_id;
        public final int promotion_id;
        public final int store_id;
        public final int store_sales;
        public final int store_cost;
        public final int unit_sales;
    }


    @AllArgsConstructor
    public static class Customer {

        public final int customer_id;
        public final int account_num;
        public final String lname;
        public final String fname;
        public final String mi;
        public final String address1;
        public final String address2;
        public final String address3;
        public final String address4;
        public final String city;
        public final String state_province;
        public final int postal_code;
        public final String country;
        public final int customer_region_id;
        public final String phone1;
        public final String phone2;
        public final String birthdate;
        public final int marital_status;
        public final int yearly_income;
        public final int gender;
        public final int total_children;
        public final int num_children_at_home;
        public final int education;
        public final String date_accnt_opened;
        public final boolean member_card;
        public final int occupation;
        public final boolean houseowner;
        public final int num_cars_owned;
    }


    @AllArgsConstructor
    public static class Product {

        public final String fullname;
        public final int product_class_id;
        public final int product_id;
        public final String brand_name;
        public final String product_nam;
        public final int SKU; // Stock Keeping Unit
        public final int SRP; // Suggested Retail Price
        public final int gross_weight;
        public final int net_weight;
        public final boolean recyclable_package;
        public final boolean low_fat;
        public final int units_per_case;
        public final int cases_per_pallet;
        public final int shelf_width;
        public final int shelf_height;
        public final int shelf_depth;
    }


    @AllArgsConstructor
    public static class ProductClass {

        public final int product_class_id;
        public final int product_subcategory;
        public final int product_category;
        public final int product_department;
        public final int product_family;
    }


    @AllArgsConstructor
    public static class Store {

        public final int store_id;
        public final int store_type;
        public final int region_id;
        public final String store_name;
        public final int store_number;
        public final String store_street_address;
        public final String store_city;
        public final String store_state;
        public final int store_postal_code;
        public final String store_country;
        public final int store_manager;
        public final String store_phone;
        public final String store_fax;
        public final String first_opened_date;
        public final String last_remodel_date;
        public final int store_sqft;
        public final int grocery_sqft;
        public final int frozen_sqft;
        public final int meat_sqft;
        public final boolean coffee_bar;
        public final boolean video_store;
        public final boolean salad_bar;
        public final boolean prepared_food;
        public final boolean florist;
    }


    @AllArgsConstructor
    public static class Department {

        public final int department_id;
        public final String department_description;
    }


    @AllArgsConstructor
    public static class Employee {

        public final int employee_id;
        public final String full_name;
        public final String first_name;
        public final String last_name;
        public final int position_id;
        public final String position_title;
        public final int store_id;
        public final int department_id;
        public final String birth_date;
        public final String hire_date;
        public final String end_date;
        public final int salary;
        public final int supervisor_id;
        public final int education_level;
        public final int marital_status;
        public final int gender;
        public final String management_role;
    }
}

