package com.sahuri.flink.pojo;

public class Customer {
    public int id;
    public String name;
    public String city;

    public Customer() {}

    public Customer(int id, String name, String city) {
        this.id = id;
        this.name = name;
        this.city = city;
    }
}
