package com.hazelcast.map.impl.proxy;

public class Employee {
    private String name;
    private long age;

    public Employee(String name, long age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public long getAge() {
        return age;
    }
}
