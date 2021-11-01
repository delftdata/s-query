package com.hazelcast.map.impl.proxy;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public class EmployeeSerializer implements StreamSerializer<Employee> {

    @Override
    public void write(ObjectDataOutput out, Employee object) throws IOException {
        out.writeUTF(object.getName());
        out.writeLong(object.getAge());
    }

    @Override
    public Employee read(ObjectDataInput in) throws IOException {
        String name = in.readUTF();
        long age = in.readLong();
        return new Employee(name, age);
    }

    @Override
    public int getTypeId() {
        return 1;
    }
}
