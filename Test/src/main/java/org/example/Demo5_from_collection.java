package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class Demo5_from_collection {
    public static  class Student{
        public String name;
        public Integer age;

        public Student() {
        }

        public Student(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment en  = StreamExecutionEnvironment.getExecutionEnvironment();
        en.setRuntimeMode(RuntimeExecutionMode.BATCH);
        en.setParallelism(1);
        ArrayList<Student> students = new ArrayList<Student>();
        students.add(new Student("zhangsan",20));
        students.add(new Student("zhangsan",21));
        students.add(new Student("lisi",20));
        students.add(new Student("wangwu",23));
        en.fromCollection(students).map(new MapFunction<Student, Tuple2<String,Integer>>() {

            @Override
            public Tuple2<String, Integer> map(Student student) throws Exception {

                return Tuple2.of(student.getName(),Integer.valueOf(student.getAge()));
            }
        }).keyBy(0).sum(1).print();
        en.execute("a");
    }

    }

