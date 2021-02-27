package com.example.kafka;

public class studentModel {

private int studentid;
private  String name;
private String dept;

studentModel(){}

    public studentModel(int studentid, String name, String dept) {
        this.studentid = studentid;
        this.name = name;
        this.dept = dept;
    }

    public int getStudentid() {
        return studentid;
    }

    public void setStudentid(int studentid) {
        this.studentid = studentid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDept() {
        return dept;
    }

    public void setDept(String dept) {
        this.dept = dept;
    }
}
