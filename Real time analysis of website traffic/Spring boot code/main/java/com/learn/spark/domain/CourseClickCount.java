package com.learn.spark.domain;

import org.springframework.stereotype.Component;

@Component
public class CourseClickCount {

    private String name;
    private Long clickvalue;

    public CourseClickCount(String name, Long value) {
        this.name = name;
        this.clickvalue = value;
    }

    public CourseClickCount() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getClickvalue() {
        return clickvalue;
    }

    public void setClickvalue(Long clickvalue) {
        this.clickvalue = clickvalue;
    }


}
