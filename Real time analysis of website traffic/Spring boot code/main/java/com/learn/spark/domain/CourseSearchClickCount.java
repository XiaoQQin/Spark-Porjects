package com.learn.spark.domain;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class CourseSearchClickCount {
    private String name;
    private Map<String,Long> search_click;


    public CourseSearchClickCount(){

        search_click=new HashMap<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Long> getSearch_click() {
        return search_click;
    }

    public void setSearch_click(Map<String, Long> search_click) {
        this.search_click = search_click;
    }


}
