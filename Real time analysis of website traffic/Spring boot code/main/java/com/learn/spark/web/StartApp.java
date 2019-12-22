package com.learn.spark.web;

import com.learn.spark.dao.CourseClickCountDao;
import com.learn.spark.domain.CourseClickCount;


import com.learn.spark.domain.CourseSearchClickCount;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class StartApp {


    @Autowired
    CourseClickCountDao courseClickCountDao;



    @RequestMapping(value="/course_clickcount_dynamic",method = RequestMethod.POST)
    @ResponseBody
    public List<CourseClickCount> courseClickCount()throws Exception{

        List<CourseClickCount> list = courseClickCountDao.quaryCourseClickCount("20191220");

        return  list;
    }


    @RequestMapping(value="/course_search_clickcount_dynamic",method = RequestMethod.POST)
    @ResponseBody
    public List<CourseSearchClickCount> courseSearchClickCount()throws Exception{

        List<CourseSearchClickCount> courseSearchClickCounts = courseClickCountDao.quarySearchCourseClickCount("20191220");

        return  courseSearchClickCounts;
    }


    @RequestMapping(value="/echarts",method = RequestMethod.GET)
    public ModelAndView echarts(){

        return new ModelAndView("echarts");
    }
}
