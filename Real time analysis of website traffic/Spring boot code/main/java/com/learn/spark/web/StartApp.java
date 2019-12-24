package com.learn.spark.web;

import com.learn.spark.dao.CourseClickCountDao;
import com.learn.spark.domain.CourseClickCount;


import com.learn.spark.domain.CourseSearchClickCount;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
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
    public List<CourseClickCount> courseClickCount(@RequestBody JSONObject params)throws Exception{
        String datetime=params.getString("datetime");

        List<CourseClickCount> list = courseClickCountDao.quaryCourseClickCount(datetime);

        return  list;
    }


    @RequestMapping(value="/course_search_clickcount_dynamic",method = RequestMethod.POST)
    @ResponseBody
    public List<CourseSearchClickCount> courseSearchClickCount(@RequestBody JSONObject params)throws Exception{
        String datetime=params.getString("datetime");
        List<CourseSearchClickCount> courseSearchClickCounts = courseClickCountDao.quarySearchCourseClickCount(datetime);

        return  courseSearchClickCounts;
    }


    @RequestMapping(value="/echarts",method = RequestMethod.GET)
    public ModelAndView echarts(){

        return new ModelAndView("echarts");
    }
}
