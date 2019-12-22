package com.learn.spark.web;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

@RestController
public class HelloBoot {

    @RequestMapping(value="/hello",method = RequestMethod.GET)
    public String sayHello(){
        return "Hello world spring boot";
    }

    @RequestMapping(value="/first",method = RequestMethod.GET)
    public ModelAndView firstDemo(){
        return new ModelAndView("test");
    }


    @RequestMapping(value="/courseclick",method = RequestMethod.GET)
    public ModelAndView courseclickDemo(){
        return new ModelAndView("demo");
    }
}
