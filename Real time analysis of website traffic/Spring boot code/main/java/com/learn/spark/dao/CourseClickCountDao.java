package com.learn.spark.dao;

import com.learn.spark.domain.CourseClickCount;
import com.learn.spark.domain.CourseSearchClickCount;
import com.learn.spark.utils.HBaseUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
@Component
public class CourseClickCountDao {

    private static Map<String,String> courses=new HashMap<>();

    static{
        courses.put("112","spark in action");
        courses.put("128","spark streaming");
        courses.put("145","Hbase in action");
        courses.put("146","machine learning");
        courses.put("131","spark");
        courses.put("130","deep learning");
    }

    public List<CourseClickCount> quaryCourseClickCount(String day)throws Exception{
        List<CourseClickCount> list=new ArrayList<>();
        // get the click count in the day
        Map<String, Long> map = HBaseUtils.getInstance().quary("course_clickcount", day);

        for(Map.Entry<String,Long> entry:map.entrySet()){
            list.add(new CourseClickCount(courses.get(entry.getKey().substring(9)),entry.getValue()));
        }
        return list;
    }


    public List<CourseSearchClickCount> quarySearchCourseClickCount(String day)throws Exception{



        Map<String,CourseSearchClickCount> course_search=new HashMap<>();
        // get the click count in the day
        Map<String, Long> map = HBaseUtils.getInstance().quary("course_search_clickcount", day);

        // the row_key date type like: 20191221_search.yahoo.com_128
        for(Map.Entry<String,Long> entry:map.entrySet()){
            String[] strs = entry.getKey().split("_");

            if (course_search.containsKey(strs[2])){
                // if the course_search has the key,put the data in search_click of CourseSearchClickCount
                course_search.get(strs[2]).getSearch_click().put(strs[1],entry.getValue());
            }else{
                // the key not in course_search
                CourseSearchClickCount courseSearchClickCount = new CourseSearchClickCount();
                courseSearchClickCount.setName(courses.get(strs[2]));

                course_search.put(strs[2],courseSearchClickCount);
            }
        }

        List<CourseSearchClickCount> list=new ArrayList<CourseSearchClickCount>(course_search.values());
        return list;
    }

    public static void main(String[] args) throws  Exception{
        CourseClickCountDao courseClickCountDao = new CourseClickCountDao();

        // test


        List<CourseClickCount> courseClickCounts = courseClickCountDao.quaryCourseClickCount("20191220");
        for(CourseClickCount s :courseClickCounts){
            System.out.println(s.getName()+":"+s.getClickvalue());
        }

        List<CourseSearchClickCount> courseSearchClickCounts = courseClickCountDao.quarySearchCourseClickCount("20191220");
        for (CourseSearchClickCount s:courseSearchClickCounts){
            System.out.println(s.getName());
            System.out.println(s.getSearch_click().toString());
        }
    }
}
