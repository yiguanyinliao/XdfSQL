SELECT sl.id,
       school.school_name                           as schoolName, -- 学校名称
       sl.teacheremail                              as teacherEmail, -- 教师邮箱
       sl.teachername                               as teacherName, -- 教师姓名
       stu.studentName                              as studentName, -- 学生姓名
       sl.recordurls                                as videoUrl, -- 视频回放 URL
       (
            CASE
               WHEN sl.reason_type = '1' THEN '调课'
               WHEN sl.reason_type = '2' THEN '会议'
               WHEN sl.reason_type = '3' THEN '磨课'
               WHEN sl.reason_type = '4' THEN '试听'
               WHEN sl.reason_type = '5' THEN '家长会'
               WHEN sl.reason_type = '6' THEN '小组磨课'
               ELSE '调课'
            END
           )  as classReason, -- 开课原因 （'1'-调课 '2'-会议  '3'-磨课  '4'-试听 '5'-家长会 '6'-小组磨课）
       sl.reason_desc                               as classNote, -- 开课备注
       DATE_FORMAT(sl.createtime, '%Y-%m-%d %H:%i') as courseDate, -- 创建时间（格式：2020-04-09 08:00-10:00）
       sl.student_in_count                          as studentInCount, -- 实际进入人数
       (
            CASE
               WHEN sl.lesson_type = '1' THEN '普通'
               WHEN sl.lesson_type = '2' THEN '多教师'
               ELSE '普通'
            END
           )  as classSpec, -- 教室类型 （'1'-普通 '2'-多教师  ）
       tmp.cnt  as teacherCount, -- 教师数量
       (
            CASE
               WHEN sl.lesson_status = '1' THEN '进行中'
               WHEN sl.lesson_status = '2' THEN '已完成'
               WHEN sl.lesson_status = '3' THEN '异常'
            END
           ) as lessonStatus -- 课程状态（1-进行中、2-已完成，3-异常）
FROM statistics_pre_lessons sl
         left join class_realtime cl on sl.classcode = cl.room_id
         left join mdm_school as school on sl.schoolid = school.school_id
         left join statistics_pre_student as stu on sl.classcode = stu.classcode
         left join (select classcode,count(distinct(teachername)) cnt from statistics_pre_lessons group by classcode) tmp on sl.classcode = tmp.classcode
where sl.delflag<>1
