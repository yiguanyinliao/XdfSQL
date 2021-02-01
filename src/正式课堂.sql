SELECT sl.id,
       sl.classcode                                 as roomId,
       school.school_name                           as schoolName,
       sl.classcode                                 as classCode,
       (CASE
          WHEN lesson_type = '1' THEN concat(ifnull(reason, ''), '(',
                                             SUBSTRING_INDEX(ifnull(sl.classcode, ''), '-', -1), ')', '-', ifnull((
                                                                                                                    select pstu.studentName
                                                                                                                    from statistics_pre_student pstu
                                                                                                                    where roleType = 2
                                                                                                                      and pstu.classCode = sl.classcode
                                                                                                                    limit 1
                                                                                                                  ),
                                                                                                                  '无听课学生'))
          ELSE concat(ifnull(reason, ''), '(', SUBSTRING_INDEX(ifnull(sl.classcode, ''), '-', -1), ')')
         END
         )                                          as className,
       sl.teacheremail                              as teacherEmail,
       sl.teachername                               as teacherName,
       sl.recordurls                                as videoUrl,
       sl.reason_type                               as classReason,
       sl.reason_desc                               as classNote,
       DATE_FORMAT(sl.createtime, '%Y-%m-%d %H:%i') as courseDate,
       sl.student_in_count                          as studentInCount,
       ifnull(sl.lesson_type, '1')                  as classSpec,
       sl.lesson_status                             as lessonStatus,
       (
         CASE
           WHEN cl.start_time is not null and cl.end_time is not null THEN '2'
           ELSE '1'
           END
         )                                          AS status,
       (
         CASE
           WHEN cl.start_time is null THEN ''
           WHEN cl.start_time is not null and cl.status = 2
             THEN CONCAT(IFNULL(DATE_FORMAT(cl.start_time, '%H:%i'), ''), '-')
           ELSE CONCAT(IFNULL(DATE_FORMAT(cl.start_time, '%H:%i'), ''), '-',
                       IFNULL(DATE_FORMAT(cl.end_time, '%H:%i'), ''))
           END
         )                                          as teacherTime
FROM statistics_pre_lessons sl
       left join class_realtime cl on sl.classcode = cl.room_id
       left join mdm_school as school on sl.schoolid = school.school_id
		