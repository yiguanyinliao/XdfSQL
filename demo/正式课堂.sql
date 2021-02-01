SELECT sl.id                                                     as id,
       sl.roomId                                                 as roomId,                 -- 教室编号
       sl.schoolname                                             as schoolName,             -- 学校名称
       sl.areaname                                               as areaName,               -- 校区名称
       sl.teachername                                            as teacherName,            -- 教师姓名
       sl.studentname                                            as studentName,            -- 学生姓名
       dl.tlessonStar                                            as studentScore,           -- 学生表现得分（来自老师
       dl.slessonStar                                            as teacherScore,           -- 老师表现得分（来自学生）
       dl.splatStar                                              as studentToPlatformScore, -- 平台体验（来自学生）
       dl.tplatStar                                              as teacherToPlatformScore, -- 平台体验（来自老师）
       sl.teacheremail                                           as teacherEmail,           -- 教师邮箱
       sl.classcode                                              as classCode,              -- 班级号
       sl.studentcode                                            as studentCode,            -- 学员号
       sl.classname                                              as className,              -- 班级名称
       CONCAT(sl.dtdate, " ", IFNULL(DATE_FORMAT(sl.sectbegin, '%H:%i'), '00:00'), "-",
              IFNULL(DATE_FORMAT(sl.sectend, '%H:%i'), '00:00')) as courseDate,             -- 课程时间 （格式2020-04-09 08:00-10:00）
       (
         CASE
           WHEN cl.start_time is null THEN ''
           WHEN cl.start_time is not null and cl.status = 2
             THEN CONCAT(IFNULL(DATE_FORMAT(cl.start_time, '%H:%i'), ''), '-')
           ELSE CONCAT(IFNULL(DATE_FORMAT(cl.start_time, '%H:%i'), ''), '-',
                       IFNULL(DATE_FORMAT(cl.end_time, '%H:%i'), ''))
           END
         )                                                       as teacherTime,            -- 老师出勤时间 （格式08:00-10:00）
       (
         CASE
           WHEN cl.student_in_time is null THEN ''
           WHEN cl.start_time is not null and cl.status = 2 THEN
             CONCAT(IFNULL(DATE_FORMAT(cl.student_in_time, '%H:%i'), ''), '-')
           ELSE
             CONCAT(IFNULL(DATE_FORMAT(cl.student_in_time, '%H:%i'), ''), '-',
                    IFNULL(DATE_FORMAT(cl.student_out_time, '%H:%i'), ''))
           END
         )                                                       as studentTime,            -- 学生出勤时间
       (
         CASE
           WHEN cl.start_time is null THEN ''
           WHEN sl.sectbegin < DATE_ADD(cl.start_time, INTERVAL -5 MINUTE) THEN '2'
           ELSE '1'
           END
         )                                                       AS teacherLate,            -- 老师是否迟到 ('1'或''-正常 '2'-迟到)
       (
         CASE
           WHEN cl.start_time is null or end_time is null THEN ''
           WHEN cl.start_time is not null and cl.status = 2 THEN ''
           WHEN cl.end_time < sl.sectend and cl.status = 1 THEN '2'
           ELSE '1'
           END
         )                                                       AS teacherLeave,           -- 老师是否早退 ('1'或''-正常 '2'-早退)
       (
         CASE
           WHEN cl.student_in_time is null THEN ''
           WHEN sl.sectbegin < DATE_ADD(cl.student_in_time, INTERVAL -5 MINUTE) THEN '2'
           ELSE '1'
           END
         )                                                       AS studentLate,            -- 学生是否迟到 ('1'或''-正常 '2'-迟到)
       (
         CASE
           WHEN cl.student_in_time is null or cl.student_out_time is null THEN ''
           WHEN cl.student_in_time is not null and cl.student_out_time is not null and cl.status = 2 THEN ''
           WHEN cl.student_out_time < sl.sectend and cl.student_in_time is not null and cl.status = 1 THEN '2'
           ELSE '1'
           END
         )                                                       AS studentLeave,           -- 学生是否早退 ('1'或''-正常 '2'-早退)
       (
         CASE
           WHEN cl.start_time is not null and cl.end_time is not null THEN '2'
           WHEN
               cl.student_in_time is not null and cl.start_time is null and
               sl.sectend < DATE_ADD(now(), INTERVAL -40 MINUTE)
             THEN '3'
           ELSE '1'
           END
         )                                                       AS status,                 -- 课程状态 ('1'-进行中 '2'-已完成 '3'-异常)
       sl.recordurls                                             as videoUrl                -- 视频回放 URL
FROM statistics_lessons sl
       INNER JOIN
     class_realtime cl on sl.roomId = cl.room_id
       LEFT JOIN
     lesson_estimate_record dl on sl.roomId = dl.roomId