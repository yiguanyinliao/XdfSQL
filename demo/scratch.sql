-- 高端报名写入宽表

set mapred.job.name = dws_um_income_reg_a  ;
set hive.merge.mapredfiles=true;
set hive.merge.mapfiles=true;
set mapred.max.split.size=5073741824;
set dfs.block.size=1073741824;
set mapreduce.map.memory.mb=8196;
set mapreduce.reduce.memory.mb=8196;

set hive.exec.compress.intermediate=true;
set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;

--压缩设置
set mapred.compress.map.output = true;
set mapred.output.compress = true;
set hive.exec.compress.output = true;
--输出设置
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize = 128000000;
--输入设置
set hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.min.split.size = 256000000;
set mapred.min.split.size.per.node = 256000000;
set mapred.min.split.size.per.rack = 256000000;
set hive.exec.reducers.bytes.per.reducer = 256000000;

insert overwrite table dws.dws_um_income_reg_a
select t1.lesson_package_code,
       '',
       t1.CONTRACTCODE,
       t1.schoolid,
       T2.schoolname,                                                                                                       -- 学校名称
       T2.school_level1_level,                                                                                              -- 一级校类别ID
       T2.school_level1_levelname,                                                                                          -- 一级校类别名称
       T2.sch_merge_id,                                                                                                      -- 学校合并id
       T2.sch_merge_name,
       T2.sch_merge_level,                                                                                                   -- 学校合并规模
       T2.sch_merge_levelname,
       CASE
           WHEN length(t1.lesson_package_code) = 0 THEN 'D'
           ELSE CASE WHEN t1.reg > 0 THEN 'B' ELSE CASE WHEN t1.cnt >= 1 THEN 'T' ELSE '' END END END biz_type,
       t1.DEPOSIT_AMOUNT + t1.REG_AMOUNT - t1.CANCEL_AMOUNT                                           metric_cash,
       case when t1.reg > 0 then 1 else case when t1.cnt >= 1 then -1 else 0 end END                  metric_reg,
       t1.ymd,
       T6.fyearid        as                                                                           dtdate_fyear,          -- 业务日期财年
       T6.fquarterid     as                                                                           dtdate_fquarter       -- 业务日期所属财季
        ,
       T6.fmonthid       as                                                                           dtdate_fmonth         -- 业务日期所属财月
        ,
       T6.fweekid        as                                                                           dtdate_fweekid,
       T1.STUDENTCODE    as                                                                           studentcode           --  学员号
        ,
       T1.studentname    as                                                                           studentname,
       T3.depart_code    as                                                                           f_dept_code           --  标准部门编码（新）
        ,
       T3.sname          as                                                                           f_dept_name           -- 标准部门名称（新）
        ,
       T1.PRODUCTID      as                                                                           productid             --  产品编码
        ,
       T1.PRODUCTNAME    as                                                                           productname           --  产品名称--  学员姓名
        ,
       gra.code          as                                                                           gradeinner            -- 年级内
        ,
       T1.gradecode      as                                                                           gradevaluecode        --  年级编码
        ,
       T8.gradeinnername as                                                                           gradeinnername        --  年级名称
        ,
       sub.scode         as                                                                           classsubjectinner     --  科目内
        ,
       T1.subject_code   as                                                                           classsubjectvaluecode --  科目编码
        ,
       T1.subject_name   as                                                                           classsubjectinnername --  科目名称
        ,
       T1.classmates     as                                                                           classmates            --  班容
        ,
       qua.scode         as                                                                           quarterinner          -- 学期
        ,
       T1.season_code    as                                                                           quartervaluecode      --  季度编码
        ,
       T1.season_name    as                                                                           quarterinnername      --  季度名称
        ,
       T1.areacode       as                                                                           areacode              --  校区编码
        ,
       T7.areaname       as                                                                           areaname              --  校区名称
        ,
       T7.are_main_code                                                                                                     --  主教学区编码
        ,
       T7.are_main_name                                                                                                     --  主教学区名称
        ,
       T7.are_region_id                                                                                                     --  大区编码
        ,
       T7.are_region_name                                                                                                   --  大区名称
        ,
       0                                                                                              history_flag,
       '',
       from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss'),
       cast(T1.lesson_num as string),
       t1.REG_AMOUNT,
       t1.reg,
       t1.CANCEL_AMOUNT,
       t1.cnt
from (SELECT A.lesson_package_code,
             A.CONTRACTCODE,
             A.schoolid,
             A.ymd,
             A.PRODUCTID,
             A.PRODUCTNAME,
             A.GRADECODE,
             A.GRADENAME,
             A.AREACODE,
             A.AREANAME,
             A.season_code,
             A.season_name,
             A.subject_code,
             A.subject_name,
             A.ORDERCODE,
             A.STUDENTCODE,
             A.STUDENTNAME,
             A.DEPOSITAMOUNT  AS              DEPOSIT_AMOUNT,
             NVL(B.AMOUNT, 0) AS              REG_AMOUNT,
             NVL(C.AMOUNT, 0) AS              CANCEL_AMOUNT,
             nvl(d.cnt, 0)                    cnt,
             nvl(B.reg, 0)                    reg,
             A.classmates,
             A.lesson_num + A.gift_lesson_num lesson_num
      from (SELECT A.lesson_package_code,
                   A.SCHOOLID,
                   A.ymd,
                   A.PRODUCTID,
                   A.PRODUCTNAME,
                   A.GRADECODE,
                   A.GRADENAME,
                   A.AREACODE,
                   A.AREANAME,
                   A.season_code,
                   A.season_name,
                   A.subject_code,
                   A.subject_name,
                   A.CONTRACTCODE,
                   A.ORDERCODE,
                   A.STUDENTCODE,
                   A.STUDENTNAME,
                   SUM(A.DEPOSITAMOUNT) AS DEPOSITAMOUNT,
                   A.BIZINOUTTYPE,
                   A.classmates,
                   A.lesson_num,
                   A.gift_lesson_num
            FROM (
                     SELECT UBFI.lesson_package_code,
                            UBFI.SCHOOL_ID    AS                           SCHOOLID,
                            date_format(UBF.COMPLETION_TIME, 'yyyy-MM-dd') ymd,
                            UBFI.PRODUCT_ID   AS                           PRODUCTID,
                            UBFI.PRODUCT_NAME AS                           PRODUCTNAME,
                            UBFI.GRADE_CODE   AS                           GRADECODE,
                            UBFI.GRADE_NAME   AS                           GRADENAME,
                            UBFI.AREA_CODE    AS                           AREACODE,
                            UBFI.AREA_NAME    AS                           AREANAME,
                            UBFI.CONTRACT_NO  AS                           CONTRACTCODE,
                            UBFI.ORDER_CODE   AS                           ORDERCODE,
                            UBFI.STUDENT_CODE AS                           STUDENTCODE,
                            UBFI.STUDENT_NAME AS                           STUDENTNAME,
                            0                 AS                           DEPOSITAMOUNT,
                            1                 AS                           BIZINOUTTYPE,
                            UBFI.season_code,
                            UBFI.season_name,
                            UBFI.subject_code,
                            UBFI.subject_name,
                            UBFI.classmates,
                            UBFI.lesson_num,
                            UBFI.gift_lesson_num
                     FROM gaoduan.UM_BIZ_FLOW_ITEM UBFI
                              LEFT JOIN gaoduan.UM_BIZ_FLOW UBF
                                        ON UBFI.ASSOCIATED_BIZ_NO = UBF.ASSOCIATED_BIZ_NO
                                            AND UBFI.BIZ_CODE = UBF.BIZ_CODE
                     WHERE UBF.BIZ_STATUS = 2
                       AND (UBFI.BIZ_TYPE IN ('B', 'Z', 'W') OR
                            (UBFI.BIZ_TYPE = 'T' AND
                             UBFI.AFFECTED_FLAG = 0))

                     union all
                     SELECT UBFI.lesson_package_code,
                            UBFI.SCHOOL_ID    AS                           SCHOOLID,
                            date_format(UBF.COMPLETION_TIME, 'yyyy-MM-dd') ymd,
                            NULL              AS                           PRODUCTID,
                            NULL              AS                           PRODUCTNAME,
                            UBFI.GRADE_CODE   AS                           GRADECODE,
                            UBFI.GRADE_NAME   AS                           GRADENAME,
                            UBFI.AREA_CODE    AS                           AREACODE,
                            UBFI.AREA_NAME    AS                           AREANAME,
                            UBFI.CONTRACT_NO  AS                           CONTRACTCODE,
                            UBFI.ORDER_CODE   AS                           ORDERCODE,
                            UBFI.STUDENT_CODE AS                           STUDENT_CODE,
                            UBFI.STUDENT_NAME AS                           STUDENTNAME,
                            CASE
                                WHEN UBFI.BIZ_TYPE = 'D' THEN
                                    UBF.AMOUNT
                                ELSE
                                    -UBF.AMOUNT
                                END           AS                           DEPOSITAMOUNT,
                            2                 AS                           BIZINOUTTYPE,
                            UBFI.season_code,
                            UBFI.season_name,
                            UBFI.subject_code,
                            UBFI.subject_name,
                            UBFI.classmates,
                            UBFI.lesson_num,
                            UBFI.gift_lesson_num
                     FROM gaoduan.UM_BIZ_FLOW UBF
                              LEFT JOIN gaoduan.UM_BIZ_FLOW_ITEM UBFI
                                        ON UBFI.ASSOCIATED_BIZ_NO = UBF.ASSOCIATED_BIZ_NO
                                            AND UBFI.BIZ_CODE = UBF.BIZ_CODE
                     WHERE (UBFI.CONTRACT_NO IS NULL OR
                            UBFI.CONTRACT_NO = '')
                       and UBF.BIZ_STATUS = 2
                       AND UBFI.BIZ_TYPE IN ('D', 'J')
                     union all
                     SELECT UBFI.lesson_package_code,
                            UBFI.SCHOOL_ID    AS                           SCHOOLID,
                            date_format(UBF.COMPLETION_TIME, 'yyyy-MM-dd') ymd,
                            NULL              AS                           PRODUCTID,
                            NULL              AS                           PRODUCTNAME,
                            UBFI.GRADE_CODE   AS                           GRADECODE,
                            UBFI.GRADE_NAME   AS                           GRADENAME,
                            UBFI.AREA_CODE    AS                           AREACODE,
                            UBFI.AREA_NAME    AS                           AREANAME,
                            NULL              AS                           CONTRACTCODE,
                            UBFI.ORDER_CODE   AS                           ORDERCODE,
                            UBFI.STUDENT_CODE AS                           STUDENT_CODE,
                            UBFI.STUDENT_NAME AS                           STUDENTNAME,
                            UBF.AMOUNT        AS                           DEPOSITAMOUNT,
                            3                 AS                           BIZINOUTTYPE,
                            UBFI.season_code,
                            UBFI.season_name,
                            UBFI.subject_code,
                            UBFI.subject_name,
                            UBFI.classmates,
                            UBFI.lesson_num,
                            UBFI.gift_lesson_num
                     FROM gaoduan.UM_BIZ_FLOW UBF
                              LEFT JOIN gaoduan.UM_BIZ_FLOW_ITEM UBFI
                                        ON UBFI.ASSOCIATED_BIZ_NO = UBF.ASSOCIATED_BIZ_NO
                                            AND UBFI.BIZ_CODE = UBF.BIZ_CODE
                     WHERE UBF.BIZ_STATUS = 2
                       AND length(if(ltrim(UBFI.CONTRACT_NO) is null, '', UBFI.CONTRACT_NO)) > 0
                       AND UBFI.BIZ_TYPE = 'D'
                     union all
                     SELECT UBFI.lesson_package_code,
                            UBFI.SCHOOL_ID    AS                          SCHOOLID,
                            date_format(UB.COMPLETION_TIME, 'yyyy-MM-dd') ymd,
                            NULL              AS                          PRODUCTID,
                            NULL              AS                          PRODUCTNAME,
                            UBFI.GRADE_CODE   AS                          GRADECODE,
                            UBFI.GRADE_NAME   AS                          GRADENAME,
                            UBFI.AREA_CODE    AS                          AREACODE,
                            UBFI.AREA_NAME    AS                          AREANAME,
                            NULL              AS                          CONTRACTCODE,
                            UBFI.ORDER_CODE   AS                          ORDERCODE,
                            UBFI.STUDENT_CODE AS                          STUDENT_CODE,
                            UBFI.STUDENT_NAME AS                          STUDENTNAME,
                            -UBF.AMOUNT       AS                          DEPOSITAMOUNT,
                            3                 AS                          BIZINOUTTYPE,
                            UBFI.season_code,
                            UBFI.season_name,
                            UBFI.subject_code,
                            UBFI.subject_name,
                            UBFI.classmates,
                            UBFI.lesson_num,
                            UBFI.gift_lesson_num
                     FROM gaoduan.UM_BIZ_FLOW UBF
                              LEFT JOIN gaoduan.UM_BIZ_FLOW_ITEM UBFI
                                        ON UBFI.ASSOCIATED_BIZ_NO = UBF.ASSOCIATED_BIZ_NO AND
                                           UBFI.BIZ_CODE = UBF.BIZ_CODE
                              LEFT JOIN gaoduan.UM_BIZ_FLOW UB ON UBF.ASSOCIATED_BIZ_NO = UB.ASSOCIATED_BIZ_NO
                     WHERE UBF.BIZ_STATUS = 2
                       AND UB.BIZ_STATUS = 2
                       AND length(if(UBFI.CONTRACT_NO is null, '', UBFI.CONTRACT_NO)) > 0
                       AND UBFI.BIZ_TYPE = 'D'
                       AND UB.BIZ_TYPE = 'W'
                 ) A
            GROUP BY A.lesson_package_code,
                     A.SCHOOLID,
                     A.PRODUCTID,
                     A.ymd,
                     A.PRODUCTNAME,
                     A.GRADECODE,
                     A.GRADENAME,
                     A.AREACODE,
                     A.AREANAME,
                     A.season_code,
                     A.season_name,
                     A.subject_code,
                     A.subject_name,
                     A.CONTRACTCODE,
                     A.ORDERCODE,
                     A.STUDENTCODE,
                     A.STUDENTNAME,
                     A.BIZINOUTTYPE,
                     A.classmates,
                     A.lesson_num,
                     A.gift_lesson_num) A
               LEFT join (SELECT UBFIA.SCHOOL_ID            AS                   SCHOOLID,
                                 UBFIA.CONTRACT_NO          AS                   CONTRACTNO,
                                 date_format(UBFA.COMPLETION_TIME, 'yyyy-MM-dd') ymd,
                                 NVL(UBFIA.TOTAL_AMOUNT, 0) AS                   AMOUNT,
                                 1                                               reg
                          FROM gaoduan.UM_BIZ_FLOW_ITEM UBFIA
                                   LEFT JOIN gaoduan.UM_BIZ_FLOW UBFA
                                             ON UBFIA.ASSOCIATED_BIZ_NO = UBFA.ASSOCIATED_BIZ_NO
                                                 AND UBFIA.BIZ_CODE = UBFA.BIZ_CODE
                          WHERE UBFIA.BIZ_TYPE IN ('B', 'Z', 'W')
                            AND UBFA.BIZ_STATUS = 2

                            AND UBFIA.CONTRACT_NO IS NOT NULL
                            AND UBFIA.CONTRACT_NO != ' '

                          group by UBFIA.SCHOOL_ID,
                                   UBFIA.CONTRACT_NO,
                                   date_format(UBFA.COMPLETION_TIME, 'yyyy-MM-dd'),
                                   NVL(UBFIA.TOTAL_AMOUNT, 0)) B
                         ON A.SCHOOLID = B.SCHOOLID
                             AND A.CONTRACTCODE = B.CONTRACTNO
                             AND A.YMD = B.YMD
                             AND A.BIZINOUTTYPE = 1
               LEFT JOIN (SELECT UBFIA.SCHOOL_ID                 AS              SCHOOLID,
                                 UBFIA.CONTRACT_NO,
                                 date_format(UBFA.COMPLETION_TIME, 'yyyy-MM-dd') ymd,
                                 SUM(NVL(UBFIA.TOTAL_AMOUNT, 0)) AS              AMOUNT
                          FROM gaoduan.UM_BIZ_FLOW_ITEM UBFIA
                                   LEFT JOIN gaoduan.UM_BIZ_FLOW UBFA
                                             ON UBFIA.ASSOCIATED_BIZ_NO = UBFA.ASSOCIATED_BIZ_NO
                                                 AND UBFIA.BIZ_CODE = UBFA.BIZ_CODE
                          WHERE UBFIA.BIZ_TYPE = 'T'
                            AND UBFA.BIZ_STATUS = 2

                            AND UBFIA.CONTRACT_NO IS NOT NULL
                            AND UBFIA.CONTRACT_NO != ' '
                          GROUP BY UBFIA.SCHOOL_ID,
                                   UBFIA.CONTRACT_NO,
                                   date_format(UBFA.COMPLETION_TIME, 'yyyy-MM-dd')) c
                         ON A.SCHOOLID = C.SCHOOLID
                             AND A.CONTRACTCODE = C.CONTRACT_NO
                             AND A.YMD = C.YMD
                             AND A.BIZINOUTTYPE = 1
               LEFT JOIN (select a.lesson_package_code,
                                 a.contractno,
                                 a.nschoolid,
                                 date_format(a.umbiz_completion_time, 'yyyy-MM-dd') ymd,
                                 1                                                  cnt
                          from dws.dws_um_biz_flow_dtl a
                                   left join dws.dws_um_contract_detail b
                                             on a.contractno = b.contract_no and a.nschoolid = b.nschoolid
                          where a.umbiz_type in ('T')
                            and b.package_status = 5
                            and a.umbiz_biz_status = 2
                            and a.affected_flag = 0) d
                         ON A.SCHOOLID = d.nschoolid
                             AND A.CONTRACTCODE = d.contractno
                             AND A.YMD = d.ymd
                             AND A.BIZINOUTTYPE = 1) t1
         left join dim.dim_school T2
                   on T1.schoolid = T2.nschoolid
         left join dim.dim_fiscal_day T6
                   on t1.ymd = cast(T6.dayid as string)
         left join (select a.*,
                           b.sname,
                           a.contract_no as contract_no1,
                           c.depart_code
                    from gaoduan.um_lesson_package c,
                         gaoduan.um_lesson_package_detail a,
                         jwyth.d_deptstd b
                    where c.lesson_package_code = a.package_code
                      and c.depart_code = b.sfcode) T3
                   on T3.contract_no1 = T1.CONTRACTCODE
                       and t1.schoolid = t3.school_id
         left join
     (select value_code, code, gradetype
      from jwyth.grade
      where value_code is not null
        and value_code <> '') gra
     on T1.GRADECODE = gra.value_code
         and T3.depart_code = gra.gradetype
         left join (select value_code, scode
                    from jwyth.d_classsubject
                    where value_code is not null
                      and value_code <> '') sub
                   on sub.value_code = T1.subject_code
         left join (select value_code, scode, sname
                    from jwyth.d_quarter
                    where value_code is not null
                      and value_code <> '') qua
                   on qua.value_code = T1.season_code
         left join dim.dim_teachingarea T7
                   on T7.areacode = T1.areacode
                       and T7.nschoolid = T1.schoolid
         left join (select gradeinnername,
                           value_code
                    from dim.dim_gd_grade_mapping
                    group by gradeinnername,
                             value_code) T8
                   on T1.gradecode = T8.value_code;

INSERT INTO dws.dws_um_income_reg_a
select T1.lesson_package_code                                                        --  课时包编码
     , ''
     , T3.contract_no                                       as umbiz_contract_no     --  合同编号
     , T1.school_id                                         as nschoolid             -- 学校ID
     , T2.schoolname                                                                 -- 学校名称
     , T2.school_level1_level                                                        -- 一级校类别ID
     , T2.school_level1_levelname                                                    -- 一级校类别名称
     , T2.sch_merge_id                                                               -- 学校合并id
     , T2.sch_merge_name                                                             -- 学校合并名称
     , T2.sch_merge_level                                                            -- 学校合并规模
     , T2.sch_merge_levelname                                                        -- 学校合并规模名称
     , TT3.biz_type                                                                  -- 业务类型

     , sum(TT3.total_amount)
     -- 金额
     , max(case when TT3.biz_type = 'T' then -1 else 1 end) as metric_reg            -- 人次
     , date_format(TT3.create_time, 'yyyy-MM-dd')           as dtdate                -----福州2次割接可能造成时间错误应该直接取TT3安业务类型来的创建时间
     -- 业务日期
     , T6.fyearid                                           as dtdate_fyear          -- 业务日期财年
     , T6.fquarterid                                        as dtdate_fquarter       -- 业务日期所属财季
     , T6.fmonthid                                          as dtdate_fmonth         -- 业务日期所属财月
     , T6.fweekid                                           as dtdate_fweekid
     , T1.student_code                                      as studentcode           --  学员号
     , T1.student_name                                      as studentname           --  学员姓名
     , T1.depart_code                                       as f_dept_code           --  标准部门编码（新）
     , T5.sname                                             as f_dept_name           -- 标准部门名称（新）
     , T1.product_id                                        as productid             --  产品编码
     , T1.product_name                                      as productname           --  产品名称
     , gra.code                                             as gradeinner            -- 年级内
     , T1.grade_code                                        as gradevaluecode        --  年级编码
     , T8.gradeinnername                                    as gradeinnername        --  年级名称
     , sub.scode                                            as classsubjectinner     --  科目内
     , T1.subject_code                                      as classsubjectvaluecode --  科目编码
     , T1.subject_name                                      as classsubjectinnername --  科目名称
     , TT3.classmates                                       as classmates            --  班容
     , qua.scode                                            as quarterinner          -- 学期
     , T1.quarter                                           as quartervaluecode      --  季度编码
     , T1.quarter_name                                      as quarterinnername      --  季度名称

     , T1.area_code                                         as areacode              --  校区编码
     , T1.area_name                                         as areaname              --  校区名称
     , T7.are_main_code                                                              --      主教学区编码
     , T7.are_main_name                                                              --      主教学区名称
     , T7.are_region_id                                                              --      大区编码
     , T7.are_region_name                                                            --      大区名称
     , TT3.history_flag                                                              --  0:非历史数据 1:历史数据; 导入数据用
     , case when TT3.biz_order_type = 1 then 1 else 0 end   as biz_order_type
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                        -- etl时间
     , cast(T3.lesson_num as string)
     , sum(if(TT3.biz_type <> 'T', TT3.total_amount, 0))
     , if(TT3.biz_type <> 'T', 1, 0)
     , sum(if(TT3.biz_type = 'T', TT3.total_amount, 0))
     , if(TT3.biz_type <> 'T', -1, 0)
from gaoduan.um_lesson_package T1
         inner join dim.dim_school T2
                    on T1.school_id = T2.nschoolid
                        and T1.history_flag = 1
         inner join gaoduan.um_lesson_package_detail T3
                    on T1.lesson_package_code = T3.package_code
         inner join (select a.*,
                            umbiz.biz_order_type,
                            row_number()
                                    over (partition by a.school_id,a.contract_no,a.student_code,a.biz_type order by a.create_time desc ) rn
                     from gaoduan.um_biz_flow_item a
                              left join gaoduan.um_biz_flow umbiz
                                        on a.biz_code = umbiz.biz_code) TT3
                    on TT3.lesson_package_code = T3.package_code
                        and T3.contract_no = TT3.contract_no
                        and rn = 1
                        and TT3.history_flag = 1
         left join jwyth.d_deptstd T5
                   on T1.depart_code = T5.sfcode
         left join dim.dim_fiscal_day T6
                   on date_format(TT3.create_time, 'yyyy-MM-dd') = cast(T6.dayid as string)
         left join
     (select value_code, code, gradetype
      from jwyth.grade
      where value_code is not null
        and value_code <> '') gra
     on T1.grade_code = gra.value_code
         and T1.depart_code = gra.gradetype
         left join (select value_code, scode
                    from jwyth.d_classsubject
                    where value_code is not null
                      and value_code <> '') sub
                   on sub.value_code = T1.subject_code
         left join (select value_code, scode, sname
                    from jwyth.d_quarter
                    where value_code is not null
                      and value_code <> '') qua
                   on qua.value_code = T1.quarter
         left join dim.dim_teachingarea T7
                   on T7.areacode = T1.area_code
                       and T7.nschoolid = T1.school_id
         left join (select gradeinnername,
                           value_code
                    from dim.dim_gd_grade_mapping
                    group by gradeinnername,
                             value_code) T8
                   on T1.grade_code = T8.value_code

group by T1.lesson_package_code                     --  课时包编码
       , T1.package_status                          --  课时包状态
       , T3.contract_no                             --  合同编号
       , T1.school_id                               -- 学校ID
       , T2.schoolname                              -- 学校名称
       , T2.school_level1_level                     -- 一级校类别ID
       , T2.school_level1_levelname                 -- 一级校类别名称
       , T2.sch_merge_id                            -- 学校合并id
       , T2.sch_merge_name                          -- 学校合并名称
       , T2.sch_merge_level                         -- 学校合并规模
       , T2.sch_merge_levelname                     -- 学校合并规模名称
       , TT3.biz_type                               -- 业务类型
       , date_format(TT3.create_time, 'yyyy-MM-dd') ---- 业务日期
       , T6.fyearid                                 -- 业务日期财年
       , T6.fquarterid                              -- 业务日期所属财季
       , T6.fmonthid                                -- 业务日期所属财月
       , T6.fweekid
       , T1.student_code                            --  学员号
       , T1.student_name                            --  学员姓名
       , T1.depart_code                             --  标准部门编码（新）
       , T5.sname                                   -- 标准部门名称（新）
       , T1.product_id                              --  产品编码
       , T1.product_name                            --  产品名称
       , gra.code                                   -- 年级内
       , T1.grade_code                              --  年级编码
       , T8.gradeinnername
       , sub.scode                                  --  科目内
       , T1.subject_code                            --  科目编码
       , T1.subject_name                            --  科目名称
       , T1.classmates                              --  班容
       , qua.scode                                  -- 学期
       , T1.quarter                                 --  季度编码
       , T1.quarter_name                            --  季度名称
       , T1.area_code                               --  校区编码
       , T1.area_name                               --  校区名称
       , T7.are_main_code
       , T7.are_main_name
       , T7.are_region_id
       , T7.are_region_name
       , TT3.history_flag
       , TT3.classmates
       , cast(T3.lesson_num as string)
       , case when TT3.biz_order_type = 1 then 1 else 0 end
       , if(TT3.biz_type <> 'T', 1, 0)
       , if(TT3.biz_type <> 'T', -1, 0);

