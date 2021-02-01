insert overwrite table dws.dws_order_um_income_reg_da partition (dt = '${date}')
select t1.school_id                                                         as school_id,                --学校ID
       t1.lesson_package_code                                               as lesson_package_code,      --课时包编码
       t1.contract_no                                                       as contract_no,              --合同编号
       t1.ymd                                                               as biz_date,                 --业务日期
       CASE
           WHEN length(t1.lesson_package_code) = 0 THEN 'D'
           ELSE CASE
                    WHEN t1.reg > 0 THEN 'B'
                    ELSE CASE WHEN t1.cnt >= 1 THEN 'T' ELSE '' END END END as biz_type,                 --业务类型B:报名 T: 退费 S：升级 Z：追加报名 D:订金 W:尾款 G:赠送 J:退订金
       t1.student_code                                                      as student_code,             --学员号
       0                                                                    as history_flag,             --0:非历史数据 1:历史数据; 导入数据用
       t1.deposit_amount + t1.reg_amount - t1.cancel_amount                 as metric_income_amt,        --现金收入
       (t1.deposit_amount + t1.reg_amount - t1.cancel_amount) *
       (1 - t1.rate)                                                        as metric_income_ex_tax_amt, --现金收入不含税
       t1.reg_amount                                                        as metric_reg_amt,           --报入金额
       t1.cancel_amount                                                     as metric_cancel_amt,        --退费金额
       case
           when t1.reg > 0 then 1
           else case when t1.cnt >= 1 then -1 else 0 end END                as metric_reg_all_cnt,       --报名人次
       t1.reg                                                               as metric_reg_in_cnt,        --报入人次
       t1.cnt                                                               as metric_cancel_cnt,        --退费人次
       null                                                                 as ext1,                     --扩展字段1
       null                                                                 as ext2,                     --扩展字段2
       null                                                                 as ext3,                     --扩展字段3
       CURRENT_TIMESTAMP                                                    as etl_time                  --运行时间
FROM (SELECT A.school_id,
             A.lesson_package_code,
             A.contract_no,
             A.ymd,
             A.student_code,
             A.deposit_amount AS deposit_amount,
             NVL(B.AMOUNT, 0) AS reg_amount,
             NVL(C.AMOUNT, 0) AS cancel_amount,
             nvl(D.cnt, 0)    AS cnt,
             nvl(B.reg, 0)    AS reg,
             E.rate           AS rate
      FROM (SELECT A1.school_id,
                   A1.lesson_package_code,
                   A1.contract_no,
                   A1.ymd,
                   A1.student_code,
                   SUM(A1.deposit_amount) AS deposit_amount,
                   A1.bizinout_type
            FROM (SELECT UBFI.school_id           AS                    school_id,
                         UBFI.lesson_package_code AS                    lesson_package_code,
                         UBFI.contract_no         AS                    contract_no,
                         date_format(UBF.COMPLETION_TIME, 'yyyy-MM-dd') ymd,
                         UBFI.student_code        AS                    student_code,
                         '0'                      AS                    deposit_amount,
                         '1'                      AS                    bizinout_type
                  FROM (select school_id,
                               lesson_package_code,
                               contract_no,
                               student_code,
                               biz_type,
                               affected_flag,
                               associated_biz_no,
                               biz_code
                        from ods.ods_order_up_market_sign_um_biz_flow_item_da
                        where dt = '${date}') UBFI
                           LEFT JOIN(select COMPLETION_TIME,
                                            associated_biz_no,
                                            biz_code,
                                            biz_status
                                     from ods.ods_order_up_market_sign_um_biz_flow_da
                                     where dt = '${date}'
                                       AND biz_status = 2) UBF
                                    ON UBFI.associated_biz_no = UBF.associated_biz_no
                                        AND UBFI.biz_code = UBF.biz_code
                  WHERE UBFI.biz_type IN ('B', 'Z', 'W')
                     OR (UBFI.biz_type = 'T' AND
                         UBFI.affected_flag = 0)
                  union all
                  SELECT UBFI.school_id           AS                    school_id,
                         UBFI.lesson_package_code AS                    lesson_package_code,
                         UBFI.contract_no         AS                    contract_no,
                         date_format(UBF.COMPLETION_TIME, 'yyyy-MM-dd') ymd,
                         UBFI.student_code        AS                    student_code,
                         CASE
                             WHEN UBFI.biz_type = 'D' THEN
                                 cast(UBF.amount as string)
                             ELSE
                                 cast(-UBF.amount as string)
                             END                  AS                    deposit_amount,
                         '2'                      AS                    bizinout_type
                  FROM (select COMPLETION_TIME, biz_status, associated_biz_no, biz_code, amount
                        from ods.ods_order_up_market_sign_um_biz_flow_da
                        where dt = '${date}'
                          and biz_status = 2) UBF
                           LEFT JOIN (select school_id,
                                             lesson_package_code,
                                             contract_no,
                                             student_code,
                                             biz_type,
                                             associated_biz_no,
                                             biz_code
                                      from ods.ods_order_up_market_sign_um_biz_flow_item_da
                                      where dt = '${date}') UBFI
                                     ON UBFI.associated_biz_no = UBF.associated_biz_no
                                         AND UBFI.biz_code = UBF.biz_code
                  WHERE (UBFI.contract_no IS NULL OR
                         UBFI.contract_no = '')
                    AND UBFI.biz_type IN ('D', 'J')
                  union all
                  SELECT UBFI.school_id             AS                  school_id,
                         UBFI.lesson_package_code   AS                  lesson_package_code,
                         NULL                       AS                  contract_no,
                         date_format(UBF.completion_time, 'yyyy-MM-dd') ymd,
                         UBFI.student_code          AS                  student_code,
                         cast(UBF.amount as string) AS                  deposit_amount,
                         '3'                        AS                  bizinout_type
                  FROM (select completion_time, amount, associated_biz_no, biz_code, biz_status
                        from ods.ods_order_up_market_sign_um_biz_flow_da
                        where dt = '${date}'
                          and biz_status = 2) UBF
                           LEFT JOIN (select school_id,
                                             lesson_package_code,
                                             student_code,
                                             associated_biz_no,
                                             biz_code,
                                             contract_no,
                                             biz_type
                                      from ods.ods_order_up_market_sign_um_biz_flow_item_da
                                      where dt = '${date}') UBFI
                                     ON UBFI.associated_biz_no = UBF.associated_biz_no
                                         AND UBFI.biz_code = UBF.biz_code
                  WHERE length(if(ltrim(UBFI.contract_no) is null, '', UBFI.contract_no)) > 0
                    AND UBFI.biz_type = 'D'
                  union all
                  SELECT UBFI.school_id              AS                school_id,
                         UBFI.lesson_package_code    AS                lesson_package_code,
                         NULL                        AS                contract_no,
                         date_format(UB.completion_time, 'yyyy-MM-dd') ymd,
                         UBFI.student_code           AS                student_code,
                         cast(-UBF.amount as string) AS                deposit_amount,
                         '3'                         AS                bizinout_type
                  FROM (select amount, biz_status, associated_biz_no, biz_code
                        from ods.ods_order_up_market_sign_um_biz_flow_da
                        where dt = '${date}') UBF
                           LEFT JOIN (select school_id,
                                             lesson_package_code,
                                             student_code,
                                             biz_type,
                                             associated_biz_no,
                                             biz_code,
                                             contract_no
                                      from ods.ods_order_up_market_sign_um_biz_flow_item_da
                                      where dt = '${date}') UBFI
                                     ON UBFI.associated_biz_no = UBF.associated_biz_no
                                         AND UBFI.biz_code = UBF.biz_code
                           LEFT JOIN (select biz_status, associated_biz_no, biz_type, completion_time
                                      from ods.ods_order_up_market_sign_um_biz_flow_da
                                      where dt = '${date}') UB
                                     ON UBF.associated_biz_no = UB.associated_biz_no
                  WHERE UBF.biz_status = 2
                    AND UB.biz_status = 2
                    AND length(if(UBFI.contract_no is null, '', UBFI.contract_no)) > 0
                    AND UBFI.biz_type = 'D'
                    AND UB.biz_type = 'W') A1
            GROUP BY A1.school_id,
                     A1.lesson_package_code,
                     A1.contract_no,
                     A1.ymd,
                     A1.student_code,
                     A1.bizinout_type) A
               LEFT join (SELECT UBFIA.school_id            AS                   school_id,
                                 UBFIA.contract_no          AS                   contract_no,
                                 date_format(UBFA.completion_time, 'yyyy-MM-dd') ymd,
                                 NVL(UBFIA.total_amount, 0) AS                   amount,
                                 1                                               reg
                          FROM (select school_id, contract_no, total_amount, associated_biz_no, biz_code, biz_type
                                from ods.ods_order_up_market_sign_um_biz_flow_item_da
                                where dt = '${date}') UBFIA
                                   LEFT JOIN (select biz_status, associated_biz_no, biz_code, completion_time
                                              from ods.ods_order_up_market_sign_um_biz_flow_da
                                              where dt = '${date}') UBFA
                                             ON UBFIA.associated_biz_no = UBFA.associated_biz_no
                                                 AND UBFIA.biz_code = UBFA.biz_code
                          WHERE UBFIA.biz_type IN ('B', 'Z', 'W')
                            AND UBFA.biz_status = 2
                            AND UBFIA.contract_no IS NOT NULL
                            AND UBFIA.contract_no != ' '
                          group by UBFIA.school_id,
                                   UBFIA.contract_no,
                                   date_format(UBFA.completion_time, 'yyyy-MM-dd'),
                                   NVL(UBFIA.total_amount, 0)) B
                         ON A.school_id = B.school_id
                             AND A.contract_no = B.contract_no
                             AND A.ymd = B.ymd
                             AND A.bizinout_type = 1
               LEFT JOIN (SELECT UBFIA.school_id                 AS              school_id,
                                 UBFIA.contract_no               AS              contract_no,
                                 date_format(UBFA.completion_time, 'yyyy-MM-dd') ymd,
                                 SUM(NVL(UBFIA.total_amount, 0)) AS              amount
                          FROM (select school_id, contract_no, total_amount, biz_type, associated_biz_no, biz_code
                                from ods.ods_order_up_market_sign_um_biz_flow_item_da
                                where dt = '${date}') UBFIA
                                   LEFT JOIN (select completion_time, associated_biz_no, biz_code, biz_status
                                              from ods.ods_order_up_market_sign_um_biz_flow_da
                                              where dt = '${date}') UBFA
                                             ON UBFIA.associated_biz_no = UBFA.associated_biz_no
                                                 AND UBFIA.biz_code = UBFA.biz_code
                          WHERE UBFIA.biz_type = 'T'
                            AND UBFA.biz_status = 2
                            AND UBFIA.contract_no IS NOT NULL
                            AND UBFIA.contract_no != ' '
                          GROUP BY UBFIA.school_id,
                                   UBFIA.contract_no,
                                   date_format(UBFA.completion_time, 'yyyy-MM-dd')) C
                         ON A.school_id = C.school_id
                             AND A.contract_no = C.contract_no
                             AND A.ymd = A.ymd
                             AND A.bizinout_type = 1
               LEFT JOIN (select UBFI.school_id           as                    school_id,
                                 UBFI.lesson_package_code as                    lesson_package_code,
                                 UBFI.contract_no         as                    contract_no,
                                 date_format(UBF.completion_time, 'yyyy-MM-dd') ymd,
                                 1                                              cnt
                          from (select biz_type, biz_status, completion_time, biz_code, associated_biz_no
                                from ods.ods_order_up_market_sign_um_biz_flow_da
                                where dt = '${date}') UBF
                                   left join (select school_id,
                                                     lesson_package_code,
                                                     contract_no,
                                                     biz_code,
                                                     associated_biz_no,
                                                     affected_flag
                                              from ods.ods_order_up_market_sign_um_biz_flow_item_da
                                              where dt = '${date}') UBFI
                                             ON UBFI.associated_biz_no = UBF.associated_biz_no
                                                 AND UBFI.biz_code = UBF.biz_code
                                   left join (select contract_no, school_id, package_status
                                              from ods.ods_order_up_market_sign_um_lesson_package_detail_da
                                              where dt = '${date}') ULPD
                                             on UBFI.contract_no = ULPD.contract_no and UBFI.school_id = ULPD.school_id
                          where UBF.biz_type in ('T')
                            and ULPD.package_status = 5
                            and UBF.biz_status = 2
                            and UBFI.affected_flag = 0) D
                         ON A.school_id = D.school_id
                             AND A.contract_no = D.contract_no
                             AND A.ymd = D.ymd
                             AND A.bizinout_type = 1
               LEFT JOIN (select FMTR.rate        as rate,
                                 UBFI.contract_no as contract_no,
                                 UBF.school_id    as school_id
                          from (select school_id, associated_biz_no, biz_code, completion_time
                                from ods.ods_order_up_market_sign_um_biz_flow_da
                                where dt = '${date}') UBF
                                   left join (select associated_biz_no, biz_code, contract_no
                                              from ods.ods_order_up_market_sign_um_biz_flow_item_da
                                              where dt = '${date}') UBFI
                                             ON UBFI.associated_biz_no = UBF.associated_biz_no
                                                 AND UBFI.biz_code = UBF.biz_code
                                   left join (select month_id, rate
                                              from dwd.dwd_fin_month_tax_rate_da
                                              where dt = '${date}') FMTR
                                             on date_format(UBF.completion_time, 'yyyyMM') = FMTR.month_id) E
                         on A.school_id = E.school_id
                             AND A.contract_no = E.contract_no
                             AND A.bizinout_type = 1
     ) t1;

insert into table dws.dws_order_um_income_reg_da partition (dt = '${date}')
select t1.school_id                                        as school_id                -- 学校ID
     , t1.lesson_package_code                              as lesson_package_code      --  课时包编码
     , t2.contract_no                                      as contract_no              --  合同编号
     , date_format(t3.create_time, 'yyyy-MM-dd')           as biz_date                 --  业务日期
     , t3.biz_type                                         as biz_type                 -- 业务类型
     , t1.student_code                                     as student_code             --  学员号
     , t3.history_flag                                     as history_flag             --  0:非历史数据 1:历史数据; 导入数据用
     , sum(t3.total_amount)                                as metric_income_amt        -- 现金收入
     , sum(t3.total_amount) * (1 - t4.rate)                as metric_income_ex_tax_amt -- 现金收入不含税
     , sum(if(t3.biz_type <> 'T', t3.total_amount, 0))     as metric_reg_amt           -- 报入金额
     , sum(if(t3.biz_type = 'T', t3.total_amount, 0))      as metric_cancel_amt        -- 退费金额
     , max(case when t3.biz_type = 'T' then -1 else 1 end) as metric_reg_all_cnt       -- 报名人次
     , if(t3.biz_type <> 'T', 1, 0)                        as metric_reg_in_cnt        -- 报入人次
     , if(t3.biz_type <> 'T', -1, 0)                       as metric_cancel_cnt        -- 退费人次
     , null                                                as ext1                     --扩展字段1
     , null                                                as ext2                     --扩展字段2
     , null                                                as ext3                     --扩展字段3
     , CURRENT_TIMESTAMP                                   as etl_time                 --运行时间
from (select school_id, lesson_package_code, student_code
      from ods.ods_class_up_market_sign_um_lesson_package_da
      where dt = '${date}') t1
         inner join (select contract_no, package_code
                     from ods.ods_order_up_market_sign_um_lesson_package_detail_da
                     where dt = '${date}') t2
                    on t1.lesson_package_code = t2.package_code
         inner join (select a.school_id,
                            a.contract_no,
                            a.create_time,
                            a.biz_code,
                            a.biz_type,
                            a.history_flag,
                            a.lesson_package_code,
                            a.student_code,
                            a.total_amount,
                            umbiz.biz_order_type,
                            row_number()
                                    over (partition by a.school_id,a.contract_no,a.student_code,a.biz_type order by a.create_time desc ) rn
                     from (select school_id,
                                  contract_no,
                                  student_code,
                                  biz_type,
                                  create_time,
                                  lesson_package_code,
                                  history_flag,
                                  biz_code,
                                  total_amount
                           from ods.ods_order_up_market_sign_um_biz_flow_item_da
                           where dt = '${date}') a
                              left join (select biz_order_type, biz_code
                                         from ods.ods_order_up_market_sign_um_biz_flow_da
                                         where dt = '${date}') umbiz
                                        on a.biz_code = umbiz.biz_code) t3
                    on t3.lesson_package_code = t2.package_code
                        and t2.contract_no = t3.contract_no
                        and rn = 1
                        and t3.history_flag = 1
         left join(select FMTR.rate                as rate,
                          UBFI.contract_no         as contract_no,
                          UBF.school_id            as school_id,
                          UBFI.lesson_package_code as lesson_package_code
                   from (select school_id, associated_biz_no, biz_code, completion_time
                         from ods.ods_order_up_market_sign_um_biz_flow_da
                         where dt = '${date}') UBF
                            left join (select associated_biz_no, biz_code, contract_no, lesson_package_code
                                       from ods.ods_order_up_market_sign_um_biz_flow_item_da
                                       where dt = '${date}') UBFI
                                      ON UBFI.associated_biz_no = UBF.associated_biz_no
                                          AND UBFI.biz_code = UBF.biz_code
                            left join (select month_id, rate
                                       from dwd.dwd_fin_month_tax_rate_da
                                       where dt = '${date}') FMTR
                                      on date_format(UBF.completion_time, 'yyyyMM') = FMTR.month_id) t4
                  on t1.lesson_package_code = t4.lesson_package_code
group by t1.lesson_package_code                    --  课时包编码
       , t2.contract_no                            --  合同编号
       , t1.school_id                              -- 学校ID
       , t3.biz_type                               -- 业务类型
       , date_format(t3.create_time, 'yyyy-MM-dd') -- 业务日期
       , t1.student_code                           --  学员号
       , t3.history_flag                           --0:非历史数据 1:历史数据; 导入数据用
       , t4.rate
       , if(t3.biz_type <> 'T', 1, 0)
       , if(t3.biz_type <> 'T', -1, 0);
