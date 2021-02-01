insert overwrite table dws.dws_um_income_reg_a
select t1.school_id                                                         as school_id,                --学校ID
       t1.lesson_package_code                                               as lesson_package_code,      --课时包编码
       t1.contract_no                                                       as contract_no,              --合同编号
       biz_date                                                             as biz_date,                 --业务日期
       CASE
           WHEN length(t1.lesson_package_code) = 0 THEN 'D'
           ELSE CASE
                    WHEN t1.reg > 0 THEN 'B'
                    ELSE CASE WHEN t1.cnt >= 1 THEN 'T' ELSE '' END END END as biz_type,                 --业务类型B:报名 T: 退费 S：升级 Z：追加报名 D:订金 W:尾款 G:赠送 J:退订金
       t1.student_code                                                      as student_code,             --学员号
       0                                                                    as history_flag,             --0:非历史数据 1:历史数据; 导入数据用
       t1.deposit_amount + t1.reg_amount - t1.cancel_amount                 as metric_income_amt,        --现金收入
       metric_income_ex_tax_amt                                             as metric_income_ex_tax_amt, --现金收入不含税
       t1.REG_AMOUNT                                                        as metric_reg_amt,           --报入金额
       t1.CANCEL_AMOUNT                                                     as metric_cancel_amt,        --退费金额
       case
           when t1.reg > 0 then 1
           else case when t1.cnt >= 1 then -1 else 0 end END                as metric_reg_all_cnt,       --报名人次
       t1.reg                                                               as metric_reg_in_cnt,        --报入人次
       t1.cnt                                                               as metric_cancel_cnt,        --退费人次
       CURRENT_TIMESTAMP                                                    as etl_time                  --运行时间
from (SELECT A.lesson_package_code,
             A.contract_no,
             A.school_id,
             A.ymd,
             A.product_id,
             A.product_name,
             A.grade_code,
             A.grade_name,
             A.area_code,
             A.area_name,
             A.season_code,
             A.season_name,
             A.subject_code,
             A.subject_name,
             A.order_code,
             A.student_code,
             A.student_name,
             A.deposit_amount AS              deposit_amount,
             NVL(B.AMOUNT, 0) AS              reg_amount,
             NVL(C.AMOUNT, 0) AS              cancel_amount,
             nvl(d.cnt, 0)                    cnt,
             nvl(B.reg, 0)                    reg,
             A.classmates,
             A.lesson_num + A.gift_lesson_num lesson_num
      from (SELECT A.lesson_package_code,
                   A.school_id,
                   A.ymd,
                   A.product_id,
                   A.product_name,
                   A.grade_code,
                   A.grade_name,
                   A.area_code,
                   A.area_name,
                   A.season_code,
                   A.season_name,
                   A.subject_code,
                   A.subject_name,
                   A.contract_no,
                   A.order_code,
                   A.student_code,
                   A.student_name,
                   SUM(A.deposit_amount) AS deposit_amount,
                   A.bizinout_type,
                   A.classmates,
                   A.lesson_num,
                   A.gift_lesson_num
            FROM (
                     SELECT UBFI.lesson_package_code,
                            UBFI.SCHOOL_ID    AS                           school_id,
                            date_format(UBF.COMPLETION_TIME, 'yyyy-MM-dd') ymd,
                            UBFI.PRODUCT_ID   AS                           product_id,
                            UBFI.PRODUCT_NAME AS                           product_name,
                            UBFI.GRADE_CODE   AS                           grade_code,
                            UBFI.GRADE_NAME   AS                           grade_name,
                            UBFI.AREA_CODE    AS                           area_code,
                            UBFI.AREA_NAME    AS                           area_name,
                            UBFI.CONTRACT_NO  AS                           contract_no,
                            UBFI.ORDER_CODE   AS                           order_code,
                            UBFI.STUDENT_CODE AS                           student_code,
                            UBFI.STUDENT_NAME AS                           student_name,
                            0                 AS                           deposit_amount,
                            1                 AS                           bizinout_type,
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
                            UBFI.SCHOOL_ID    AS                           school_id,
                            date_format(UBF.COMPLETION_TIME, 'yyyy-MM-dd') ymd,
                            NULL              AS                           product_id,
                            NULL              AS                           product_name,
                            UBFI.GRADE_CODE   AS                           grade_code,
                            UBFI.GRADE_NAME   AS                           grade_name,
                            UBFI.AREA_CODE    AS                           area_code,
                            UBFI.AREA_NAME    AS                           area_name,
                            UBFI.CONTRACT_NO  AS                           contract_no,
                            UBFI.ORDER_CODE   AS                           order_code,
                            UBFI.STUDENT_CODE AS                           STUDENT_CODE,
                            UBFI.STUDENT_NAME AS                           student_name,
                            CASE
                                WHEN UBFI.BIZ_TYPE = 'D' THEN
                                    UBF.AMOUNT
                                ELSE
                                    -UBF.AMOUNT
                                END           AS                           deposit_amount,
                            2                 AS                           bizinout_type,
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
                            UBFI.SCHOOL_ID    AS                           school_id,
                            date_format(UBF.COMPLETION_TIME, 'yyyy-MM-dd') ymd,
                            NULL              AS                           product_id,
                            NULL              AS                           product_name,
                            UBFI.GRADE_CODE   AS                           grade_code,
                            UBFI.GRADE_NAME   AS                           grade_name,
                            UBFI.AREA_CODE    AS                           area_code,
                            UBFI.AREA_NAME    AS                           area_name,
                            NULL              AS                           contract_no,
                            UBFI.ORDER_CODE   AS                           order_code,
                            UBFI.STUDENT_CODE AS                           student_code,
                            UBFI.STUDENT_NAME AS                           student_name,
                            UBF.AMOUNT        AS                           deposit_amount,
                            3                 AS                           bizinout_type,
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
                            UBFI.SCHOOL_ID    AS                          school_id,
                            date_format(UB.COMPLETION_TIME, 'yyyy-MM-dd') ymd,
                            NULL              AS                          product_id,
                            NULL              AS                          product_name,
                            UBFI.GRADE_CODE   AS                          grade_code,
                            UBFI.GRADE_NAME   AS                          grade_name,
                            UBFI.AREA_CODE    AS                          area_code,
                            UBFI.AREA_NAME    AS                          area_name,
                            NULL              AS                          contract_no,
                            UBFI.ORDER_CODE   AS                          order_code,
                            UBFI.STUDENT_CODE AS                          student_code,
                            UBFI.STUDENT_NAME AS                          student_name,
                            -UBF.AMOUNT       AS                          deposit_amount,
                            3                 AS                          bizinout_type,
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
                     A.school_id,
                     A.product_id,
                     A.ymd,
                     A.product_name,
                     A.grade_code,
                     A.grade_name,
                     A.area_code,
                     A.area_name,
                     A.season_code,
                     A.season_name,
                     A.subject_code,
                     A.subject_name,
                     A.contract_no,
                     A.order_code,
                     A.student_code,
                     A.student_name,
                     A.bizinout_type,
                     A.classmates,
                     A.lesson_num,
                     A.gift_lesson_num) A
               LEFT join (SELECT UBFIA.SCHOOL_ID            AS                   school_id,
                                 UBFIA.CONTRACT_NO          AS                   contract_no,
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
                         ON A.school_id = B.school_id
                             AND A.contract_no = B.contract_no
                             AND A.YMD = B.YMD
                             AND A.bizinout_type = 1
               LEFT JOIN (SELECT UBFIA.SCHOOL_ID                 AS              school_id,
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
                         ON A.school_id = C.school_id
                             AND A.contract_no = C.CONTRACT_NO
                             AND A.YMD = C.YMD
                             AND A.bizinout_type = 1
               LEFT JOIN (select a.lesson_package_code,
                                 a.contract_no,
                                 a.nschool_id,
                                 date_format(a.umbiz_completion_time, 'yyyy-MM-dd') ymd,
                                 1                                                  cnt
                          from dws.dws_um_biz_flow_dtl a
                                   left join dws.dws_um_contract_detail b
                                             on a.contract_no = b.contract_no and a.nschool_id = b.nschool_id
                          where a.umbiz_type in ('T')
                            and b.package_status = 5
                            and a.umbiz_biz_status = 2
                            and a.affected_flag = 0) d
                         ON A.school_id = d.nschool_id
                             AND A.contract_no = d.contract_no
                             AND A.YMD = d.ymd
                             AND A.bizinout_type = 1) t1