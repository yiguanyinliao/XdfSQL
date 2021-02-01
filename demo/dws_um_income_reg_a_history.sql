INSERT INTO dws.dws_um_income_reg_a
select t1.school_id                                           as school_id                -- 学校ID
     , t1.lesson_package_code                                 as lesson_package_code      --  课时包编码
     , t2.contract_no                                         as contract_no              --  合同编号
     , date_format(t3.create_time, 'yyyy-MM-dd')             as biz_date                 --  业务日期
     , t3.biz_type                                           as biz_type                 -- 业务类型
     , t1.student_code                                        as student_code             --  学员号
     , t3.history_flag                                       as history_flag             --  0:非历史数据 1:历史数据; 导入数据用
     , sum(t3.total_amount)                                  as metric_income_amt        -- 现金收入
     , sum(t3.total_amount) * (1 - t4.rate)                   as metric_income_ex_tax_amt -- 现金收入不含税
     , sum(if(t3.biz_type <> 'T', t3.total_amount, 0))      as metric_reg_amt           -- 报入金额
     , sum(if(t3.biz_type = 'T', t3.total_amount, 0))       as metric_cancel_amt        -- 退费金额
     , max(case when t3.biz_type = 'T' then -1 else 1 end)   as metric_reg_all_cnt       -- 报名人次
     , if(t3.biz_type <> 'T', 1, 0)                          as metric_reg_in_cnt        -- 报入人次
     , if(t3.biz_type <> 'T', -1, 0)                         as metric_cancel_cnt        -- 退费人次
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as etl_time                 -- etl时间
from ods.ods_class_up_market_sign_um_lesson_package_da t1
         inner join ods.ods_order_up_market_sign_um_lesson_package_detail_da t2
                    on t1.lesson_package_code = t2.package_code
         inner join (select a.*,
                            umbiz.biz_order_type,
                            row_number()
                                    over (partition by a.school_id,a.contract_no,a.student_code,a.biz_type order by a.create_time desc ) rn
                     from ods.ods_order_up_market_sign_um_biz_flow_item_da a
                              left join ods.ods_order_up_market_sign_um_biz_flow_da umbiz
                                        on a.biz_code = umbiz.biz_code) t3
                    on t3.lesson_package_code = t2.package_code
                        and t2.contract_no = t3.contract_no
                        and rn = 1
                        and t3.history_flag = 1
         left join (select FMTR.rate                as rate,
                           UBFI.lesson_package_code as lesson_package_code,
                           UBFI.contract_no,
                           UBFI.school_id           as school_id
                    from ods.ods_order_up_market_sign_um_biz_flow_item_da UBFI
                             left join dwd.dwd_fin_month_tax_rate_da FMTR
                                       on date_format(UBFI.create_time, 'yyyyMM') = FMTR.smonth) t4
                   on t1.lesson_package_code = t4.lesson_package_code

group by t1.lesson_package_code                     --  课时包编码
       , t2.contract_no                             --  合同编号
       , t1.school_id                               -- 学校ID
       , t3.biz_type                               -- 业务类型
       , date_format(t3.create_time, 'yyyy-MM-dd') ---- 业务日期
       , t1.student_code                            --  学员号
       , t3.history_flag
       , if(t3.biz_type <> 'T', 1, 0)
       , if(t3.biz_type <> 'T', -1, 0)
