-- author: hewenhao
-- describe: 合同明细
-- createTime: 2020-03-24
-- updatType: 全量
-- computeCycles: 天
-- targetTable: dws_um_biz_flow_dtl
-- fromTable: um_lesson_package、um_biz_flow_item、um_biz_flow

    -- 参数加载
    set mapred.job.name = dws_um_biz_flow_dtl;
    set hive.execution.engine  = tez;
    --设置每个reduce处理数据为1G
    set hive.exec.reducers.bytes.per.reducer=1000000000;
    --设置任务的最大reduce数
    set hive.exec.reducers.max = 200;
    set hive.optimize.skewjoin = true;
    set mapred.max.split.size=128000000;
    set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
    set hive.merge.mapfiles    = true;
    set hive.merge.tezfiles    = true;
    set hive.merge.mapredfiles = true;
    set hive.merge.size.per.task = 128000000;
    set hive.merge.smallfiles.avgsize=100000000;

    insert overwrite table dws.dws_um_biz_flow_dtl
    select umbiz.id as id1, --  合同ID
           umbiz.school_id, --  学校ID
           umbiz.biz_code, --  业务编码
           umbiz.biz_type, --  业务类型 B:报名 T: 退费 S：升级 Z：追加报名 D:订金 W:尾款 G:赠送 J:退订金
		   umbiz.biz_order_type as umbiz_order_type, -- 签约类型 1：新签 2：续费 3：赠送
           umbiz.operator_code, --  操作人编码
           umbiz.operator_name, --  操作人姓名
		   umbiz.operator_mail, --  操作人邮箱
           umbiz.phone, --  电话号码
           umbiz.lesson_num, --  总课时
           umbiz.amount, --  总金额
           umbiz.associated_biz_no, --  关联业务单号
           umbiz.reg_zone_name, --  报名点
           umbiz.reg_zone_code, --  报名点编码
           umbiz.cashier_code, --  收银员编码
           umbiz.cashier_name, --  收银员名称
           umbiz.in_out_type, --  1：收入 2：支出
           umbiz.biz_status, --  状态 1：办理中 2：办理完成 3：取消
           umbiz.completion_time, --  办理完成时间
           umbiz.pay_code, --  支付号
           umbiz.ext, --  扩展字段
           umbiz.create_time, --  创建时间
           umbiz.update_time, --
           umbizit.id, --  合同明细ID
           umbizit.student_code, --
           umbizit.student_name, --
           umbizit.product_id, --  产品编码
           umbizit.product_name, --  产品名称
           gra.code,
           umbizit.grade_code, --  年级编码
           umbizit.grade_name, --  年级名称
           sub.scode as scode1,
           umbizit.subject_code, --  科目编码
           umbizit.subject_name, --  科目名称
           umbizit.classmates, --  班容
           qua.scode,
           umbizit.season_code, --  季度编码
           umbizit.season_name, --  季度名称
           umbizit.resource_days, --  住宿天数
           umbizit.lesson_fee, --  课时费
           umbizit.other_fee, --  其他费用
           umbizit.resource_fee, --  住宿费
           umbizit.book_fee, --  教材费
           umbizit.original_fee, --  课时原费用
           umbizit.total_amount, --  总额
       --    umbizit.biz_type, --  业务类型 B:报名 T: 退费 S：升级 Z：追加报名 D:订金 W:尾款 J:退订金 G:赠送
           umbizit.associated_biz_no, --  关联业务单号
           umbizit.lesson_num, --  课时
           umbizit.gift_lesson_num, --  赠送课时
           umbizit.coupon_code, --  优惠券号
           umbizit.lesson_package_code, --  课时包编码
           umbizit.contract_no, --  合同编码
           umbizit.order_code, --  订单号(退费用，原订单号)
           umbizit.batch_code, --  批次号
           umbizit.old_batch_code, --  原批次号
           umbizit.batch_item_code, --  批次行号
           umbizit.item_group_code, --  批次行组号
           umbizit.old_item_code, --  原批次行号
           umbizit.affected_flag, --  是否受影响区分 0:本订单1:受影响订单
           umbizit.price_type, --  定价方式
           umbizit.area_code, --  校区编码
           umbizit.area_name, --  校区名称
           umbizit.price_code, --
           umbizit.history_flag, --  0:非历史数据 1:历史数据; 导入数据用
           umbizit.create_time, --
           umbizit.update_time, --
		   ps.manager,      -- 学管
		   ps.adviser,-- 咨询顾问
           current_timestamp()
      from gaoduan.um_biz_flow_item umbizit
     inner join gaoduan.um_biz_flow umbiz
        on umbizit.biz_code = umbiz.biz_code
      left join gaoduan.um_lesson_package um
        on um.lesson_package_code = umbizit.lesson_package_code
      left join (select value_code,code,gradetype
                   from jwyth.grade
                  where value_code is not null
                    and value_code <> '') gra
        on umbizit.grade_code = gra.value_code
       and um.depart_code = gra.gradetype
      left join (select value_code, scode
                   from jwyth.d_classsubject
                  where value_code is not null
                    and value_code <> '') sub
        on sub.value_code = umbizit.subject_code
      left join jwyth.d_deptstd dep
        on dep.sfcode = um.depart_code
      left join (select value_code, scode
                   from jwyth.d_quarter
                  where value_code is not null
                    and value_code <> '') qua
        on qua.value_code = umbizit.season_code
	left join gaoduan.um_vip_student ps
	    on umbizit.school_id=ps.school_id
	   and umbizit.student_code=ps.student_code
