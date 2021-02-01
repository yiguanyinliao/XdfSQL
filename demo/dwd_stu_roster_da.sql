--********************************************************************--
--Author: 胡广超
--CreateTime: 2020-11-16 19:28:29
--Comment: dwd花名册
--UpdatType: 全量
--ComputeCycles: 天
--TargetTable: dwd_stu_roster_da
--FromTable: ods_reg_bm_bs_roster_di
--********************************************************************--

--参数加载
set hive.exec.reducers.bytes.per.reducer=1000000000;
set mapred.max.split.size=128000000;
set mapred.min.split.size.per.node=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.merge.mapfiles = true;
set hive.merge.tezfiles = true;
set hive.merge.size.per.task = 128000000;
set hive.merge.smallfiles.avgsize = 50000000;
insert overwrite table dwd.dwd_stu_roster_da partition (dt = '${date}')
select id                as id,               --id
       tid               as tid,              --ID
       school_id         as school_id,        --学校ID
       card_code         as card_code,        --听课证号
       class_code        as class_code,       --班级编码
       student_code      as student_code,     --学员编号
       valid             as valid,            --是否有效
       is_insert         as is_insert,        --插班标志
       in_date           as in_date,          --进班日期
       from_class_code   as from_class_code,  --原班号
       out_date          as out_date,         --离班日期
       to_class_code     as to_class_code,    --转出班号
       seat_no           as seat_no,          --座位号
       start_lesson      as start_lesson,     --开始课次
       realstart_lesson  as realstart_lesson, --实际开始课次
       end_lesson        as end_lesson,       --结束课次
       realend_lesson    as realend_lesson,   --实际结束课次
       in_type           as in_type,          --入班方式
       out_type          as out_type,         --离班方式
       in_biz            as in_biz,           --入班流水号
       out_biz           as out_biz,          --离班流水号
       pay               as pay,              --学费
       voucher           as voucher,          --优惠金额
       rsrv_book_fee     as rsrv_book_fee,    --已退教材金额
       comment           as comment,          --备注
       has_biz_times     as has_biz_times,    --业务操作次数
       web_reg_bill_code as web_reg_bill_code,--网上报名单号
       in_time           as in_time,          --进班时间
       out_time          as out_time,         --离班时间
       new_card_code     as new_card_code,    --新听课证号（只有转入和延转入的时候，才有值）
       old_stu_voucher   as old_stu_voucher,  --老生优惠金额
       agent_name        as agent_name,       --代办人名称
       agent_id_card     as agent_id_card,    --代办人ID
       channel           as channel,          --渠道
       biz_memo          as biz_memo,         --业务备注
       modify            as modify,           --修改日期
       vchr_trans_fee    as vchr_trans_fee,   --优惠转移金额
       reg_zone_code     as reg_zone_code,    --报名点
       order_code        as order_code,       --订单编号
       tran_can_rsn_type as tran_can_rsn_type,--转退班类型
       tran_can_rsn_item as tran_can_rsn_item,--转退班选项
       old_rcmd_stu_code as old_rcmd_stu_code,--老推新学员号
       sub_channel       as sub_channel,      --子渠道
       system_source     as system_source,    --系统来源
       mkt_sources       as mkt_sources,      --市场营销来源
       used_pay          as used_pay,         --听课证消耗金额
       is_occupation     as is_occupation,    --是否占名额
       CURRENT_TIMESTAMP as etl_time          --ETL时间戳
from ods.ods_stu_bm3_bs_roster_da
      where dt = '${date}';