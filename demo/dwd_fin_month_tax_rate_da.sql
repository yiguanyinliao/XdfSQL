insert overwrite table dwd.dwd_fin_month_tax_rate_da partition (dt = '${date}')
select school_id         as school_id, --学校
       month_id          as month_id,  --自然月
       rate / 100        as rate,      --税率
       CURRENT_TIMESTAMP as etl_time   --运行时间
from ods.ods_fin_sxuse_acc_report_tex_rate_da
         lateral view
             explodeMonth(begin_date, Nvl(end_date, CURRENT_TIMESTAMP)) tmp as month_id
where dt = '${date}';