set mapred.job.name=ods.ods_user_xdfxm_user_info_da;
set hive.auto.convert.join=true;
set hive.optimize.skewjoin=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=100000000;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles = true;
set hive.merge.smallfiles.avgsize=64000000;
set hive.merge.size.per.task=230000000;

insert overwrite table dwd.dwd_rt_stu_roster
select regexp_replace(if(ros.id ='', null, ros.id),' ',''),
       regexp_replace(if(ros.tid ='', null, ros.tid),' ',''),
       regexp_replace(if(ros.school_id ='', null, ros.school_id),' ',''),
       cls.school_name            as school_name,
       cls.f_dept_code,
       cls.f_dept_name,
       cls.management_dept_code,
       cls.management_dept_name,
       cls.management_code,
       cls.management_name,
       ros.student_code,
       stu.student_name           as student_name,
       ros.card_code,
       ros.new_card_code,
       cls.class_code,
       cls.class_name,
       ros.from_class_code,
       ros.to_class_code,
       regexp_replace(if(ros.valid ='', null, ros.valid),' ','') as is_valid,
       ros.in_date,
       regexp_replace(if(ros.in_type ='', null, ros.in_type),' ',''),
       tin.biz_type_name          as in_type_name,
       ros.pay,
       regexp_replace(if(ros.start_lesson ='', null, ros.start_lesson),' ',''),
       regexp_replace(if(ros.realstart_lesson ='', null, ros.realstart_lesson),' ','') as real_start_lesson,
       ros.out_date,
       regexp_replace(if(ros.out_type ='', null, ros.out_type),' ',''),
       tout.biz_type_name         as out_type_name,
       regexp_replace(if(ros.realend_lesson ='', null, ros.realend_lesson),' ','')   as real_end_lesson,
       regexp_replace(if(ros.channel ='', null, ros.channel),' ',''),
       cha.channel_name           as channel_name,
       ros.biz_memo,
       ros.reg_zone_code,
       ros.tran_can_rsn_type,
       ros.tran_can_rsn_item,
       ros.old_rcmd_stu_code,
       regexp_replace(if(cls.state ='', null, cls.state),' ',''),
       regexp_replace(if(cls.can_register ='', null, cls.can_register),' ',''),
       regexp_replace(if(cls.is_end ='', null, cls.is_end),' ',''),
       cls.print_address,
       cls.room_code,
       cls.room_name,
       cls.begin_date,
       cls.end_date,
       cls.real_begin_date,
       cls.real_end_date,
       regexp_replace(if(cls.lesson ='', null, cls.lesson),' ',''),
       regexp_replace(if(cls.normal_count ='', null, cls.normal_count),' ',''),
       regexp_replace(if(cls.max_count ='', null, cls.max_count),' ',''),
       regexp_replace(if(cls.fee ='', null, cls.fee),' ',''),
       regexp_replace(if(cls.current_count ='', null, cls.current_count),' ',''),
       regexp_replace(if(cls.audit_status ='', null, cls.audit_status),' ',''),
       cls.setup_date,
       regexp_replace(if(cls.fyear ='', null, cls.fyear),' ',''),
       regexp_replace(if(cls.plan_minutes ='', null, cls.plan_minutes),' ',''),
       regexp_replace(if(cls.real_minutes ='', null, cls.real_minutes),' ',''),
       regexp_replace(if(cls.is_vip ='', null, cls.is_vip),' ',''),
       cls.cls_mode,
       cls.cls_mode_name,
       cls.quarter_inner,
       cls.quarter_inner_name,
       cls.area_code,
       cls.area_name,
       cls.class_capacity_property,
       regexp_replace(if(cls.continue_remark ='', null, cls.continue_remark),' ',''),
       cls.product_system_id,
       cls.product_system_name,
       cls.product_level_id,
       cls.product_level_name,
       cls.relation_code,
       cls.relation_class_name,
       cls.relation_mode,
       cls.class_subject_inner,
       cls.class_subject_inner_name,
       regexp_replace(if(cls.business_year ='', null, cls.business_year),' ',''),
       cls.mother_class_id,
       cls.project_name,
       cls.project_code,
       cls.grade_inner,
       cls.grade_inner_name,
       cls.class_type_code,
       cls.class_type_name,
       regexp_replace(if(cls.is_reside_class ='', null, cls.is_reside_class),' ',''),
       cls.main_teacher,
       cls.main_teacher_name,
       cls.min_dept_code,
       cls.teaching_method_ext,
       cls.teaching_method_name_ext,
       CURRENT_TIMESTAMP()        as etl_time
from dwd.dwd_stu_roster_da ros
         left join (select *
                    from (select *, row_number() over (partition by id order by etl_time desc) rn
                          from ods.ods_dim_xdfnis_d_channel_da
                          where dt = '${date}') b
                    where b.rn = 1) cha on ros.channel = cha.id
         left join (select *
                    from (select *, row_number() over (partition by id order by etl_time desc) rn
                          from ods.ods_dim_xdfnis_d_biztype_da
                          where dt = '${date}') c
                    where c.rn = 1) tin on ros.in_type = tin.id
         left join (select *
                    from (select *, row_number() over (partition by id order by etl_time desc) rn
                          from ods.ods_dim_xdfnis_d_biztype_da
                          where dt = '${date}') d
                    where d.rn = 1) tout on ros.out_type = tout.id
         left join (select *
                    from (select *, row_number() over (partition by school_id,student_code order by etl_time desc) rn
                          from dim.dim_stu_student_da
                          where dt = '${date}') e
                    where e.rn = 1) stu
                   on ros.school_id = stu.school_id and ros.student_code = stu.student_code
         left join dwd.dwd_tmp_rt_class cls on ros.school_id = cls.school_id and ros.class_code = cls.class_code
where ros.dt = '${date}'
