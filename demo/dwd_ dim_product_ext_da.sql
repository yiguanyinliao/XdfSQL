insert overwrite table dwd.dwd_dim_product_ext_da partition (dt = '${date}')
select t1.school_id as school_id
     , - -学校ID
  t1.product_code            as product_code
     , - -课程产品编号
  t1.standard_name           as product_course_standard_name
     , - -课程产品(内)
  t1.category_code           as product_course_category_code
     , - -课程产品类别编码
  t1.period                  as product_course_period
     , - -课程产品期间
  t3.f_dept_code             as f_dept_code
     , - -新标准部门编码
  t3.f_dept_name             as f_dept_name
     , - -新标准部门名称
  t4.dept_code               as dept_code
     , - -部门编码
  t4.dept_fcode              as dept_fcode
     , - -部门财务编码
  t4.dept_name               as dept_name
     , - -部门名称
  t4.project_fcode           as project_fcode
     , - -项目财务编码
  t4.project_code            as project_code
     , - -项目编码
  t4.project_name            as project_name
     , - -项目名称
  t2.product_system_code     as product_system_id
     , - -产品体系ID
  t2.product_system_name     as product_system_name
     , - -产品体系名称
  t2.product_level_id        as product_level_id
     , - -产品品类ID
  t2.product_level_code      as product_level_code
     , - -产品品类编码
  t2.product_level_name      as product_level_name
     , - -产品品类名称
  t5.learn_stage_code        as learn_stage_code
     , - -学习阶段编码
  t5.learn_stage_name        as learn_stage_name
     , - -学习阶段名称
  t5.product_brand_code      as product_brand_code
     , - -产品品牌编码
  t5.product_brand_name      as product_brand_name
     , - -产品品牌名称
  t5.study_abroad_stage_code as study_abroad_stage_code
     , - -留学阶段编码
  t5.study_abroad_stage_name as study_abroad_stage_name
     , - -留学阶段名称
  t5.current_level_name      as current_level_name
     , - -学生当前水平名称
  t5.exam_target_name        as exam_target_name
     , - -学生考试目标名称
  CURRENT_TIMESTAMP as etl_time --运行时间

from (select school_id,
  product_code,
  project_code,
  standard_name, --product_course_standard_name更改来的
  category_code, --product_course_category_code更改来的
  period, --product_course_period更改来的
  is_deleted
  from ods.ods_dim_api_product_schoolcourse_da
  where dt = '${date}'
  and is_deleted = 0
  ) t1
  left join
  (select pcsld.school_id as school_id,
  pcsld.product_code as product_code,
  pcsld.product_system_code as product_system_code,
  cd.product_system_name as product_system_name,
  pcsld.product_level_code as product_level_code,
  pld.product_level_name as product_level_name,
  pld.id as product_level_id
  from (select school_id,
  product_code,
  product_system_code,
  product_level_code
  from ods.ods_dim_api_product_course_system_level_da
  where dt =
             '${date}') pcsld
  left join (select id,
  product_level_code,
  product_level_name
  from ods.ods_dim_api_product_level_da
  where dt =
             '${date}') pld
  on pcsld.product_level_code = pld.product_level_code
  left join (select product_system_code,
  product_system_name
  from ods.ods_dim_api_d_classsystem_da
  where dt =
             '${date}') cd
  on cd.product_system_code = pcsld.product_system_code
  ) t2
on t1.school_id = t2.school_id and t1.product_code = t2.product_code
  left join
  (select odapsd.school_id as school_id,
  odapsd.product_code as product_code,
  odaddd.f_dept_code as f_dept_code,
  odaddd.f_dept_name as f_dept_name
  from (select f_dept_code,
  f_dept_name
  from ods.ods_dim_api_d_deptstd_da
  where dt = '${date}') odaddd
  left join (select school_id,
  product_code,
  dept_code
  from ods.ods_dim_api_product_schoolcourse_da
  where dt = '${date}') odapsd
  on odapsd.dept_code = odaddd.f_dept_code
  ) t3 on t1.school_id = t3.school_id and t1.product_code = t3.product_code
  left join
  (select odaspd.school_id as school_id,
  odabpd.project_code as project_code,
  odaspd.dept_code as dept_code,
  odaspd.fin_dept_code as dept_fcode,
  odaspd.dept_name as dept_name,
  odabpd.fin_project_code as project_fcode,
  odabpd.project_name as project_name
  from (select school_id,
  dept_code,
  fin_dept_code,
  dept_name
  from ods.ods_dim_api_s_dept_da
  where dt = '${date}') odaspd
  left join
  (select school_id,
  project_code,
  fin_project_code,
  project_name,
  dept_code -- 由project_dept_code改变字段来的
  from ods.ods_dim_api_bs_project_da
  where dt = '${date}') odabpd
  on odabpd.school_id = odaspd.school_id and odabpd.dept_code = odaspd.dept_code
  ) t4 on t1.school_id = t4.school_id and t1.project_code = t4.project_fcode
  left join
  (select aa.school_id,
  aa.product_code,
  regexp_replace(concat_ws(',', sort_array(collect_list(
  if (aa.property_code = '51', concat_ws(':', cast(aa.rank as string), aa.property_value_code),
  null)))), '\\\\d\\:', '') as learn_stage_code, -- 学习阶段
  regexp_replace(concat_ws(',', sort_array(collect_list(
  if (aa.property_code = '51', concat_ws(':', cast(aa.rank as string), aa.property_value_name),
  null)))), '\\\\d\\:', '') as learn_stage_name, -- 学习阶段
  regexp_replace(concat_ws(',', sort_array(collect_list(
  if (aa.property_code = '100', concat_ws(':', cast(aa.rank as string), aa.property_value_code),
  null)))), '\\\\d\\:', '') as product_brand_code, -- 产品品牌
  regexp_replace(concat_ws(',', sort_array(collect_list(
  if (aa.property_code = '100', concat_ws(':', cast(aa.rank as string), aa.property_value_name),
  null)))), '\\\\d\\:', '') as product_brand_name, -- 产品品牌
  regexp_replace(concat_ws(',', sort_array(collect_list(
  if (aa.property_code = '101', concat_ws(':', cast(aa.rank as string), aa.property_value_code),
  null)))), '\\\\d\\:', '') as study_abroad_stage_code, --  留学阶段
  regexp_replace(concat_ws(',', sort_array(collect_list(
  if (aa.property_code = '101', concat_ws(':', cast(aa.rank as string), aa.property_value_name),
  null)))), '\\\\d\\:', '') as study_abroad_stage_name, --  留学阶段
  regexp_replace(concat_ws(',', sort_array(collect_list(
  if (aa.property_code = '102', concat_ws(':', cast(aa.rank as string), aa.input_value),
  null)))), '\\\\d\\:', '') as current_level_name, --  学生当前水平
  regexp_replace(concat_ws(',', sort_array(collect_list(
  if (aa.property_code = '103', concat_ws(':', cast(aa.rank as string), aa.input_value),
  null)))), '\\\\d\\:', '') as exam_target_name --  学生考试目标
  from (select row_number ()
  over (partition by psc.school_id, psc.product_code order by pcp.property_code,pcp.property_value_code desc) AS rank,
  psc.school_id as school_id,
  psc.product_code as product_code,
  pcp.property_code as property_code,
  pcp.property_value_code,
  pbpv.property_value_name,
  pcp.input_value
  from (select school_id,product_code,is_deleted from ods.ods_dim_api_product_schoolcourse_da where dt='${date}') psc
  left join (select school_id,product_code,property_code,property_value_code,input_value from ods.ods_dim_api_product_courseproperty_da where dt='${date}') pcp
  on pcp.school_id = psc.school_id and pcp.product_code = psc.product_code
  left join (select property_code,property_value_code,property_value_name from ods.ods_dim_api_product_basepropertyvalue_da where dt='${date}') pbpv
  on pbpv.property_code = pcp.property_code and
  pbpv.property_value_code = pcp.property_value_code
  where pcp.property_code in ('51', '100', '101', '102', '103')
  and psc.is_deleted = 0
  ) aa
  group by aa.school_id, aa.product_code
  ) t5 on t5.school_id = t1.school_id and t5.product_code = t1.product_code;