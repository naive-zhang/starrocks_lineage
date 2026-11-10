-- DolphinScheduler 项目 8655547341888 的 30 条 INSERT
-- 任务实例页 http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/task/instances
-- 来源为 30 个成功完成的 SQL 类型任务实例，目标表各不相同。
-- SQL 取自执行结果日志。JDBC 占位符使用同次执行的绑定值还原。
-- 日志行号按下载的任务日志从第 1 行起算。SQL 缺少末尾分号时，另起一行补上分隔符。
-- 去重时忽略普通注释、空白、关键字大小写和字面量取值，保留表、字段和查询结构。

-- 索引（序号 | 目标表 | 任务实例 ID）
-- 01 | lbdb.dws_busi_inbill_site_wgt_rt | 4655690
-- 02 | lbdb.tmp_busi_inbill_site_wgt_rt_1 | 4655689
-- 03 | lbdb.tmp_busi_inbill_site_wgt_rt_2 | 4655687
-- 04 | dws_oper_org_violation_i_h | 4655681
-- 05 | dws_oper_operator_violation_i_h | 4655679
-- 06 | dws_oper_first_site_sign_distance_rt | 4655678
-- 07 | dws_oper_sign_first_site_distance_rt | 4655677
-- 08 | dwd_oper_sign_timeout_detail_rt | 4655664
-- 09 | dws_oper_center_retain_1d | 4655621
-- 10 | dws_busi_org_input_cost_i_d | 4655612
-- 11 | lbdb.ads_surch_policy_sign_site_review_1d | 4655611
-- 12 | dws_busi_org_shuttle_bus_input_i_d | 4655610
-- 13 | ads_compass_timepoint_progress_monitor | 4655603
-- 14 | dws_opt_site_send_transit_fee_i_d | 4655600
-- 15 | dws_oper_center_resign_cnt_i_d | 4655596
-- 16 | lbdb.dws_oper_waybill_retain_1d | 4655595
-- 17 | lbdb.dwd_fin_wayb_sign_site_income_cost_i_m | 4655587
-- 18 | dws_oper_org_send_arrival_diff_bf9_1d | 4655586
-- 19 | dwd_oper_send_arrival_diff_af9_i_d | 4655585
-- 20 | dwd_oper_region_position_status_i_d | 4655582
-- 21 | dwd_busi_waybill_today_due_detail_i_d | 4655221
-- 22 | dim_gis_tb_base_time_efficient | 4655191
-- 23 | dim_gis_tb_base_site_info_d | 4655187
-- 24 | lbdb.dws_oper_eff_delivery_rate_v | 4655168
-- 25 | lbdb.dws_oper_eff_delivery_rate_delivery | 4655160
-- 26 | lbdb.dws_oper_eff_delivery_rate_dest_site | 4655155
-- 27 | lbdb.dws_oper_eff_delivery_rate | 4655150
-- 28 | lbdb.dwd_oper_eff_delivery_rate_should_sign | 4655147
-- 29 | lbdb.dwd_oper_eff_delivery_rate_waybill_change | 4655145
-- 30 | lbdb.dwd_oper_eff_delivery_rate_next_site | 4655143

-- 以下为 SQL 正文。

-- [01] lbdb.dws_busi_inbill_site_wgt_rt
-- 任务 insert_dws_busi_inbill_site_wgt_rt | 任务实例 ID 4655690 | 任务定义编码 18550995426761
-- 工作流实例 货量播报刷数_PROD-5-20261110090510364 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682486
-- 执行结果 2026-11-10 09:05:49.599 +0800 | 日志第 192 行 | JDBC 返回行数 3699
insert into lbdb.dws_busi_inbill_site_wgt_rt(stats_date_bgn_tm
	, stats_date_end_tm
	, stats_site_lvl
	, stats_site_code
	, weight_type
	, weight_group
	, class_area_code
	, stats_date
	, stats_site_name
	, stats_hour
	, stats_min
	, calc_weight_ccd
	, target_val
	, workday_cnt_ccm
	, total_workday_cnt_cur_mon
	, total_ticket_ccd
	, parent_site_code)
select stats_date_bgn_tm
	, '2026-11-10 09:05:00' as stats_date_end_tm
	, stats_site_lvl
	, stats_site_code
	, weight_type
	, weight_group
	, class_area_code
	, stats_date
	, stats_site_name
	, stats_hour
	, stats_min
	, calc_weight_ccd
	, target_val
	, workday_cnt_ccm
	, total_workday_cnt_cur_mon
	, total_ticket_ccd
	, parent_site_code
from lbdb.tmp_busi_inbill_site_wgt_rt_1;

-- [02] lbdb.tmp_busi_inbill_site_wgt_rt_1
-- 任务 insert_tmp_busi_inbill_site_wgt_rt_1 | 任务实例 ID 4655689 | 任务定义编码 18550995426760
-- 工作流实例 货量播报刷数_PROD-5-20261110090510364 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682486
-- 执行结果 2026-11-10 09:05:46.059 +0800 | 日志第 293 行 | JDBC 返回行数 3699
insert into lbdb.tmp_busi_inbill_site_wgt_rt_1( stats_date_bgn_tm
                                         , stats_date_end_tm
                                         , stats_site_lvl
                                         , stats_site_code
                                         , weight_type
                                         , weight_group
                                         , class_area_code
                                         , stats_date
                                         , stats_site_name
                                         , stats_hour
                                         , stats_min
                                         , calc_weight_ccd
                                         , target_val
                                         , workday_cnt_ccm
                                         , total_workday_cnt_cur_mon
                                         , total_ticket_ccd
                                         , parent_site_code)
with acc_tbl as (select stats_date_bgn_tm
                      , stats_date_end_tm
                      , stats_site_lvl
                      , stats_site_code
                      , weight_type
                      , weight_group
                      , class_area_code
                      , stats_date
                      , stats_site_name
                      , stats_hour
                      , stats_min
                      , calc_weight_ccd
                      , target_val
                      , workday_cnt_ccm
                      , total_workday_cnt_cur_mon
                      , total_ticket_ccd
                      , parent_site_code
                 from lbdb.tmp_busi_inbill_site_wgt_rt_2)
select stats_date_bgn_tm
     , stats_date_end_tm
     , stats_site_lvl
     , stats_site_code
     , weight_type
     , weight_group
     , class_area_code
     , stats_date
     , stats_site_name
     , stats_hour
     , stats_min
     , sum(calc_weight_ccd)  as calc_weight_ccd
     , target_val
     , workday_cnt_ccm
     , total_workday_cnt_cur_mon
     , sum(total_ticket_ccd) as total_ticket_ccd
     , parent_site_code
from acc_tbl
group by stats_date_bgn_tm
       , stats_date_end_tm
       , stats_site_lvl
       , stats_site_code
       , weight_type
       , weight_group
       , class_area_code
       , stats_date
       , stats_site_name
       , stats_hour
       , stats_min
       , target_val
       , workday_cnt_ccm
       , total_workday_cnt_cur_mon
       , parent_site_code
;

-- [03] lbdb.tmp_busi_inbill_site_wgt_rt_2
-- 任务 insert_tmp_busi_inbill_site_wgt_rt_2 | 任务实例 ID 4655687 | 任务定义编码 18550995426758
-- 工作流实例 货量播报刷数_PROD-5-20261110090510364 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682486
-- 执行结果 2026-11-10 09:05:36.635 +0800 | 日志第 2177 行 | JDBC 返回行数 3716
insert into lbdb.tmp_busi_inbill_site_wgt_rt_2( stats_date_bgn_tm
                                         , stats_date_end_tm
                                         , stats_site_lvl
                                         , stats_site_code
                                         , weight_type
                                         , weight_group
                                         , class_area_code
                                         , stats_date
                                         , stats_site_name
                                         , stats_hour
                                         , stats_min
                                         , calc_weight_ccd
                                         , target_val
                                         , workday_cnt_ccm
                                         , total_workday_cnt_cur_mon
                                         , total_ticket_ccd
                                         , parent_site_code)
with site_code2name_tbl as (select site_code
                                 , site_name
                            from tx_bd_site_info),
-- 非网点的当前和上层映射表
     site2parent_tbl as (select svth_lvl_org_brnch_cd                   as site_code
                              , concat('900600', sxth_lvl_org_brnch_cd) as parent_site_code
                              , sxth_lvl_org_brnch_nm                   as parent_site_name
                         from dm_pb_bizop_org_brnch
                         where org_brnch_lvl = 7
                         union all
                         select concat('900600', sxth_lvl_org_brnch_cd) as site_code
                              , fith_lvl_org_brnch_cd                   as parent_site_code
                              , fith_lvl_org_brnch_nm                   as parent_site_name
                         from dm_pb_bizop_org_brnch
                         where org_brnch_lvl = 6
                         union all
                         select fith_lvl_org_brnch_cd                   as site_code
                              , concat('900800', thrd_lvl_org_brnch_cd) as parent_site_code
                              , thrd_lvl_org_brnch_nm                   as parent_site_name
                         from dm_pb_bizop_org_brnch
                         where org_brnch_lvl = 5
                         union all
                         select concat('900800', thrd_lvl_org_brnch_cd) as site_code
                              , fst_lvl_org_brnch_cd                    as parent_site_code
                              , fst_lvl_org_brnch_nm                    as parent_site_name
                         from dm_pb_bizop_org_brnch
                         where org_brnch_lvl = 3),
-- 临时子查询, 用于生成数据
     tmp_basic_first_site_agg_tbl as (select stats_date_bgn_tm
                                           , stats_date_end_tm
                                           , l.parent_site_code as stats_site_code
                                           , weight_type
                                           , weight_group
                                           , class_area_code
                                           , stats_date
                                           , r2.site_name       as stats_site_name
                                           , stats_hour
                                           , stats_min
                                           , calc_weight_ccd
                                           , target_val
                                           , workday_cnt_ccm
                                           , total_workday_cnt_cur_mon
                                           , total_ticket_ccd
                                           , r.parent_site_code as parent_site_code
                                      from lbdb.tmp_busi_inbill_site_wgt_rt_1 l
                                               left join site2parent_tbl r
                                                         on l.parent_site_code = r.site_code
                                               left join site_code2name_tbl r2
                                                         on l.parent_site_code = r2.site_code),
-- 一级网点汇总
     basic_first_site_agg_tbl as (select stats_date_bgn_tm
                                       , stats_date_end_tm
                                       , 7                     as stats_site_lvl
                                       , stats_site_code
                                       , weight_type
                                       , weight_group
                                       , class_area_code
                                       , stats_date
                                       , stats_site_name
                                       , stats_hour
                                       , stats_min
                                       , sum(calc_weight_ccd)  as calc_weight_ccd
                                       , target_val
                                       , workday_cnt_ccm
                                       , total_workday_cnt_cur_mon
                                       , sum(total_ticket_ccd) as total_ticket_ccd
                                       , parent_site_code
                                  from tmp_basic_first_site_agg_tbl
                                  group by stats_date_bgn_tm
                                         , stats_date_end_tm
                                         , stats_site_code
                                         , weight_type
                                         , weight_group
                                         , class_area_code
                                         , stats_date
                                         , stats_site_name
                                         , stats_hour
                                         , stats_min
                                         , target_val
                                         , workday_cnt_ccm
                                         , total_workday_cnt_cur_mon
                                         , parent_site_code),
-- 营销大区临时子查询, 用于生成数据
     tmp_basic_big_area_agg_tbl as (select stats_date_bgn_tm
                                         , stats_date_end_tm
                                         , l.parent_site_code as stats_site_code
                                         , weight_type
                                         , weight_group
                                         , class_area_code
                                         , stats_date
                                         , r2.site_name       as stats_site_name
                                         , stats_hour
                                         , stats_min
                                         , calc_weight_ccd
                                         , target_val
                                         , workday_cnt_ccm
                                         , total_workday_cnt_cur_mon
                                         , total_ticket_ccd
                                         , r.parent_site_code as parent_site_code
                                    from basic_first_site_agg_tbl l
                                             left join site2parent_tbl r
                                                       on l.parent_site_code = r.site_code
                                             left join site_code2name_tbl r2
                                                       on l.parent_site_code = r2.site_code),
-- 营销大区汇总
     basic_big_area_agg_tbl as (select stats_date_bgn_tm
                                     , stats_date_end_tm
                                     , 6                     as stats_site_lvl
                                     , stats_site_code
                                     , weight_type
                                     , weight_group
                                     , class_area_code
                                     , stats_date
                                     , stats_site_name
                                     , stats_hour
                                     , stats_min
                                     , sum(calc_weight_ccd)  as calc_weight_ccd
                                     , target_val
                                     , workday_cnt_ccm
                                     , total_workday_cnt_cur_mon
                                     , sum(total_ticket_ccd) as total_ticket_ccd
                                     , parent_site_code
                                from tmp_basic_big_area_agg_tbl
                                group by stats_date_bgn_tm
                                       , stats_date_end_tm
                                       , stats_site_code
                                       , weight_type
                                       , weight_group
                                       , class_area_code
                                       , stats_date
                                       , stats_site_name
                                       , stats_hour
                                       , stats_min
                                       , target_val
                                       , workday_cnt_ccm
                                       , total_workday_cnt_cur_mon
                                       , parent_site_code),
-- 分拨临时子查询, 用于生成数据
     tmp_basic_center_agg_tbl as (select stats_date_bgn_tm
                                       , stats_date_end_tm
                                       , l.parent_site_code as stats_site_code
                                       , weight_type
                                       , weight_group
                                       , class_area_code
                                       , stats_date
                                       , r2.site_name       as stats_site_name
                                       , stats_hour
                                       , stats_min
                                       , calc_weight_ccd
                                       , target_val
                                       , workday_cnt_ccm
                                       , total_workday_cnt_cur_mon
                                       , total_ticket_ccd
                                       , r.parent_site_code as parent_site_code
                                  from basic_big_area_agg_tbl l
                                           left join site2parent_tbl r
                                                     on l.parent_site_code = r.site_code
                                           left join site_code2name_tbl r2
                                                     on l.parent_site_code = r2.site_code),
-- 分拨汇总
     basic_center_agg_tbl as (select stats_date_bgn_tm
                                   , stats_date_end_tm
                                   , 5                     as stats_site_lvl
                                   , stats_site_code
                                   , weight_type
                                   , weight_group
                                   , class_area_code
                                   , stats_date
                                   , stats_site_name
                                   , stats_hour
                                   , stats_min
                                   , sum(calc_weight_ccd)  as calc_weight_ccd
                                   , target_val
                                   , workday_cnt_ccm
                                   , total_workday_cnt_cur_mon
                                   , sum(total_ticket_ccd) as total_ticket_ccd
                                   , parent_site_code
                              from tmp_basic_center_agg_tbl
                              group by stats_date_bgn_tm
                                     , stats_date_end_tm
                                     , stats_site_code
                                     , weight_type
                                     , weight_group
                                     , class_area_code
                                     , stats_date
                                     , stats_site_name
                                     , stats_hour
                                     , stats_min
                                     , target_val
                                     , workday_cnt_ccm
                                     , total_workday_cnt_cur_mon
                                     , parent_site_code),
-- 营运区临时子查询, 用于生成数据
     tmp_basic_province_area_agg_tbl as (select stats_date_bgn_tm
                                              , stats_date_end_tm
                                              , l.parent_site_code as stats_site_code
                                              , weight_type
                                              , weight_group
                                              , class_area_code
                                              , stats_date
                                              , r2.site_name       as stats_site_name
                                              , stats_hour
                                              , stats_min
                                              , calc_weight_ccd
                                              , target_val
                                              , workday_cnt_ccm
                                              , total_workday_cnt_cur_mon
                                              , total_ticket_ccd
                                              , r.parent_site_code as parent_site_code
                                         from basic_center_agg_tbl l
                                                  left join site2parent_tbl r
                                                            on l.parent_site_code = r.site_code
                                                  left join site_code2name_tbl r2
                                                            on l.parent_site_code = r2.site_code),
-- 营运区汇总
     basic_province_area_agg_tbl as (select stats_date_bgn_tm
                                          , stats_date_end_tm
                                          , 3                     as stats_site_lvl
                                          , stats_site_code
                                          , weight_type
                                          , weight_group
                                          , class_area_code
                                          , stats_date
                                          , stats_site_name
                                          , stats_hour
                                          , stats_min
                                          , sum(calc_weight_ccd)  as calc_weight_ccd
                                          , target_val
                                          , workday_cnt_ccm
                                          , total_workday_cnt_cur_mon
                                          , sum(total_ticket_ccd) as total_ticket_ccd
                                          , parent_site_code
                                     from tmp_basic_province_area_agg_tbl
                                     group by stats_date_bgn_tm
                                            , stats_date_end_tm
                                            , stats_site_code
                                            , weight_type
                                            , weight_group
                                            , class_area_code
                                            , stats_date
                                            , stats_site_name
                                            , stats_hour
                                            , stats_min
                                            , target_val
                                            , workday_cnt_ccm
                                            , total_workday_cnt_cur_mon
                                            , parent_site_code),
-- 总部临时子查询, 用于生成数据
     tmp_basic_ane_agg_tbl as (select stats_date_bgn_tm
                                    , stats_date_end_tm
                                    , l.parent_site_code as stats_site_code
                                    , weight_type
                                    , weight_group
                                    , class_area_code
                                    , stats_date
                                    , r2.site_name       as stats_site_name
                                    , stats_hour
                                    , stats_min
                                    , calc_weight_ccd
                                    , target_val
                                    , workday_cnt_ccm
                                    , total_workday_cnt_cur_mon
                                    , total_ticket_ccd
                                    , r.parent_site_code as parent_site_code
                               from basic_province_area_agg_tbl l
                                        left join site2parent_tbl r
                                                  on l.parent_site_code = r.site_code
                                        left join site_code2name_tbl r2
                                                  on l.parent_site_code = r2.site_code),
-- 总部汇总
     basic_ane_agg_tbl as (select stats_date_bgn_tm
                                , stats_date_end_tm
                                , 1                     as stats_site_lvl
                                , stats_site_code
                                , weight_type
                                , weight_group
                                , class_area_code
                                , stats_date
                                , stats_site_name
                                , stats_hour
                                , stats_min
                                , sum(calc_weight_ccd)  as calc_weight_ccd
                                , target_val
                                , workday_cnt_ccm
                                , total_workday_cnt_cur_mon
                                , sum(total_ticket_ccd) as total_ticket_ccd
                                , parent_site_code
                           from tmp_basic_ane_agg_tbl
                           group by stats_date_bgn_tm
                                  , stats_date_end_tm
                                  , stats_site_code
                                  , weight_type
                                  , weight_group
                                  , class_area_code
                                  , stats_date
                                  , stats_site_name
                                  , stats_hour
                                  , stats_min
                                  , target_val
                                  , workday_cnt_ccm
                                  , total_workday_cnt_cur_mon
                                  , parent_site_code),
-- 汇总的基础数据
     overall_basic_tbl as (select stats_date_bgn_tm
                                , stats_date_end_tm
                                , stats_site_lvl
                                , stats_site_code
                                , weight_type
                                , weight_group
                                , class_area_code
                                , stats_date
                                , stats_site_name
                                , stats_hour
                                , stats_min
                                , calc_weight_ccd
                                , target_val
                                , workday_cnt_ccm
                                , total_workday_cnt_cur_mon
                                , total_ticket_ccd
                                , parent_site_code
                           from basic_ane_agg_tbl
                           union all
                           select stats_date_bgn_tm
                                , stats_date_end_tm
                                , stats_site_lvl
                                , stats_site_code
                                , weight_type
                                , weight_group
                                , class_area_code
                                , stats_date
                                , stats_site_name
                                , stats_hour
                                , stats_min
                                , calc_weight_ccd
                                , target_val
                                , workday_cnt_ccm
                                , total_workday_cnt_cur_mon
                                , total_ticket_ccd
                                , parent_site_code
                           from basic_province_area_agg_tbl
                           union all
                           select stats_date_bgn_tm
                                , stats_date_end_tm
                                , stats_site_lvl
                                , stats_site_code
                                , weight_type
                                , weight_group
                                , class_area_code
                                , stats_date
                                , stats_site_name
                                , stats_hour
                                , stats_min
                                , calc_weight_ccd
                                , target_val
                                , workday_cnt_ccm
                                , total_workday_cnt_cur_mon
                                , total_ticket_ccd
                                , parent_site_code
                           from basic_center_agg_tbl
                           union all
                           select stats_date_bgn_tm
                                , stats_date_end_tm
                                , stats_site_lvl
                                , stats_site_code
                                , weight_type
                                , weight_group
                                , class_area_code
                                , stats_date
                                , stats_site_name
                                , stats_hour
                                , stats_min
                                , calc_weight_ccd
                                , target_val
                                , workday_cnt_ccm
                                , total_workday_cnt_cur_mon
                                , total_ticket_ccd
                                , parent_site_code
                           from basic_big_area_agg_tbl
                           union all
                           select stats_date_bgn_tm
                                , stats_date_end_tm
                                , stats_site_lvl
                                , stats_site_code
                                , weight_type
                                , weight_group
                                , class_area_code
                                , stats_date
                                , stats_site_name
                                , stats_hour
                                , stats_min
                                , calc_weight_ccd
                                , target_val
                                , workday_cnt_ccm
                                , total_workday_cnt_cur_mon
                                , total_ticket_ccd
                                , parent_site_code
                           from basic_first_site_agg_tbl
                           union all
                           select stats_date_bgn_tm
                                , stats_date_end_tm
                                , stats_site_lvl
                                , stats_site_code
                                , weight_type
                                , weight_group
                                , class_area_code
                                , stats_date
                                , stats_site_name
                                , stats_hour
                                , stats_min
                                , calc_weight_ccd
                                , target_val
                                , workday_cnt_ccm
                                , total_workday_cnt_cur_mon
                                , total_ticket_ccd
                                , parent_site_code
                           from lbdb.tmp_busi_inbill_site_wgt_rt_1
                           where parent_site_code <> stats_site_code
                             and stats_site_lvl = 8),
     final_tbl as (select stats_date_bgn_tm
                        , stats_date_end_tm
                        , stats_site_lvl
                        , stats_site_code
                        , weight_type
                        , weight_group
                        , class_area_code
                        , stats_date
                        , stats_site_name
                        , stats_hour
                        , stats_min
                        , calc_weight_ccd
                        , target_val
                        , workday_cnt_ccm
                        , total_workday_cnt_cur_mon
                        , total_ticket_ccd
                        , parent_site_code
                   from overall_basic_tbl
                   union all
-- 全网
                   select stats_date_bgn_tm
                        , stats_date_end_tm
                        , stats_site_lvl
                        , stats_site_code
                        , 0                     as weight_type
                        , weight_group
                        , class_area_code
                        , stats_date
                        , stats_site_name
                        , stats_hour
                        , stats_min
                        , sum(calc_weight_ccd)  as calc_weight_ccd
                        , target_val
                        , workday_cnt_ccm
                        , total_workday_cnt_cur_mon
                        , sum(total_ticket_ccd) as total_ticket_ccd
                        , parent_site_code
                   from overall_basic_tbl
                   group by stats_date_bgn_tm
                          , stats_date_end_tm
                          , stats_site_lvl
                          , stats_site_code
                          , weight_group
                          , class_area_code
                          , stats_date
                          , stats_site_name
                          , stats_hour
                          , stats_min
                          , target_val
                          , workday_cnt_ccm
                          , total_workday_cnt_cur_mon
                          , parent_site_code
                   union all
-- 大客户汇总
                   select stats_date_bgn_tm
                        , stats_date_end_tm
                        , stats_site_lvl
                        , stats_site_code
                        , 500                   as weight_type
                        , weight_group
                        , class_area_code
                        , stats_date
                        , stats_site_name
                        , stats_hour
                        , stats_min
                        , sum(calc_weight_ccd)  as calc_weight_ccd
                        , target_val
                        , workday_cnt_ccm
                        , total_workday_cnt_cur_mon
                        , sum(total_ticket_ccd) as total_ticket_ccd
                        , parent_site_code
                   from overall_basic_tbl
                   where weight_type <> 200
                   group by stats_date_bgn_tm
                          , stats_date_end_tm
                          , stats_site_lvl
                          , stats_site_code
                          , weight_group
                          , class_area_code
                          , stats_date
                          , stats_site_name
                          , stats_hour
                          , stats_min
                          , target_val
                          , workday_cnt_ccm
                          , total_workday_cnt_cur_mon
                          , parent_site_code)
select stats_date_bgn_tm
     , stats_date_end_tm
     , stats_site_lvl
     , stats_site_code
     , weight_type
     , weight_group
     , class_area_code
     , stats_date
     , stats_site_name
     , stats_hour
     , stats_min
     , calc_weight_ccd
     , target_val
     , workday_cnt_ccm
     , total_workday_cnt_cur_mon
     , total_ticket_ccd
     , parent_site_code
from final_tbl;

-- [04] dws_oper_org_violation_i_h
-- 任务 dws_oper_org_violation_i_h | 任务实例 ID 4655681 | 任务定义编码 21404444982627
-- 工作流实例 罗盘-违规监控迭代-6-20261110090516382 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682488
-- 执行结果 2026-11-10 09:05:20.735 +0800 | 日志第 2249 行 | JDBC 返回行数 438
insert into dws_oper_org_violation_i_h
WITH temp1 as 
(
	SELECT
		site_code,
	CASE
		WHEN alert_type = 1 THEN 10
		-- 抛扔
		WHEN alert_type = 5 THEN 20
		-- 拖拽
		WHEN alert_type = 7 THEN 30
		-- 叉车撞击
		ELSE 0
	END AS alert_type,
			CASE
		WHEN alert_type IN (1, 5, 7) THEN alert_type_name
		ELSE '其他'
	END AS alert_type_name,
	count(id) as violation_count
	FROM
		ods_tx_tx_oper_alert_message
	WHERE
		alert_time >= DATE_FORMAT('2026-11-09', '%Y-%m-%d 12:00:00')
		AND alert_time < DATE_FORMAT(DATE_ADD('2026-11-09' ,INTERVAL 1 DAY), '%Y-%m-%d 12:00:00')
		AND data_valid = 10
		AND is_cdc_delete = 0
		group by site_code,
		CASE
		WHEN alert_type IN (1, 5, 7) THEN alert_type_name
		ELSE '其他'
	END ,
	CASE
		WHEN alert_type = 1 THEN 10
		-- 抛扔
		WHEN alert_type = 5 THEN 20
		-- 拖拽
		WHEN alert_type = 7 THEN 30
		-- 叉车撞击
		ELSE 0
	END 
	
	UNION ALL
	
	SELECT
		site_code,
		99 AS alert_type,
		'合计' AS alert_type_name,
		count(id) as violation_count
	FROM
		ods_tx_tx_oper_alert_message
	WHERE
		alert_time >= DATE_FORMAT('2026-11-09', '%Y-%m-%d 12:00:00')
		AND alert_time < DATE_FORMAT(DATE_ADD('2026-11-09' ,INTERVAL 1 DAY), '%Y-%m-%d 12:00:00')
		AND data_valid = 10
		AND is_cdc_delete = 0
		group by site_code
		
  ),
--计算当月违规次数
temp2 as (
select 
org_brnch_code,
alert_type,
alert_type_name,
SUM(violation_count) as violation_count,
SUM(violation_count_ccm)+SUM(violation_count) as violation_count_ccm
from 
(select 
site_code as org_brnch_code,
alert_type,
alert_type_name,
violation_count,
0 as violation_count_ccm
from temp1
UNION ALL
select 
org_brnch_code,
alert_type,
alert_type_name,
0 as violation_count,
SUM(violation_count) AS violation_count_ccm
from 
dws_oper_org_violation_i_h
where stats_date>=DATE_FORMAT('2026-11-09', '%Y-%m-01 00:00:00')
and stats_date<'2026-11-09'
and stats_hour=24
and org_brnch_lvl=5
--and alert_type is not NULL
group by 
org_brnch_code,
alert_type,
alert_type_name
)tt
group by org_brnch_code,
alert_type,
alert_type_name
)
--关联组织架构
,temp3 as (
SELECT
t1.org_brnch_code as center_code,
t2.site_name as center_name,
t2.province_area_code,
t2.province_area_name,
999999 as ane_code,
'安能物流' as ane_name,
t1.alert_type,
t1.alert_type_name,
t1.violation_count,
t1.violation_count_ccm
FROM  temp2 t1
left join tx_bd_site_info t2
on t1.org_brnch_code=t2.site_code
)

select 
'2026-11-09' as stats_date,
(HOUR('2026-11-10 09:00:05') - 12 + 24) % 24 + 1 AS stats_hour,
org_brnch_code,
org_brnch_lvl,
alert_type,
org_brnch_name,
parent_site_code,
parent_site_name,
alert_type_name,
violation_count,
violation_count_ccm,
CURRENT_TIMESTAMP as etl_time
FROM
(
--分拨违规
select 
center_code as org_brnch_code,
center_name as org_brnch_name,
province_area_code as parent_site_code,
province_area_name as parent_site_name,
5 as org_brnch_lvl,
alert_type,
alert_type_name,
violation_count,
violation_count_ccm
from temp3

UNION ALL

--营运区违规
select 
province_area_code as org_brnch_code,
province_area_name as org_brnch_name,
ane_code as parent_site_code,
ane_name as parent_site_name,
3 as org_brnch_lvl,
alert_type,
alert_type_name,
sum(violation_count) as violation_count,
sum(violation_count_ccm) as violation_count_ccm
from temp3
group by 
province_area_code ,
province_area_name ,
ane_code ,
ane_name ,
alert_type,
alert_type_name

UNION ALL

--总部违规
select 
ane_code as org_brnch_code,
ane_name as org_brnch_name,
NULL as parent_site_code,
NULL as parent_site_name,
1 as org_brnch_lvl,
alert_type,
alert_type_name,
sum(violation_count) as violation_count,
sum(violation_count_ccm) as violation_count_ccm
from temp3
group by 
ane_code ,
ane_name ,
alert_type,
alert_type_name
)t
;

-- [05] dws_oper_operator_violation_i_h
-- 任务 dws_oper_operator_violation_i_h | 任务实例 ID 4655679 | 任务定义编码 21404444982625
-- 工作流实例 罗盘-违规监控迭代-6-20261110090516382 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682488
-- 执行结果 2026-11-10 09:05:20.738 +0800 | 日志第 2229 行 | JDBC 返回行数 38650
insert into dws_oper_operator_violation_i_h

WITH temp1 AS (
SELECT
	m.alert_no,
	m.id,
	o.operator_code,
	o.operator_name,
	m.site_code,
	t.site_name,
	t.province_area_code,
	t.province_area_name,
	CASE
		WHEN m.alert_type IN (1, 5, 7) THEN m.alert_type_name
		ELSE '其他'
	END AS mapped_alert_name,
	CASE
		WHEN m.alert_type = 1 THEN 10
		-- 抛扔
		WHEN m.alert_type = 5 THEN 20
		-- 拖拽
		WHEN m.alert_type = 7 THEN 30
		-- 叉车撞击
		ELSE 0
	END AS mapped_alert_type
FROM
	(
	SELECT
		alert_no,
		id,
		site_code,
		alert_type,
		alert_type_name
	FROM
		ods_tx_tx_oper_alert_message
	WHERE
		alert_time >= DATE_FORMAT('2026-11-09', '%Y-%m-%d 12:00:00')
		AND alert_time < DATE_FORMAT(DATE_ADD('2026-11-09' ,INTERVAL 1 DAY), '%Y-%m-%d 12:00:00')
		AND data_valid = 10
		AND is_cdc_delete = 0
  ) m
LEFT JOIN (
	SELECT
		message_id,
		operator_code,
		operator_name
	FROM
		ods_tx_tx_oper_alert_operator
	WHERE
		create_time >= DATE_FORMAT('2026-11-09', '%Y-%m-%d 00:00:00')
		AND create_time < DATE_FORMAT(DATE_ADD('2026-11-09' ,INTERVAL 2 DAY), '%Y-%m-%d 00:00:00')
		AND data_valid = 10
		AND is_cdc_delete = 0
  ) o ON
	m.id = o.message_id
LEFT JOIN tx_bd_site_info t ON
	m.site_code = t.site_code
)
-- 操作人违规
select 
stats_date,   --统计日期
(HOUR('2026-11-10 09:00:05') - 12 + 24) % 24 + 1 AS stats_hour,
operator_code,  --操作工code
max(operator_site_code) as operator_site_code, --操作工所属分拨code
alert_type,  --违规类型
operator_name,  --操作工名称
max(operator_site_name) as operator_site_name,  --操作工所属分拨名称
alert_type_name,  --违规类型名称
SUM(violation_count) AS violation_count,  --当日违规次数
SUM(violation_count_ccm)+SUM(violation_count) AS violation_count_ccm,  --当月违规次数
CURRENT_TIMESTAMP as etl_time
from 
(
select 
		'2026-11-09' as stats_date,
		operator_code,
		operator_name,
		operator_site_code,
		operator_site_name,
		alert_type,
		alert_type_name,
		0 AS violation_count,
		SUM(violation_count) AS violation_count_ccm
from 
		dws_oper_operator_violation_i_h 
where stats_date>=DATE_FORMAT('2026-11-09', '%Y-%m-01 00:00:00')
and stats_date<'2026-11-09'
and stats_hour=24
group by operator_code,
		operator_name,
		operator_site_code,
		operator_site_name,
		alert_type,
		alert_type_name
UNION ALL
SELECT 
stats_date,
operator_code,
operator_name,
operator_site_code,
operator_site_name,
alert_type,
alert_type_name,
violation_count,
violation_count_ccm
FROM 
	(SELECT
		'2026-11-09' as stats_date,
		operator_code,
		operator_name,
		site_code AS operator_site_code,
		site_name AS operator_site_name,
		mapped_alert_type AS alert_type,
		mapped_alert_name AS alert_type_name,
		COUNT(*) AS violation_count,
		0 AS violation_count_ccm
	FROM
		temp1
	WHERE
		operator_code IS NOT NULL
	GROUP BY
		operator_code,
		operator_name,
		site_code,
		site_name,
		mapped_alert_type,
		mapped_alert_name
UNION ALL
	SELECT
	'2026-11-09' as stats_date,
		operator_code ,
		operator_name ,
		site_code AS operator_site_code,
		site_name AS operator_site_name,
		99 AS alert_type,
		'合计' AS alert_type_name,
		COUNT(*) AS violation_count,
		0 AS violation_count_ccm
	FROM
		temp1
	WHERE
		operator_code IS NOT NULL
	GROUP BY
		operator_code,
		operator_name,
		site_code,
		site_name,
		mapped_alert_type,
		mapped_alert_name
	)t1

)t2
group by 
stats_date,
operator_code,
operator_name,
alert_type,
alert_type_name
;

-- [06] dws_oper_first_site_sign_distance_rt
-- 任务 dws_oper_first_site_sign_distance_rt | 任务实例 ID 4655678 | 任务定义编码 23571663912160
-- 工作流实例 罗盘-签收实时监控-19-20261110090512372 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682487
-- 执行结果 2026-11-10 09:05:19.021 +0800 | 日志第 6705 行 | JDBC 返回行数 8241
insert into dws_oper_first_site_sign_distance_rt
(
stats_time,
dest_first_site_code,
kilometer_range,
dest_first_site_name,
dest_area_code,
dest_area_name,
dest_center_code,
dest_center_name,
dest_province_area_code,
dest_province_area_name,
distance,
should_ticket_ccd, 
signed_ticket_ccd,
unsigned_ticket_ccd, 
should_ticket_ccm, 
signed_ticket_ccm,
sign_timeout_ticket, 
sign_timeout_ticket_3d 
)
WITH  site_info as (
select 
 t1.site_code
,t1.site_name
,t1.first_site_code
,t2.site_name as first_site_name
,t1.area_code
,t1.area_name
,t1.center_code
,t1.center_name
,t1.province_area_code
,t1.province_area_name
 from tx_bd_site_info t1
 left join tx_bd_site_info t2
 on t1.first_site_code=t2.site_code
)
--当日指标
,temp1 as (
select 
t1.waybill_no,
t1.validity_lasted_sign_time, --时效最晚签收时间
t1.sign_status,  --签收状态
t2.province_area_code as dest_province_area_code,  --目的营运区code
t2.province_area_name as dest_province_area_name,   --目的营运区名称
t1.waybill_dest_center_code,  --运单目的分拨code
-- t4.site_name as waybill_dest_center_name,  --运单目的分拨名称
t2.center_code as dest_center_code,  --目的所属分拨code
t2.center_name as dest_center_name,  --目的所属分拨名称
t2.area_code as dest_area_code,   --目的片区code
t2.area_name as dest_area_name, --目的片区名称
t2.first_site_code as dest_first_site_code,  --目的一级网点code
t2.first_site_name as dest_first_site_name,  --目的一级网点名称
t1.dest_site_code,  --目的网点code 
t2.site_name as dest_site_name , --目的网点名称
t1.send_site_code , --寄件网点
t1.should_sign_site_code , --应签网点
if( t1.validity_lasted_sign_time= '2026-11-10 23:59:59'  and t1.wait_notice_release_type  IN (30,0) and t3.dict_name  not like '%自提%' ,1,0)  as should_ticket_ccd,  --今日应签票数
if( t1.validity_lasted_sign_time= '2026-11-10 23:59:59' and t1.sign_status <> 10 and t1.wait_notice_release_type  IN (30,0) and t3.dict_name  not like '%自提%' ,1,0)  as signed_ticket_ccd,  --今日已签票数
if( t1.validity_lasted_sign_time= '2026-11-10 23:59:59' and t1.sign_status = 10 and t1.wait_notice_release_type IN (30,0) and t3.dict_name  not like '%自提%' ,1,0)  as unsigned_ticket_ccd  --今日未签票数
from 
(
select 
waybill_no,
validity_lasted_sign_time, --时效最晚签收时间
sign_status,  --签收状态
waybill_dest_center_code,  --运单目的分拨code
dest_site_code,  --目的网点code 
send_site_code , --寄件网点
should_sign_site_code , --应签网点
wait_notice_release_type,
delivery_type_code,
data_valid,
is_cdc_delete,
row_number() over(partition by waybill_no order by update_time desc) as rn
from ods_tx_tx_site_should_sign_detail t1
where  validity_lasted_sign_time >='2026-10-31 00:00:00'
and validity_lasted_sign_time < '2026-12-10 09:05:05'
)t1
left join site_info t2 ON t1.dest_site_code = t2.site_code
left join tx_basic_dict_entry t3 ON type_code = 'pick_up' and t3.dict_code = t1.delivery_type_code
where t1.rn = 1
and t1.data_valid = 10
and t1.is_cdc_delete = 0
and t1.validity_lasted_sign_time >= '2026-11-10 00:00:00'
AND t1.validity_lasted_sign_time <=  '2026-11-10 23:59:59' 
)
--距离表
,temp2 as (
select 
site_code
,center_code
,distance
 from 
(select 
site_code
,center_code
,distance
,ROW_NUMBER() over (PARTITION BY site_code,center_code order by create_time desc, distance) as rn 
from tx_site_delivery_distance 
)t-- 
where t.rn=1
)
--select * from temp2

--当日相关指标带距离
,temp3 as (
  select 
t2.distance,
t1.dest_province_area_code,  --目的营运区code
t1.dest_province_area_name,   --目的营运区名称
--t1.waybill_dest_center_code,  --运单目的分拨code
--t1.waybill_dest_center_name,  --运单目的分拨名称
t1.dest_center_code,  --目的所属分拨code
t1.dest_center_name,  --目的所属分拨名称
t1.dest_area_code,   --目的片区code
t1.dest_area_name, --目的片区名称
t1.dest_first_site_code,  --目的一级网点code
t1.dest_first_site_name,  --目的一级网点名称
sum(t1.should_ticket_ccd)   as should_ticket_ccd,  --今日应签票数
sum(t1.signed_ticket_ccd)   as signed_ticket_ccd,  --今日已签票数
sum(t1.unsigned_ticket_ccd) as unsigned_ticket_ccd   --今日未签票数
  from temp1 t1
    left join (select site_code,scan_site_code from 
(
select site_code,scan_site_code,ROW_NUMBER() over (PARTITION by site_code order by must_clock_time desc) as rn 
from dim_site_clock_maintain_s_d where dt='2026-11-09'
and data_valid =10 
and  clock_type=20
)t1
where rn=1)t4 on t1.dest_first_site_code=t4.site_code  
left join 
temp2 t2
on t1.dest_first_site_code=t2.site_code
and t4.scan_site_code=t2.center_code
group by 
t2.distance,
t1.dest_province_area_code,  --目的营运区code
t1.dest_province_area_name,   --目的营运区名称
-- t1.waybill_dest_center_code,  --运单目的分拨code
-- t1.waybill_dest_center_name,  --运单目的分拨名称
t1.dest_center_code,  --目的所属分拨code
t1.dest_center_name,  --目的所属分拨名称
t1.dest_area_code,   --目的片区code
t1.dest_area_name, --目的片区名称
t1.dest_first_site_code,  --目的一级网点code
t1.dest_first_site_name,  --目的一级网点名称
t1.dest_site_code,  --目的网点code 
t1.dest_site_name  --目的网点名称
)
--select * from temp3 

--老bi求出当月相关指标带距离
,temp4 as (
select 
t2.province_area_code as dest_province_area_code,  --目的营运区code
t2.province_area_name as dest_province_area_name,   --目的营运区名称
t2.center_code as dest_center_code,  --目的所属分拨code
t2.center_name as dest_center_name,  --目的所属分拨名称
t2.area_code as dest_area_code,   --目的片区code
t2.area_name as dest_area_name, --目的片区名称
t2.first_site_code as dest_first_site_code,  --目的一级网点code
t2.first_site_name as dest_first_site_name,  --目的一级网点名称
-- COALESCE(t3.distance, t4.distance) as distance,
-- t1.site_to_center_distance_range  as distance_2,  --距离
sum(real_sign_cnt) as   signed_ticket_ccm, --月签收票数
sum(should_sign_cnt) as should_ticket_ccm  --月应签票数
from dws_oper_eff_sign_rate t1
left join site_info t2
on t1.next_site_code=t2.site_code
where stats_date>='2026-11-01' and stats_date <'2026-11-10' 
and t1.time_efficient_name1='T+0' 
group by 
t2.province_area_code ,  --目的营运区code
t2.province_area_name,   --目的营运区名称
t2.center_code ,  --目的所属分拨code
t2.center_name,  --目的所属分拨名称
t2.area_code,   --目的片区code
t2.area_name, --目的片区名称
t2.first_site_code,  --目的一级网点code
t2.first_site_name  --目的一级网点名称


)
--select * from temp4
--超时未签表计算相关指标
,temp5 as (
select 
dest_first_site_code,  -- 目的一级网点code
dest_first_site_name,  -- 目的一级网点名称
t2.distance,
dest_province_area_code,  --  目的营运区code
dest_province_area_name,   -- 目的营运区名称
dest_center_code,  -- 目的所属分拨code
dest_center_name,  --  目的所属分拨名称
dest_area_code,   -- 目的片区code
dest_area_name, -- 目的片区名称
count(*) as sign_timeout_ticket,  -- 签收超时票数
sum(if (UNIX_TIMESTAMP('2026-11-10 09:05:00') - UNIX_TIMESTAMP(t1.validity_lasted_sign_time)>3*3600*24,1,0)) as sign_timeout_ticket_3d  --超3日票数
from 
dwd_oper_sign_timeout_detail_rt t1
 left join (select site_code,scan_site_code from 
(
select site_code,scan_site_code,ROW_NUMBER() over (PARTITION by site_code order by must_clock_time desc) as rn 
from dim_site_clock_maintain_s_d where dt='2026-11-09'
and data_valid =10 
and  clock_type=20
)t1
where rn=1)t4 on t1.dest_first_site_code=t4.site_code  
left join temp2 t2
on t1.dest_first_site_code=t2.site_code
and t4.scan_site_code=t2.center_code
where t1.stats_date='2026-11-09'
and t1.delivery_type_name not like '%自提%'
and t1.wait_notice_release_type IN (30,0)
and t1.sign_status = 10
and t1.is_cdc_delete=0
and t1.data_valid=10
and UNIX_TIMESTAMP('2026-11-10 09:05:00') - UNIX_TIMESTAMP(t1.validity_lasted_sign_time)>0
group by 
dest_first_site_code,  -- 目的一级网点code
dest_first_site_name,  -- 目的一级网点名称
t2.distance ,
dest_province_area_code,  --  目的营运区code
dest_province_area_name,   -- 目的营运区名称
dest_center_code,  -- 目的所属分拨code
dest_center_name,  --  目的所属分拨名称
dest_area_code,   -- 目的片区code
dest_area_name -- 目的片区名称
)
--select * from temp5
--合并
select 
stats_time,
dest_first_site_code,  --目的一级网点code
 case when  distance <=30 then '0-30km'
when distance >30 and distance <=50 then '30-50km'
when distance >50 and distance <=80 then '50-80km'
when distance >80 and distance <=120 then '80-120km'
when distance >120 and distance <=150 then '120-150km'
when distance >150 and distance <=200 then '150-200km'
when distance >200 and distance <=300 then '200-300km'
when distance >300 and distance <=500 then '300-500km'
when distance >500  then '500km以上'
else '其他'  end as kilometer_range,  --公里段  
dest_first_site_name,  --目的一级网点名称
dest_area_code,   --目的片区code
dest_area_name, --目的片区名称
dest_center_code,  -- 目的所属分拨code
dest_center_name,  --  目的所属分拨名称
dest_province_area_code,  --目的营运区code
dest_province_area_name,   --目的营运区名称
distance,
should_ticket_ccd,  --今日应签票数
signed_ticket_ccd,  --今日已签票数
unsigned_ticket_ccd,   --今日未签票数
should_ticket_ccm,  --当月应签票数
signed_ticket_ccm,  --当月已签票数
sign_timeout_ticket,  --超时未签
sign_timeout_ticket_3d  --超三日未签
from 
(
select 
'2026-11-10 09:05:00' as stats_time,
dest_first_site_code,  --目的一级网点code
dest_first_site_name,  --目的一级网点名称
dest_area_code,   --目的片区code
dest_area_name, --目的片区名称
dest_center_code,  -- 目的所属分拨code
dest_center_name,  --  目的所属分拨名称
dest_province_area_code,  --目的营运区code
dest_province_area_name,   --目的营运区名称
max(distance) as distance,
sum(should_ticket_ccd) as should_ticket_ccd,  --今日应签票数
sum(signed_ticket_ccd) as signed_ticket_ccd,  --今日已签票数
sum(unsigned_ticket_ccd) as unsigned_ticket_ccd,   --今日未签票数
sum(should_ticket_ccm) as should_ticket_ccm,  --当月应签票数
sum(signed_ticket_ccm) as signed_ticket_ccm,  --当月已签票数
sum(sign_timeout_ticket) as sign_timeout_ticket,  --超时未签
sum(sign_timeout_ticket_3d) as sign_timeout_ticket_3d  --超三日未签
from 
(
select 
dest_first_site_code,  --目的一级网点code
distance,
dest_first_site_name,  --目的一级网点名称
dest_area_code,   --目的片区code
dest_area_name, --目的片区名称
dest_center_code,  -- 目的所属分拨code
dest_center_name,  --  目的所属分拨名称
dest_province_area_code,  --目的营运区code
dest_province_area_name,   --目的营运区名称
should_ticket_ccd,  --今日应签票数
signed_ticket_ccd,  --今日已签票数
unsigned_ticket_ccd,   --今日未签票数
0 as signed_ticket_ccm,
0 as should_ticket_ccm,
0 as sign_timeout_ticket,
0 as sign_timeout_ticket_3d
from temp3 

union all
select 
dest_first_site_code,  --目的一级网点code
null distance,
dest_first_site_name,  --目的一级网点名称
dest_area_code,   --目的片区code
dest_area_name, --目的片区名称
dest_center_code,  -- 目的所属分拨code
dest_center_name,  --  目的所属分拨名称
dest_province_area_code,  --目的营运区code
dest_province_area_name,   --目的营运区名称
0 as should_ticket_ccd,  --今日应签票数
0 as signed_ticket_ccd,  --今日已签票数
0 as unsigned_ticket_ccd,   --今日未签票数
signed_ticket_ccm,
should_ticket_ccm,
0 as sign_timeout_ticket,
0 as sign_timeout_ticket_3d
from
 temp4  
 union all
select 
dest_first_site_code,  --目的一级网点code
distance,
dest_first_site_name,  --目的一级网点名称
dest_area_code,   --目的片区code
dest_area_name, --目的片区名称
dest_center_code,  -- 目的所属分拨code
dest_center_name,  --  目的所属分拨名称
dest_province_area_code,  --目的营运区code
dest_province_area_name,   --目的营运区名称
0 as should_ticket_ccd,  --今日应签票数
0 as signed_ticket_ccd,  --今日已签票数
0 as unsigned_ticket_ccd,   --今日未签票数
0 as signed_ticket_ccm,
0 as should_ticket_ccm,
sign_timeout_ticket,
sign_timeout_ticket_3d
from 
temp5
)t1
group by 
dest_first_site_code,  --目的一级网点code
dest_first_site_name,  --目的一级网点名称
dest_area_code,   --目的片区code
dest_area_name, --目的片区名称
dest_center_code,  -- 目的所属分拨code
dest_center_name,  --  目的所属分拨名称
dest_province_area_code,  --目的营运区code
dest_province_area_name   --目的营运区名称
)tt
;

-- [07] dws_oper_sign_first_site_distance_rt
-- 任务 dws_oper_sign_first_site_distance_rt | 任务实例 ID 4655677 | 任务定义编码 22434623479776
-- 工作流实例 罗盘-签收实时监控-19-20261110090512372 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682487
-- 执行结果 2026-11-10 09:05:19.115 +0800 | 日志第 5321 行 | JDBC 返回行数 8653
insert into dws_oper_sign_first_site_distance_rt
(
stats_time,
dest_first_site_code,
distance,
dest_first_site_name,
dest_area_code,
dest_area_name,
dest_province_area_code,
dest_province_area_name,
should_ticket_ccd, 
signed_ticket_ccd,
unsigned_ticket_ccd, 
should_ticket_ccm, 
signed_ticket_ccm,
sign_timeout_ticket, 
sign_timeout_ticket_3d 
)
WITH  site_info as (
select 
 t1.site_code
,t1.site_name
,t1.first_site_code
,t2.site_name as first_site_name
,t1.area_code
,t1.area_name
,t1.center_code
,t1.center_name
,t1.province_area_code
,t1.province_area_name
 from tx_bd_site_info t1
 left join tx_bd_site_info t2
 on t1.first_site_code=t2.site_code
)
--当日指标
,temp1 as (
select 
t1.waybill_no,
t1.validity_lasted_sign_time, --时效最晚签收时间
t1.sign_status,  --签收状态
t2.province_area_code as dest_province_area_code,  --目的营运区code
t2.province_area_name as dest_province_area_name,   --目的营运区名称
t1.waybill_dest_center_code,  --运单目的分拨code
t4.site_name as waybill_dest_center_name,  --运单目的分拨名称
t2.center_code as dest_center_code,  --目的所属分拨code
t2.center_name as dest_center_name,  --目的所属分拨名称
t2.area_code as dest_area_code,   --目的片区code
t2.area_name as dest_area_name, --目的片区名称
t2.first_site_code as dest_first_site_code,  --目的一级网点code
t2.first_site_name as dest_first_site_name,  --目的一级网点名称
t1.dest_site_code,  --目的网点code 
t2.site_name as dest_site_name , --目的网点名称
t1.send_site_code , --寄件网点
t1.should_sign_site_code , --应签网点
if( t1.validity_lasted_sign_time= '2026-11-10 23:59:59'  and t1.wait_notice_release_type  IN (30,0) and t3.dict_name  not like '%自提%' ,1,0)  as should_ticket_ccd,  --今日应签票数
if( t1.validity_lasted_sign_time= '2026-11-10 23:59:59' and t1.sign_status <> 10 and t1.wait_notice_release_type  IN (30,0) and t3.dict_name  not like '%自提%' ,1,0)  as signed_ticket_ccd,  --今日已签票数
if( t1.validity_lasted_sign_time= '2026-11-10 23:59:59' and t1.sign_status = 10 and t1.wait_notice_release_type IN (30,0) and t3.dict_name  not like '%自提%' ,1,0)  as unsigned_ticket_ccd  --今日未签票数
from 
(
select 
waybill_no,
validity_lasted_sign_time, --时效最晚签收时间
sign_status,  --签收状态
waybill_dest_center_code,  --运单目的分拨code
dest_site_code,  --目的网点code 
send_site_code , --寄件网点
should_sign_site_code , --应签网点
wait_notice_release_type,
delivery_type_code,
data_valid,
is_cdc_delete,
row_number() over(partition by waybill_no order by update_time desc) as rn
from ods_tx_tx_site_should_sign_detail t1
where  validity_lasted_sign_time >='2026-10-31 00:00:00'
and validity_lasted_sign_time < '2026-12-10 09:05:05'
)t1
left join site_info t2 ON t1.dest_site_code = t2.site_code
left join site_info t4 on t1.waybill_dest_center_code=t4.site_code
left join tx_basic_dict_entry t3 ON type_code = 'pick_up' and t3.dict_code = t1.delivery_type_code
where t1.rn = 1
and t1.data_valid = 10
and t1.is_cdc_delete = 0
and t1.validity_lasted_sign_time >= '2026-11-10 00:00:00'
AND t1.validity_lasted_sign_time <=  '2026-11-10 23:59:59' 
)
--距离表
,temp2 as (
select 
site_code
,center_code
,distance
 from 
(select 
site_code
,center_code
,distance
,ROW_NUMBER() over (PARTITION by site_code,center_code order by create_time desc ) as rn 
from tx_site_delivery_distance 
)t
where t.rn=1
)
--select * from temp2

--当日相关指标带距离
,temp3 as (
  select 
case when  t2.distance <=50 then '0-50km'
when t2.distance >50 and t2.distance <=100 then '50-100km'
when t2.distance >100 and t2.distance <=150 then '100-150km'
when t2.distance >150 and t2.distance <=300 then '150-300km'
when t2.distance >300  then '300km以上'
else '其它' end as distance,  --距离
t1.dest_province_area_code,  --目的营运区code
t1.dest_province_area_name,   --目的营运区名称
t1.waybill_dest_center_code,  --运单目的分拨code
t1.waybill_dest_center_name,  --运单目的分拨名称
t1.dest_center_code,  --目的所属分拨code
t1.dest_center_name,  --目的所属分拨名称
t1.dest_area_code,   --目的片区code
t1.dest_area_name, --目的片区名称
t1.dest_first_site_code,  --目的一级网点code
t1.dest_first_site_name,  --目的一级网点名称
sum(t1.should_ticket_ccd)   as should_ticket_ccd,  --今日应签票数
sum(t1.signed_ticket_ccd)   as signed_ticket_ccd,  --今日已签票数
sum(t1.unsigned_ticket_ccd) as unsigned_ticket_ccd   --今日未签票数
  from temp1 t1
left join 
temp2 t2
on t1.should_sign_site_code=t2.site_code
and t1.dest_center_code=t2.center_code
group by 
case when  t2.distance <=50 then '0-50km'
when t2.distance >50 and t2.distance <=100 then '50-100km'
when t2.distance >100 and t2.distance <=150 then '100-150km'
when t2.distance >150 and t2.distance <=300 then '150-300km'
when t2.distance >300  then '300km以上'
else '其它' end ,  --距离
t1.dest_province_area_code,  --目的营运区code
t1.dest_province_area_name,   --目的营运区名称
t1.waybill_dest_center_code,  --运单目的分拨code
t1.waybill_dest_center_name,  --运单目的分拨名称
t1.dest_center_code,  --目的所属分拨code
t1.dest_center_name,  --目的所属分拨名称
t1.dest_area_code,   --目的片区code
t1.dest_area_name, --目的片区名称
t1.dest_first_site_code,  --目的一级网点code
t1.dest_first_site_name,  --目的一级网点名称
t1.dest_site_code,  --目的网点code 
t1.dest_site_name  --目的网点名称
)
--select * from temp3 

--老bi求出当月相关指标带距离
,temp4 as (
select 
t2.province_area_code as dest_province_area_code,  --目的营运区code
t2.province_area_name as dest_province_area_name,   --目的营运区名称
t2.center_code as dest_center_code,  --目的所属分拨code
t2.center_name as dest_center_name,  --目的所属分拨名称
t2.area_code as dest_area_code,   --目的片区code
t2.area_name as dest_area_name, --目的片区名称
t2.first_site_code as dest_first_site_code,  --目的一级网点code
t2.first_site_name as dest_first_site_name,  --目的一级网点名称
t1.site_to_center_distance_range  as distance,  --距离
sum(real_sign_cnt) as   signed_ticket_ccm, --月签收票数
sum(should_sign_cnt) as should_ticket_ccm  --月应签票数
from dws_oper_eff_sign_rate t1
left join site_info t2
on t1.next_site_code=t2.site_code
where stats_date>='2026-11-01' and stats_date <'2026-11-10' 
and t1.time_efficient_name1='T+0' 
group by 
t2.province_area_code ,  --目的营运区code
t2.province_area_name,   --目的营运区名称
t2.center_code ,  --目的所属分拨code
t2.center_name,  --目的所属分拨名称
t2.area_code,   --目的片区code
t2.area_name, --目的片区名称
t2.first_site_code,  --目的一级网点code
t2.first_site_name,  --目的一级网点名称
t1.site_to_center_distance_range  --距离
)
--select * from temp4
--超时未签表计算相关指标
,temp5 as (
select 
dest_first_site_code,  -- 目的一级网点code
dest_first_site_name,  -- 目的一级网点名称
case when  t2.distance <=50 then '0-50km'
when t2.distance >50 and t2.distance <=100 then '50-100km'
when t2.distance >100 and t2.distance <=150 then '100-150km'
when t2.distance >150 and t2.distance <=300 then '150-300km'
when t2.distance >300  then '300km以上'
else '其它' end as distance,  --距离
dest_province_area_code,  --  目的营运区code
dest_province_area_name,   -- 目的营运区名称
dest_center_code,  -- 目的所属分拨code
dest_center_name,  --  目的所属分拨名称
dest_area_code,   -- 目的片区code
dest_area_name, -- 目的片区名称
count(*) as sign_timeout_ticket,  -- 签收超时票数
sum(if (UNIX_TIMESTAMP('2026-11-10 09:05:00') - UNIX_TIMESTAMP(t1.validity_lasted_sign_time)>3*3600*24,1,0)) as sign_timeout_ticket_3d  --超3日票数
from 
dwd_oper_sign_timeout_detail_rt t1
left join temp2 t2
on t1.should_sign_site_code=t2.site_code
and t1.dest_center_code=t2.center_code
where t1.stats_date='2026-11-09'
and t1.delivery_type_name not like '%自提%'
and t1.wait_notice_release_type IN (30,0)
and t1.sign_status = 10
and t1.is_cdc_delete=0
and t1.data_valid=10
and UNIX_TIMESTAMP('2026-11-10 09:05:00') - UNIX_TIMESTAMP(t1.validity_lasted_sign_time)>0
group by 
dest_first_site_code,  -- 目的一级网点code
dest_first_site_name,  -- 目的一级网点名称
case when  t2.distance <=50 then '0-50km'
when t2.distance >50 and t2.distance <=100 then '50-100km'
when t2.distance >100 and t2.distance <=150 then '100-150km'
when t2.distance >150 and t2.distance <=300 then '150-300km'
when t2.distance >300  then '300km以上'
else '其它' end ,  --距离
dest_province_area_code,  --  目的营运区code
dest_province_area_name,   -- 目的营运区名称
dest_center_code,  -- 目的所属分拨code
dest_center_name,  --  目的所属分拨名称
dest_area_code,   -- 目的片区code
dest_area_name -- 目的片区名称
)
--select * from temp5
--合并

select 
'2026-11-10 09:05:00' as stats_time,
dest_first_site_code,  --目的一级网点code
distance,
dest_first_site_name,  --目的一级网点名称
dest_area_code,   --目的片区code
dest_area_name, --目的片区名称
dest_province_area_code,  --目的营运区code
dest_province_area_name,   --目的营运区名称
sum(should_ticket_ccd) as should_ticket_ccd,  --今日应签票数
sum(signed_ticket_ccd) as signed_ticket_ccd,  --今日已签票数
sum(unsigned_ticket_ccd) as unsigned_ticket_ccd,   --今日未签票数
sum(should_ticket_ccm) as should_ticket_ccm,  --当月应签票数
sum(signed_ticket_ccm) as signed_ticket_ccm,  --当月已签票数
sum(sign_timeout_ticket) as sign_timeout_ticket,  --超时未签
sum(sign_timeout_ticket_3d) as sign_timeout_ticket_3d  --超三日未签
from 
(
select 
dest_first_site_code,  --目的一级网点code
distance,
dest_first_site_name,  --目的一级网点名称
dest_area_code,   --目的片区code
dest_area_name, --目的片区名称
dest_province_area_code,  --目的营运区code
dest_province_area_name,   --目的营运区名称
should_ticket_ccd,  --今日应签票数
signed_ticket_ccd,  --今日已签票数
unsigned_ticket_ccd,   --今日未签票数
0 as signed_ticket_ccm,
0 as should_ticket_ccm,
0 as sign_timeout_ticket,
0 as sign_timeout_ticket_3d
from temp3 
union all
select 
dest_first_site_code,  --目的一级网点code
distance,
dest_first_site_name,  --目的一级网点名称
dest_area_code,   --目的片区code
dest_area_name, --目的片区名称
dest_province_area_code,  --目的营运区code
dest_province_area_name,   --目的营运区名称
0 as should_ticket_ccd,  --今日应签票数
0 as signed_ticket_ccd,  --今日已签票数
0 as unsigned_ticket_ccd,   --今日未签票数
signed_ticket_ccm,
should_ticket_ccm,
0 as sign_timeout_ticket,
0 as sign_timeout_ticket_3d
from
 temp4  
 union all
select 
dest_first_site_code,  --目的一级网点code
distance,
dest_first_site_name,  --目的一级网点名称
dest_area_code,   --目的片区code
dest_area_name, --目的片区名称
dest_province_area_code,  --目的营运区code
dest_province_area_name,   --目的营运区名称
0 as should_ticket_ccd,  --今日应签票数
0 as signed_ticket_ccd,  --今日已签票数
0 as unsigned_ticket_ccd,   --今日未签票数
0 as signed_ticket_ccm,
0 as should_ticket_ccm,
sign_timeout_ticket,
sign_timeout_ticket_3d
from 
temp5
)t1
group by 
dest_first_site_code,  --目的一级网点code
distance,
dest_first_site_name,  --目的一级网点名称
dest_area_code,   --目的片区code
dest_area_name, --目的片区名称
dest_province_area_code,  --目的营运区code
dest_province_area_name   --目的营运区名称
;

-- [08] dwd_oper_sign_timeout_detail_rt
-- 任务 insert_dwd_oper_sign_timeout_detail_rt | 任务实例 ID 4655664 | 任务定义编码 20953663401952
-- 工作流实例 罗盘-签收实时监控明细表-10-20261110090506354 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682485
-- 执行结果 2026-11-10 09:05:10.724 +0800 | 日志第 1196 行 | JDBC 返回行数 7730
INSERT INTO dwd_oper_sign_timeout_detail_rt
 WITH  site_info as (
select 
 t1.site_code
,t1.site_name
,t1.first_site_code
,t2.site_name as first_site_name
,t1.area_code
,t1.area_name
,t1.center_code
,t1.center_name
,t1.province_area_code
,t1.province_area_name
 from tx_bd_site_info t1
 left join tx_bd_site_info t2
 on t1.first_site_code=t2.site_code
)
 select 
t1.stats_date,
t1.waybill_no,
t2.validity_lasted_sign_time, --时效最晚签收时间
t2.sign_validity,   --签收时效
t2.sign_status,  --签收状态
t2.should_sorting_time,  --应分拣时间
t2.dest_province_area_code,  --目的营运区code
t2.dest_province_area_name,   --目的营运区名称
t1.waybill_dest_center_code,  --运单目的分拨code
t1.waybill_dest_center_name,  --运单目的分拨名称
t2.dest_center_code,  --目的所属分拨code
t2.dest_center_name,  --目的所属分拨名称
t2.dest_area_code,   --目的片区code
t2.dest_area_name, --目的片区名称
t2.dest_first_site_code,  --目的一级网点code
t2.dest_first_site_name,  --目的一级网点名称
t2.dest_site_code,  --目的网点code 
t2.dest_site_name , --目的网点名称
t2.should_sign_site_code,
t1.send_site_code,  --寄件网点code
t1.send_site_name, --寄件网点名称
t1.send_first_site_code,  --寄件一级网点code
t1.send_first_site_name,  --寄件一级网点名称
t1.send_center_code,  --寄件分拨code
t1.send_center_name ,  --寄件分拨名称
t2.receive_province_code, --收件省份code
t2.receive_province_name,  --收件省份名称
t2.receive_city_code,  --收件城市code
t2.receive_city_name, --收件城市名称
t2.receive_district_code as receiver_county_code,  --收件区县code
t2.receive_district_name as receiver_county_name,  --收件区县名称
t2.receive_street_code as receiver_town_code,  --收件乡镇code
t2.receive_street as receiver_town_name,  --收件乡镇街道
t2.total_piece as parcel_count, --运单件数
t2.calc_weight,  --结算重量
t2.delivery_type_code, --送货方式code
t2.delivery_type_name,  --送货方式名称
t2.wait_notice_release_type,  --等通知放货类型:10-待寄方通知20-已通知放货30-空
t2.in_warehouse_type,  --进仓件 10-否；20-是
t2.in_warehouse_date,  --进仓日期
t2.should_leave_time, --应离港时间
t2.sort_scan_time,  --分拣扫描时间
t2.load_scan_time,  --装车扫描时间
t2.out_confirm_time,  --出站确认时间
t2.sign_time,  --签收扫描时间
t2.dest_modify_time,  --目的网点变更时间
t2.unload_scan_time,  --卸车扫描时间
t2.reach_scan_time as arrival_scan_time,  --到件扫描时间
t2.delivery_time,  --派件时间 
0 as sign_timeout_time , --签收超时时间 
t2.is_cdc_delete,
t2.data_valid,
CURRENT_TIMESTAMP as etl_time
from 
(select * from dwd_oper_sign_timeout_detail_rt t1 where t1.stats_date='2026-11-09' 
)t1
inner  join
(select * from 
(
select 
t1.waybill_no,
t1.validity_lasted_sign_time, --时效最晚签收时间
t1.sign_validity,   --签收时效
t1.sign_status,  --签收状态
t2.province_area_code as dest_province_area_code,  --目的营运区code
t2.province_area_name as dest_province_area_name,   --目的营运区名称
t2.center_code as dest_center_code,  --目的所属分拨code
t2.center_name as dest_center_name,  --目的所属分拨名称
t2.area_code as dest_area_code,   --目的片区code
t2.area_name as dest_area_name, --目的片区名称
t2.first_site_code as dest_first_site_code,  --目的一级网点code
t2.first_site_name as dest_first_site_name,  --目的一级网点名称
t1.dest_site_code,  --目的网点code 
t2.site_name as dest_site_name , --目的网点名称
t1.receive_province_code, --收件省份code
t1.receive_province_name,  --收件省份名称
t1.receive_city_code,  --收件城市code
t1.receive_city_name, --收件城市名称
t1.receive_district_code,  --收件区县code
t1.receive_district_name,  --收件区县名称
t1.receive_street_code,  --收件乡镇code
t1.receive_street,  --收件乡镇街道
t1.should_sorting_time,  --应分拣时间
t1.total_piece, --运单件数
t1.calc_weight,  --结算重量
t1.delivery_type_code, --送货方式code
t3.dict_name as delivery_type_name,  --送货方式名称
t1.wait_notice_release_type,  --等通知放货类型:10-待寄方通知20-已通知放货30-空
t1.in_warehouse_type,  --进仓件 10-否；20-是
t1.in_warehouse_date,  --进仓日期
t1.should_leave_time, --应离港时间
t1.sort_scan_time,  --分拣扫描时间
t1.load_scan_time,  --装车扫描时间
t1.out_confirm_time,  --出站确认时间
t1.sign_time,  --签收扫描时间
t1.dest_modify_time,  --目的网点变更时间
t1.unload_scan_time,  --卸车扫描时间
t1.reach_scan_time,  --到件扫描时间
t1.delivery_time,  --派件时间
t1.data_valid,
t1.should_sign_site_code,
t1.is_cdc_delete,
row_number() over(partition by t1.waybill_no order by t1.update_time desc) as rn
from ods_tx_tx_site_should_sign_detail t1
left join site_info t2 ON t1.dest_site_code = t2.site_code
left join tx_basic_dict_entry t3 ON type_code = 'pick_up' and t3.dict_code = t1.delivery_type_code
where  t1.validity_lasted_sign_time >=DATE_FORMAT(DATE_ADD('2026-11-10 09:05:00', INTERVAL -40 day ), '%Y-%m-%d 00:00:00')  
and t1.validity_lasted_sign_time < '2026-12-10 09:05:05'
and t1.etl_time>= DATE_ADD('2026-11-10 09:05:00', INTERVAL -1 hour) 
and t1.etl_time<'2026-11-10 09:05:00'
)ts where ts.rn=1
)t2
on t1.waybill_no=t2.waybill_no
;

-- [09] dws_oper_center_retain_1d
-- 任务 dws_oper_center_retain_1d | 任务实例 ID 4655621 | 任务定义编码 18819138154048
-- 工作流实例 实时库存&分拨留仓-28-20261110090001036 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682475
-- 执行结果 2026-11-10 09:00:17.205 +0800 | 日志第 242 行 | JDBC 返回行数 18487
INSERT INTO  dws_oper_center_retain_1d
SELECT
        stats_time,--统计日期
		instock_center_code,         --入库分拨code
		instock_center_name,                 --入库分拨名称
		instock_province_area_code,   --入库营运区code
		instock_province_area_name,   --入库营运区名称
		class_area,  --类区大类
		class_area_name,  --类区
		kilometer_range, --公里段
		stock_node_type,   --节点类型
        site_type_name,   --网点类型
		next_site_code,        --下一站编码
		next_site_name,       --下一站名称
		next_province_area_code,      --下一站营运区code
		next_province_area_name,      --下一站营运区name
        retain_time_slot,    --留仓时段
        stock_status,                --库存状态
        loader_dept,            --操作部
        loader_team,            --操作队
        COUNT(*) as retain_ticket,               --留仓票数
        SUM(stock_calc_weight) as retain_weight,  --留仓重量
		CURRENT_TIMESTAMP as etl_time 
    FROM dws_oper_waybill_retain_1d
	where stats_time='2026-11-10 09:00:00'
    and zone_type_name='正常库区'
    GROUP BY
        stats_time,--统计日期
        stock_node_type,   --节点类型
        site_type_name,   --网点类型
		class_area,  --类区大类
		class_area_name,  --类区
		kilometer_range, --公里段
		instock_province_area_code,   --入库营运区code
		instock_province_area_name,   --入库营运区名称
		next_province_area_code,      --下一站营运区code
		next_province_area_name,      --下一站营运区name
		instock_center_code,         --入库分拨code
		instock_center_name,                 --入库分拨名称
        next_site_code,        --下一站编码
		next_site_name,       --下一站名称
        retain_time_slot,    --留仓时段
        stock_status,                --库存状态
        loader_dept,            --操作部
        loader_team            --操作队
;

-- [10] dws_busi_org_input_cost_i_d
-- 任务 dws_busi_org_input_cost_i_d | 任务实例 ID 4655612 | 任务定义编码 21758110061664
-- 工作流实例 罗盘-网点班车-31-20261110090001036 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682474
-- 执行结果 2026-11-10 09:00:14.206 +0800 | 日志第 1980 行 | JDBC 返回行数 481
insert into dws_busi_org_input_cost_i_d

--总成本本月金额
with temp1 as (
select 
area_code  --营销大区code
,area_name  --营销大区名称
,province_area_code -- 营运区code
,province_area_name -- 营运区名称
,COALESCE(SUM(if (launch_date<='2026-11-09'and expiration_date>=t2.first_departure_date and t2.first_departure_date>=launch_date,t2.plan_cost,0)),0) as month_amount  --总成本本月金额
from 
(
select  
area_code  --营销大区code
,area_name  --营销大区名称
,province_area_code -- 营运区code
,province_area_name -- 营运区名称
,schedule_name
,date(launch_time) as launch_date
,date(expiration_time) as expiration_date
from  dwd_busi_shuttle_bus_input_detail_f_d 
where stats_date='2026-11-09' and  site_type_code in (710100,800200)
group by 
area_code  --营销大区code
,area_name  --营销大区名称
,province_area_code -- 营运区code
,province_area_name -- 营运区名称
,schedule_name
,date(launch_time) 
,date(expiration_time)
)t1
left join 
(
select plan_schedule_name,plan_cost,date(first_departure_time) as first_departure_date
from ods_eagle_t_dispatch_task_i_d 
where create_time>=DATE_FORMAT(DATE_ADD('2026-11-09', INTERVAL -1 MONTH), '%Y-%m-15 00:00:00')  
and create_time<='2026-11-09 23:59:59' 
and data_valid='10'  
and task_status in (50,70) 
and actual_dest_arrive_time>=DATE_FORMAT('2026-11-09', '%Y-%m-01 00:00:00')
and actual_dest_arrive_time<='2026-11-09 23:59:59'
)t2
on t1.schedule_name=t2.plan_schedule_name
group by area_code  --营销大区code
,area_name  --营销大区名称
,province_area_code -- 营运区code
,province_area_name -- 营运区名称

),
--回收成本本月金额
temp2 as (
select 
t3.area_code  --营销大区code
,t3.area_name  --营销大区名称
,t3.province_area_code -- 营运区code
,t3.province_area_name -- 营运区名称
,sum(COALESCE(t2.month_recycle_cost,0))/1.06 as month_recycle_cost
 from 
(
select
t1.site_code
,t1.site_name 
,sum(if(transaction_type=1,amount,0))-sum(if(transaction_type=0,amount,0)) as month_recycle_cost --回收成本
from bd_bill_busi_detail t1
where send_time>=DATE_FORMAT('2026-11-09', '%Y-%m-01 00:00:00')
and send_time<='2026-11-09 23:59:59' 
and charge_itme_id='159' -- 网点直发车-扣款
group by t1.site_code
,t1.site_name 
)t2
left join tx_bd_site_info t3
on t2.site_code=t3.site_code
group by 
t3.area_code  --营销大区code
,t3.area_name  --营销大区名称
,t3.province_area_code -- 营运区code
,t3.province_area_name -- 营运区名称
)
--营销大区
,temp3 as 
(
select 
area_code as org_brnch_code  --组织编码
,area_name as  org_brnch_name --组织编码名称
,province_area_code as  parent_site_code -- 上级组织编码
,province_area_name as  parent_site_name -- 上级组织名称
,6 as org_brnch_lvl  -- 组织层级（6-营销大区，3-营运区，1-总部）
,10 as cost_type --成本类型（10：总成本本月金额 20：回收成本本月金额） 
,month_amount  as month_cost_amount --成本本月金额
from temp1
UNION all
select  
area_code as org_brnch_code  --组织编码
,area_name as  org_brnch_name --组织编码名称
,province_area_code as  parent_site_code -- 上级组织编码
,province_area_name as  parent_site_name -- 上级组织名称
,6 as org_brnch_lvl  -- 组织层级（6-营销大区，3-营运区，1-总部）
,20 as cost_type --成本类型（10：总成本本月金额 20：回收成本本月金额）
,month_recycle_cost as month_cost_amount  --成本本月金额
from 
temp2)

--汇总
select 
'2026-11-09' as stats_date
,org_brnch_code  --组织编码
,org_brnch_lvl  -- 组织层级（6-营销大区，3-营运区，1-总部）
,cost_type --成本类型（10：总成本本月金额 20：回收成本本月金额）
,org_brnch_name --组织编码名称
,parent_site_code -- 上级组织编码
,parent_site_name -- 上级组织名称
,month_cost_amount  --成本本月金额
,CURRENT_TIMESTAMP as etl_time
from 
(
select 
org_brnch_code  --组织编码
,org_brnch_name --组织编码名称
,parent_site_code -- 上级组织编码
,parent_site_name -- 上级组织名称
,org_brnch_lvl  -- 组织层级（6-营销大区，3-营运区，1-总部）
,cost_type --成本类型（10：总成本本月金额 20：回收成本本月金额） 
,month_cost_amount  --成本本月金额
from temp3
where org_brnch_code<>0
union all
select 
parent_site_code as org_brnch_code  --组织编码
,parent_site_name as  org_brnch_name --组织编码名称
,999999 as  parent_site_code -- 上级组织编码
,'安能物流' as  parent_site_name -- 上级组织名称
,3 as org_brnch_lvl  -- 组织层级（6-营销大区，3-营运区，1-总部）
,cost_type --成本类型（10：总成本本月金额 20：回收成本本月金额） 
,sum(month_cost_amount) as month_cost_amount --成本本月金额
from temp3
group by 
parent_site_code
,parent_site_name
,cost_type
union all
select 
999999 as org_brnch_code  --组织编码
,'安能物流' as  org_brnch_name --组织编码名称
,NULL as  parent_site_code -- 上级组织编码
,NULL as  parent_site_name -- 上级组织名称
,1 as org_brnch_lvl  -- 组织层级（6-营销大区，3-营运区，1-总部）
,cost_type --成本类型（10：总成本本月金额 20：回收成本本月金额） 
,sum(month_cost_amount) as month_cost_amount --成本本月金额
from temp3
group by 
cost_type)t
;

-- [11] lbdb.ads_surch_policy_sign_site_review_1d
-- 任务 ads_surch_policy_sign_site_review_1d | 任务实例 ID 4655611 | 任务定义编码 18440367376320
-- 工作流实例 3300特殊区域加收检视报表-32-20261110090001036 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682469
-- 执行结果 2026-11-10 09:00:15.127 +0800 | 日志第 1217 行 | JDBC 返回行数 123935
INSERT INTO lbdb.ads_surch_policy_sign_site_review_1d
WITH main_temp_table AS (
    SELECT
        sign_date,
        first_site_code,
        first_site_name AS derive_first_site_name,
        fee_1412,
        varchar_standby_2,
        calc_weight,
        waybill_no,
        CASE
            WHEN m1.first_site_name LIKE '%集散站%' THEN m1.sign_site_name
            ELSE m1.first_site_name
        END AS first_site_name,
        CASE
            WHEN m2.dict_name LIKE '%电商项目%'
                OR m1.send_site_name LIKE '%宇宏项目%'
                OR m2.dict_name in ('安能小程序-安能平台','支付宝寄大件-安能平台')
            THEN '大客户'
            ELSE '网络'
        END AS source
    FROM dwd_fin_wayb_sign_site_income_cost_i_m m1
    LEFT JOIN tx_basic_dict_entry m2
        ON m1.ec_id = m2.dict_code
        AND m2.type_code = 'platform_name'
        AND m2.data_status = '10'
    WHERE
        m1.stats_date = DATE_FORMAT('2026-11-09', '%Y-%m-01')
        AND m1.sign_date BETWEEN DATE_FORMAT('2026-11-09', '%Y-%m-01')
        AND '2026-11-09'
        AND m1.first_site_code is not null
)
SELECT
    t.sign_date AS stats_date,
    t.first_site_code,
    first_site_name,
    MAX(t.derive_first_site_name) AS derive_first_site_name,
    MAX(t1.area_code) AS area_code,
    MAX(t1.area_name) AS area_name,
    MAX(t1.province_area_code) AS province_area_code,
    MAX(t1.province_area_name) AS province_area_name,
    MAX(t1.site_to_center_distance) AS site_to_center_distance,
    MAX(t1.city_name) AS city_name,
    t.source,
    MAX(t2.receipt_rate) AS receipt_rate,
    COUNT(DISTINCT
        IF(
            COALESCE(t.fee_1412, 0) > 0
            AND COALESCE(t.calc_weight, 0) <= 800
            AND t.varchar_standby_2 IS NOT NULL
            AND t.varchar_standby_2 <> '',
            waybill_no,
            NULL
        )
    ) AS zero_surch_policy_ticket,
    SUM(
        IF(
            COALESCE(t.fee_1412, 0) > 0
            AND COALESCE(t.calc_weight, 0) <= 800
            AND t.varchar_standby_2 IS NOT NULL
            AND t.varchar_standby_2 <> '',
            COALESCE(t.calc_weight, 0),
            0
        )
    ) AS zero_surch_policy_calc_weight,
    SUM(
        IF(
            COALESCE(t.calc_weight, 0) > 0
            AND COALESCE(t.calc_weight, 0) <= 800,
            COALESCE(t.calc_weight, 0),
            0
        )
    ) AS calc_weight_0_between_300,
    SUM(
        IF(
            COALESCE(t.fee_1412, 0) > 0
            AND COALESCE(t.calc_weight, 0) <= 800
            AND t.varchar_standby_2 IS NOT NULL
            AND t.varchar_standby_2 <> '',
            COALESCE(t.fee_1412, 0),
            0
        )
    ) AS zero_surch_policy_cost,
    NOW() AS etl_time,
    sum(COALESCE(day_sign_cnt,0)) as day_sign_cnt,
    sum(COALESCE(should_sign_cnt,0)) as should_sign_cnt
FROM main_temp_table t
LEFT JOIN (
    SELECT
        site_code,
        area_code,
        area_name,
        province_area_code,
        province_area_name,
        site_to_center_distance,
        city_name
    FROM dim_tx_site_info_d
    WHERE stats_date = '2026-11-09'
) t1
    ON t.first_site_code = t1.site_code
LEFT JOIN (
    SELECT
        stats_date,
        next_first_site_code AS first_site_code,
        sum(day_sign_cnt) as day_sign_cnt,
        sum(should_sign_cnt) as should_sign_cnt,
        SUM(day_sign_cnt) / SUM(should_sign_cnt) AS receipt_rate
    FROM dws_oper_eff_sign_rate
    WHERE stats_date BETWEEN DATE_FORMAT('2026-11-09', '%Y-%m-01')
        AND '2026-11-09'
    GROUP BY next_first_site_code, stats_date
) t2
    ON t.first_site_code = t2.first_site_code
    AND t.sign_date = t2.stats_date
GROUP BY
    t.sign_date,
    t.first_site_code,
    first_site_name,
    source;

-- [12] dws_busi_org_shuttle_bus_input_i_d
-- 任务 dws_busi_org_shuttle_bus_input_i_d | 任务实例 ID 4655610 | 任务定义编码 21758031994720
-- 工作流实例 罗盘-网点班车-31-20261110090001036 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682474
-- 执行结果 2026-11-10 09:00:14.155 +0800 | 日志第 6569 行 | JDBC 返回行数 2262
insert into dws_busi_org_shuttle_bus_input_i_d
WITH temp0 as 
(
select *,date(launch_time) as launch_date,DATE(expiration_time) as expiration_date 
from 
 dwd_busi_shuttle_bus_input_detail_f_d
where stats_date='2026-11-09'
and site_type_code in (710100,800200)

), temp1 as 
(
select 
site_code
,site_name
,site_type_code
,car_type
,area_code  --营销大区code
,area_name  --营销大区名称
,province_area_code -- 营运区code
,province_area_name -- 营运区名称
,schedule_name
,launch_date
,input_type
,expiration_date
,CASE WHEN input_type=10  and expiration_date>='2026-11-09' THEN input_cnt
WHEN input_type=20  and expiration_date>='2026-11-09' THEN input_cnt
ELSE 0 end as input_cnt  --投入数量

,CASE WHEN input_type=10  and launch_date>=DATE_FORMAT('2026-11-09', '%Y-%m-01')  and launch_date<='2026-11-09' THEN input_cnt
when input_type=20 and launch_date>=DATE_FORMAT('2026-11-09', '%Y-%m-01')  and launch_date<='2026-11-09' then input_cnt
ELSE 0 end as month_new_input_cnt  --本月新增数量

,CASE WHEN input_type=10  and expiration_date>=date_sub(DATE_FORMAT('2026-11-09', '%Y-%m-01'),1)  and expiration_date<'2026-11-09' THEN input_cnt
when input_type=20 and expiration_date>=date_sub(DATE_FORMAT('2026-11-09', '%Y-%m-01'),1)  and expiration_date<'2026-11-09' then input_cnt
ELSE 0 end as month_cancel_input_cnt --本月取消数量

from temp0

),
--剔除网点，去重
temp2 as (
select DISTINCT
car_type
,area_code  --营销大区code
,area_name  --营销大区名称
,province_area_code -- 营运区code
,province_area_name -- 营运区名称
,schedule_name
,launch_date
,input_type
,input_cnt  --投入数量
,month_new_input_cnt  --本月新增数量
,month_cancel_input_cnt --本月取消数量
from temp1 
)
--网点数单独计算
,temp3 as (
select 
car_type
,area_code  --营销大区code
,area_name  --营销大区名称
,province_area_code -- 营运区code
,province_area_name -- 营运区名称
,30 as input_type 
,COUNT(distinct (CASE WHEN expiration_date>='2026-11-09' THEN site_code ELSE NULL end )) as input_cnt  --投入数量
,COUNT(distinct (CASE WHEN  launch_date>=DATE_FORMAT('2026-11-09', '%Y-%m-01')  and launch_date<='2026-11-09' THEN site_code
ELSE NULL end )) as month_new_input_cnt  --本月新增数量
,COUNT(distinct (CASE WHEN  expiration_date>=date_sub(DATE_FORMAT('2026-11-09', '%Y-%m-01'),1)  and expiration_date<'2026-11-09' THEN site_code
ELSE NULL end )) as month_cancel_input_cnt --本月取消数量
from temp1 
group by 
car_type
,area_code  --营销大区code
,area_name  --营销大区名称
,province_area_code -- 营运区code
,province_area_name -- 营运区名称
union all
select 
999999 as car_type
,area_code  --营销大区code
,area_name  --营销大区名称
,province_area_code -- 营运区code
,province_area_name -- 营运区名称
,30 as input_type 
,COUNT(distinct (CASE WHEN expiration_date>='2026-11-09' THEN site_code ELSE NULL end )) as input_cnt  --投入数量
,COUNT(distinct (CASE WHEN  launch_date>=DATE_FORMAT('2026-11-09', '%Y-%m-01')  and launch_date<='2026-11-09' THEN site_code
ELSE null end )) as month_new_input_cnt  --本月新增数量
,COUNT(distinct (CASE WHEN  DATE(expiration_date)>=date_sub(DATE_FORMAT('2026-11-09', '%Y-%m-01'),1) and expiration_date<'2026-11-09' THEN site_code
ELSE null end )) as month_cancel_input_cnt --本月取消数量
from temp1 
group by 
area_code  --营销大区code
,area_name  --营销大区名称
,province_area_code -- 营运区code
,province_area_name -- 营运区名称
),
--营销大区计算
temp4 as (
select 
car_type
,area_code as org_brnch_code --组织code
,area_name as org_brnch_name --组织名称
,province_area_code as parent_site_code -- 上级code
,province_area_name as parent_site_name  -- 上级名称
,6 as org_brnch_lvl  -- 组织层级（6-营销大区，3-营运区，1-总部）
,input_type  ---- 投入类型 （10：头数量 20：挂数量 30:网点数）  
,sum(input_cnt ) as input_cnt --投入数量
,sum(month_new_input_cnt ) as month_new_input_cnt  --本月新增数量
,sum(month_cancel_input_cnt ) as month_cancel_input_cnt --本月取消数量
from temp2
group by 
car_type
,area_code  --营销大区code
,area_name  --营销大区名称
,province_area_code
,province_area_name
,input_type
UNION all

select 
car_type
,area_code as org_brnch_code --组织code
,area_name as org_brnch_name --组织名称
,province_area_code as parent_site_code -- 上级code
,province_area_name as parent_site_name  -- 上级名称
,6 as org_brnch_lvl  -- 组织层级（6-营销大区，3-营运区，1-总部）
,input_type ---- 投入类型 （10：头数量 20：挂数量 30:网点数） 
,input_cnt  --投入数量
,month_new_input_cnt  --本月新增数量
,month_cancel_input_cnt --本月取消数量
from temp3
)
--车型合计
,temp5 as (
select 
999999 as car_type
,org_brnch_code --组织code
,org_brnch_name --组织名称
,parent_site_code -- 上级code
,parent_site_name  -- 上级名称
,org_brnch_lvl  -- 组织层级（6-营销大区，3-营运区，1-总部）
,input_type ---- 投入类型 （10：头数量 20：挂数量 30:网点数） 
,sum(input_cnt)as input_cnt  --投入数量
,sum(month_new_input_cnt) as month_new_input_cnt  --本月新增数量
,sum(month_cancel_input_cnt) as month_cancel_input_cnt --本月取消数量
from temp4
where input_type<>30
group by 
org_brnch_code --组织code
,org_brnch_name --组织名称
,parent_site_code -- 上级code
,parent_site_name  -- 上级名称
,org_brnch_lvl  -- 组织层级（6-营销大区，3-营运区，1-总部）
,input_type ---- 投入类型 （10：头数量 20：挂数量 30:网点数） 
union all
select 
car_type
,org_brnch_code --组织code
,org_brnch_name --组织名称
,parent_site_code -- 上级code
,parent_site_name  -- 上级名称
,org_brnch_lvl  -- 组织层级（6-营销大区，3-营运区，1-总部）
,input_type ---- 投入类型 （10：头数量 20：挂数量 30:网点数） 
,input_cnt  --投入数量
,month_new_input_cnt  --本月新增数量
,month_cancel_input_cnt --本月取消数量
from temp4
)
select 
stats_date
,org_brnch_code --组织code
,org_brnch_lvl  -- 组织层级（6-营销大区，3-营运区，1-总部）
,input_type ---- 投入类型 （10：头数量 20：挂数量 30:网点数） 
,car_type
,org_brnch_name --组织名称
,parent_site_code -- 上级code
,parent_site_name  -- 上级名称
,input_cnt  --投入数量
,month_new_input_cnt  --本月新增数量
,month_cancel_input_cnt --本月取消数量
,CURRENT_TIMESTAMP as etl_time
from 
(
select 
'2026-11-09' as stats_date
,car_type
,org_brnch_code --组织code
,org_brnch_name --组织名称
,parent_site_code -- 上级code
,parent_site_name  -- 上级名称
,org_brnch_lvl  -- 组织层级（6-营销大区，3-营运区，1-总部）
,input_type ---- 投入类型 （10：头数量 20：挂数量 30:网点数） 
,input_cnt  --投入数量
,month_new_input_cnt  --本月新增数量
,month_cancel_input_cnt --本月取消数量
from temp5
union all
select 
'2026-11-09' as stats_date
,car_type
,parent_site_code as org_brnch_code --组织code
,parent_site_name as org_brnch_name --组织名称
,999999 as parent_site_code -- 上级code
,'安能物流' as parent_site_name  -- 上级名称
,3 as org_brnch_lvl  -- 组织层级（6-营销大区，3-营运区，1-总部）
,input_type ---- 投入类型 （10：头数量 20：挂数量 30:网点数） 
,sum(input_cnt)as input_cnt  --投入数量
,sum(month_new_input_cnt) as month_new_input_cnt  --本月新增数量
,sum(month_cancel_input_cnt) as month_cancel_input_cnt --本月取消数量
from temp5
group by 
car_type
,parent_site_code
,parent_site_name
,input_type 
UNION ALL
select 
'2026-11-09' as stats_date
,car_type
,999999 as org_brnch_code --组织code
,'安能物流' as org_brnch_name --组织名称
,NULL as parent_site_code -- 上级code
,NULL as parent_site_name  -- 上级名称
,1 as org_brnch_lvl  -- 组织层级（6-营销大区，3-营运区，1-总部）
,input_type ---- 投入类型 （10：头数量 20：挂数量 30:网点数） 
,sum(input_cnt)as input_cnt  --投入数量
,sum(month_new_input_cnt) as month_new_input_cnt  --本月新增数量
,sum(month_cancel_input_cnt) as month_cancel_input_cnt --本月取消数量
from temp5
group by 
car_type
,input_type 
)t
;

-- [13] ads_compass_timepoint_progress_monitor
-- 任务 insert_ads_compass_timepoint_progress_monitor_分拨 | 任务实例 ID 4655603 | 任务定义编码 21879560147552
-- 工作流实例 罗盘运营-进度监控-10分钟-59-20261110090001036 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682468
-- 执行结果 2026-11-10 09:00:11.067 +0800 | 日志第 2177 行 | JDBC 返回行数 9504
insert into ads_compass_timepoint_progress_monitor
-- 分拨
with org_info as (
    select site_code,
           site_name,
           center_code,
           center_name,
           province_area_code,
           province_area_name
    from tx_bd_site_info
    where date_format(current_date(), '%Y-%m-01') = '2026-11-01' -- 传入1号日期
    union all
    select svth_lvl_org_brnch_cd as site_code,
           svth_lvl_org_brnch_nm as site_name,
           fith_lvl_org_brnch_cd as center_code,
           fith_lvl_org_brnch_nm as center_name,
           thrd_lvl_org_brnch_cd as province_area_code,
           thrd_lvl_org_brnch_nm as province_area_name
    from dm_pb_org_brnch_tx_fin_month
    where stats_month='2026-11-01' and stats_month < date_format(current_date(), '%Y-%m-01')
),
hq_base as (
    select timepoint,
           date_format(timepoint, '%H:%i:00') minute_point,
           b.center_code org_code,
           b.center_name org_name,
           sum(calc_weight) calc_weight,
           sum(tickets) tickets,
           sum(pieces) pieces,
           sum(unload_calc_weight) unload_calc_weight,
           sum(unload_tickets) unload_tickets,
           sum(unload_pieces) unload_pieces,
           sum(sort_calc_weight) sort_calc_weight,
           sum(sort_tickets) sort_tickets,
           sum(sort_pieces) sort_pieces,
           sum(load_calc_weight) load_calc_weight,
           sum(load_tickets) load_tickets,
           sum(load_pieces) load_pieces,
           coalesce(sum(unload_calc_weight)/sum(calc_weight),0) unload_progress
    from dws_compass_timepoint_center_progress_monitor a
    left join org_info b on a.send_center_code=b.site_code
    where timepoint>=date_sub('2026-11-10', 365) and timepoint<date_add('2026-11-10',1)
      and b.site_name not like '%测试%'
    group by timepoint, b.center_code, b.center_name
),
hq_base_day as (
    select timepoint,
           date_format(timepoint, '%H:%i:00') minute_point,
           b.center_code org_code,
           b.center_name org_name,
           sum(calc_weight) calc_weight,
           sum(tickets) tickets,
           sum(pieces) pieces,
           sum(unload_calc_weight) unload_calc_weight,
           sum(unload_tickets) unload_tickets,
           sum(unload_pieces) unload_pieces,
           sum(sort_calc_weight) sort_calc_weight,
           sum(sort_tickets) sort_tickets,
           sum(sort_pieces) sort_pieces,
           sum(load_calc_weight) load_calc_weight,
           sum(load_tickets) load_tickets,
           sum(load_pieces) load_pieces,
           coalesce(sum(unload_calc_weight)/sum(calc_weight),0) unload_progress
    from dws_compass_timepoint_center_progress_monitor a
    left join org_info b on a.send_center_code=b.site_code
    where timepoint>='2026-11-10' and timepoint<date_add('2026-11-10',1)
      and b.site_name not like '%测试%' 
    group by timepoint, b.center_code, b.center_name
),
mvp_3 as (
    select timepoint, org_code, minute_point, if(unload_progress=0, null, unload_progress) unload_progress
    from (select timepoint, org_code, minute_point, unload_progress, 
                 row_number() over (partition by a.minute_point, a.org_code order by a.unload_progress desc, a.timepoint desc) rn
          from (select a.timepoint, a.org_code, a.minute_point, a.unload_progress
                from hq_base a
                left join hq_base_day b on a.minute_point = b.minute_point and a.org_code = b.org_code
                where a.calc_weight >= b.calc_weight * 0.97 and a.calc_weight <= b.calc_weight * 1.03
          ) a
    ) t
    where rn=1
),
mvp_6 as (
    select timepoint, org_code, minute_point, if(unload_progress=0, null, unload_progress) unload_progress
    from (select timepoint, org_code, minute_point, unload_progress, 
                 row_number() over (partition by a.minute_point, a.org_code order by a.unload_progress desc, a.timepoint desc) rn
          from (select a.timepoint, a.org_code, a.minute_point, a.unload_progress
                from hq_base a
                left join hq_base_day b on a.minute_point = b.minute_point and a.org_code = b.org_code
                where a.calc_weight >= b.calc_weight * 0.94 and a.calc_weight <= b.calc_weight * 1.06
          ) a
    ) t
    where rn=1
),
mvp_9 as (
    select timepoint, org_code, minute_point, if(unload_progress=0, null, unload_progress) unload_progress
    from (select timepoint, org_code, minute_point, unload_progress, 
                 row_number() over (partition by a.minute_point, a.org_code order by a.unload_progress desc, a.timepoint desc) rn
          from (select a.timepoint, a.org_code, a.minute_point, a.unload_progress
                from hq_base a
                left join hq_base_day b on a.minute_point = b.minute_point and a.org_code = b.org_code
                where a.calc_weight >= b.calc_weight * 0.91 and a.calc_weight <= b.calc_weight * 1.09
          ) a
    ) t
    where rn=1
),
mvp_12 as (
    select timepoint, org_code, minute_point, if(unload_progress=0, null, unload_progress) unload_progress
    from (select timepoint, org_code, minute_point, unload_progress, 
                 row_number() over (partition by a.minute_point, a.org_code order by a.unload_progress desc, a.timepoint desc) rn
          from (select a.timepoint, a.org_code, a.minute_point, a.unload_progress
                from hq_base a
                left join hq_base_day b on a.minute_point = b.minute_point and a.org_code = b.org_code
                where a.calc_weight >= b.calc_weight * 0.88 and a.calc_weight <= b.calc_weight * 1.12
          ) a
    ) t
    where rn=1
),
mvp_all as (
    select timepoint, org_code, minute_point, if(unload_progress=0, null, unload_progress) unload_progress
    from (select timepoint, org_code, minute_point, unload_progress,
                 row_number() over (partition by minute_point, org_code order by unload_progress desc, timepoint desc) rn
          from hq_base
    ) a
    where rn = 1
)
select a.timepoint,
       5 as org_lvl,
       a.org_code,
       a.org_name,
       a.calc_weight,
       a.tickets,
       a.pieces,
       a.unload_calc_weight,
       a.unload_tickets,
       a.unload_pieces,
       a.sort_calc_weight,
       a.sort_tickets,
       a.sort_pieces,
       a.load_calc_weight,
       a.load_tickets,
       a.load_pieces,
       a.history_mvp_moment,
       a.history_mvp_progress,
       a.unload_progress,
       case when unload_progress - history_mvp_progress > -0.05 then 1 -- '正常'
            when unload_progress - history_mvp_progress > -0.15 and unload_progress - history_mvp_progress <= -0.05 then 2 -- '较落后'
            when unload_progress - history_mvp_progress > -0.3 and unload_progress - history_mvp_progress <= -0.15 then 3 -- '落后'
            when unload_progress - history_mvp_progress <= -0.3 then 4 -- '严重落后'
       else -1 end unload_progress_type, -- '未知'
      now() as etl_time
from (select a.timepoint,
             a.org_code,
             a.org_name,
             a.calc_weight,
             a.tickets,
             a.pieces,
             a.unload_calc_weight,
             a.unload_tickets,
             a.unload_pieces,
             a.sort_calc_weight,
             a.sort_tickets,
             a.sort_pieces,
             a.load_calc_weight,
             a.load_tickets,
             a.load_pieces,
             coalesce(b.timepoint, c.timepoint, d.timepoint, e.timepoint, f.timepoint, a.timepoint) history_mvp_moment,
             if(coalesce(b.unload_progress, c.unload_progress, d.unload_progress, e.unload_progress, f.unload_progress, a.unload_progress)>1,
                1,
                coalesce(b.unload_progress, c.unload_progress, d.unload_progress, e.unload_progress, f.unload_progress, a.unload_progress)
                ) history_mvp_progress,
             if(a.unload_progress>1,1,a.unload_progress) unload_progress
      from hq_base_day a
      left join mvp_3 b on a.minute_point = b.minute_point and a.org_code = b.org_code 
      left join mvp_6 c on a.minute_point = c.minute_point and a.org_code = c.org_code 
      left join mvp_9 d on a.minute_point = d.minute_point and a.org_code = d.org_code 
      left join mvp_12 e on a.minute_point = e.minute_point and a.org_code = e.org_code 
      left join mvp_all f on a.minute_point = f.minute_point and a.org_code = f.org_code 
      where a.timepoint >= '2026-11-10' and a.timepoint < date_add('2026-11-10', 1)
) a
;

-- [14] dws_opt_site_send_transit_fee_i_d
-- 任务 insert_dws_opt_site_send_transit_fee_i_d | 任务实例 ID 4655600 | 任务定义编码 17623414030528
-- 工作流实例 网点运输费-15-20261110090000029 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682467
-- 执行结果 2026-11-10 09:00:12.860 +0800 | 日志第 3697 行 | JDBC 返回行数 643817
insert into dws_opt_site_send_transit_fee_i_d(send_site_code,
                                              dest_site_code,
                                              stats_date,
                                              order_source_id,
                                              order_source_name,
                                              send_center_code,
                                              send_province_area_code,
                                              dest_center_code,
                                              dest_province_area_code,
                                              send_site_name,
                                              send_center_name,
                                              send_province_area_name,
                                              dest_site_name,
                                              dest_center_name,
                                              dest_province_area_name,
                                              area_property,
                                              weight_range,
                                              distance_range,
                                              calc_weight,
                                              waybill_cnt,
                                              total_piece,
                                              total_transfer_out_fees,
                                              total_dispatch_out_fees,
                                              total_increment_out_fees,
                                              total_transfer_in_fees,
                                              total_dispatch_in_fees,
                                              total_increment_in_fees,
                                              stats_month,
                                              etl_time,
                                              weight_range_rn,
                                              distance_range_rn,
                                              data_type
						, product_type_code
						, product_type_name)
with site_info_tbl as (select l.site_code
                            , l.site_name
                            , r.join_ane_time
                            , l.first_site_code
                            , site_info.site_name as first_site_name
                            , l.province_area_name
                            , l.province_area_code
                            , l.center_code
                            , l.area_name
                            , l.center_name
                            , r.center_distance
                            , dict_tbl.dict_name  as site_type
                       from tx_bd_site_info l
                                left join tx_bd_site_detail r
                                          on l.site_code = r.site_code
                                left join tx_bd_site_info site_info
                                          on l.first_site_code = site_info.site_code
                                inner join (select dict_code, dict_name
                                            from tx_basic_dict_entry
                                            where type_code = 'site_type_code'
                                              and dict_code in (
                                                                '139',
                                                                '140',
                                                                '146',
                                                                '710100',
                                                                '710200',
                                                                '710300',
                                                                '720100',
                                                                '720200',
                                                                '720300'
                                                )) dict_tbl
                                           on l.site_type_code = dict_tbl.dict_code
    -- where site_info.site_name is null
),
     distance_tbl as (select from_site_code, to_site_code, actual_distance / 1000 as actual_distance
                      from ods_eagle_t_aging_distance_detail_f_d
                      where aging_type = 2),
     basic_tbl as (select b.waybill_no
                        , date(date_format(date(date_add(sign_time, interval -9 hour)), '%Y-%m-01')) as stats_month
                        , date(date_add(sign_time, interval -9 hour))                                as stats_date
                        , t2.area_property
                        , b.send_site_code
                        , b.dest_site_code
                        , dict_tbl.dict_name                                                         as order_source
                        , dest_site_info.site_name                                                   as dest_site_name
                        , dest_site_info.province_area_name                                          as dest_province_area_name
                        , dest_site_info.province_area_code                                          as dest_province_area_code
                        , dest_site_info.center_name                                                 as dest_center_name
                        , dest_site_info.center_code                                                 as dest_center_code
                        , send_site_info.site_name                                                   as send_site_name
                        , send_site_info.center_code                                                 as send_center_code
                        , send_site_info.center_name                                                 as send_center_name
                        , send_site_info.province_area_code                                          as send_province_area_code
                        , send_site_info.province_area_name                                          as send_province_area_name
                        , concat(case
                                     when actual_distance is null or actual_distance <= 150 then '0-150'
                                     when actual_distance <= 350 then '150-350'
                                     when actual_distance <= 550 then '350-550'
                                     when actual_distance <= 1200 then '550-1200'
                                     when actual_distance <= 1450 then '1200-1450'
                                     when actual_distance <= 1800 then '1450-1800'
                                     when actual_distance <= 3000 then '1800-3000'
                                     when actual_distance <= 3300 then '3000-3300'
                                     else '3300+'
                                     end, 'KM')                                                      as distance_range
                        , case
                              when actual_distance is null or actual_distance <= 150 then 100
                              when actual_distance <= 350 then 200
                              when actual_distance <= 550 then 300
                              when actual_distance <= 1200 then 400
                              when actual_distance <= 1450 then 500
                              when actual_distance <= 1800 then 600
                              when actual_distance <= 3000 then 700
                              when actual_distance <= 3300 then 800
                              else 900 end                                                           as distance_range_rn
                        , concat(case
                                     when b.calc_weight <= 35 THEN '0-35'
                                     WHEN b.calc_weight <= 70 THEN '35-70'
                                     WHEN b.calc_weight <= 300 THEN '70-300'
                                     WHEN b.calc_weight <= 500 THEN '300-500'
                                     WHEN b.calc_weight <= 800 then '500-800'
                                     when b.calc_weight <= 1500 then '800-1500'
                                     else '1500+'
                                     END, 'KG')                                                      as weight_range
                        , case
                              when b.calc_weight <= 35 THEN 100
                              WHEN b.calc_weight <= 70 THEN 200
                              WHEN b.calc_weight <= 300 THEN 300
                              WHEN b.calc_weight <= 500 THEN 400
                              WHEN b.calc_weight <= 800 then 500
                              when b.calc_weight <= 1500 then 600
                              else 700 end                                                           as weight_range_rn
                        , b.calc_weight
                        , b.total_piece
                        , t.transfer_out_fees
                        , t.dispatch_out_fees
                        , t.increment_out_fees
                        , 0                                                                          as transfer_in_fees
                        , t.dispatch_in_fees
                        , t.increment_in_fees
                        , order_source_id
						, product_type_code
						, product_type_name
                   from (SELECT *
                         FROM ods_tx_tx_waybill_info_i_30mi
                         WHERE create_time >= date_add('2026-11-09', interval -60 day)
                           AND create_time < date_add('2026-11-09', interval 20 day)) b
                            INNER JOIN (select *
                                        from t_bill_center_sum_total
                                        where accounts_time >= date_add('2026-11-09', interval -60 day)
                                          and accounts_time < date_add('2026-11-09', interval 15 day)) t
                                       on b.waybill_no = t.ewb_no
                            left join (select cast(dict_code as int) as dict_code, dict_name
                                       from tx_basic_dict_entry
                                       where type_code = 'platform_name') dict_tbl
                                      on order_source_id = dict_tbl.dict_code
                            left join distance_tbl r
                                      on b.send_center_code = r.from_site_code
                                          and b.dest_center_code = r.to_site_code
                            left join site_info_tbl dest_site_info
                                      on b.dest_site_code = dest_site_info.site_code
                            left join site_info_tbl send_site_info
                                      on b.send_site_code = send_site_info.site_code
                            left join
                        (SELECT area_property       -- 区域属性(10区内 20区域 30长途)
                              , send_operating_code-- 始发营运区
                              , dest_operating_code -- 目的营运区
                         FROM (SELECT area_property       -- 区域属性(10区内 20区域 30长途)
                                    , send_operating_code-- 始发营运区
                                    , dest_operating_code -- 目的营运区
                                    , ROW_NUMBER() OVER (PARTITION BY send_operating_code,dest_operating_code ORDER BY CREATE_time DESC ) AS rn
                               FROM t_basic_area_conf -- 区域属性配置表
                              ) t2_1
                         WHERE rn = 1) t2
                        ON send_site_info.province_area_code = t2.send_operating_code -- 始发营运区
                            AND dest_site_info.province_area_code = t2.dest_operating_code -- 目的营运区
                   where b.sign_time >= '2026-11-09 09:00:00'
                     and b.sign_time < date_add('2026-11-09 09:00:00', interval 1 day)
                     and b.data_valid = 10
                     and b.site_received >= 20
                     and b.data_valid <> 20),
     basic_agg_tbl as (select l.stats_date
                            , l.stats_month
                            , l.area_property
                            , l.send_site_code
                            , l.send_site_name
                            , l.dest_site_code
                            , order_source_id
                            , l.dest_site_name
                            , l.send_center_code
                            , l.send_center_name
                            , l.dest_center_code
                            , l.dest_center_name
                            , l.send_province_area_code
                            , l.send_province_area_name
                            , l.dest_province_area_code
                            , l.dest_province_area_name
                            , l.weight_range
                            , l.weight_range_rn
                            , l.distance_range
                            , l.distance_range_rn
                            , l.order_source
                            , sum(l.calc_weight)        as calc_weight
                            , count(1)                  as waybill_cnt
                            , sum(l.total_piece)        as total_piece
                            , sum(l.transfer_out_fees)  as total_transfer_out_fees
                            , sum(l.dispatch_out_fees)  as total_dispatch_out_fees
                            , sum(l.increment_out_fees) as total_increment_out_fees
                            , sum(l.transfer_in_fees)   as total_transfer_in_fees
                            , sum(l.dispatch_in_fees)   as total_dispatch_in_fees
                            , sum(l.increment_in_fees)  as total_increment_in_fees
						, product_type_code
						, product_type_name
                       from basic_tbl l
                       group by l.stats_date
                              , l.stats_month
                              , l.area_property
                              , l.send_site_code
                              , l.send_site_name
                              , l.order_source_id
                              , l.dest_site_code
                              , l.dest_site_name
                              , l.send_center_code
                              , l.send_center_name
                              , l.dest_center_code
                              , l.dest_center_name
                              , l.send_province_area_code
                              , l.send_province_area_name
                              , l.dest_province_area_code
                              , l.dest_province_area_name
                              , l.weight_range
                              , l.weight_range_rn
                              , l.distance_range
                              , l.distance_range_rn
                              , l.order_source
						, product_type_code
						, product_type_name),
     result_tbl as (select l.stats_date
                         , l.stats_month
                         , l.area_property
                         , send_site_info.site_name          as send_site_name
                         , send_site_info.join_ane_time      as send_join_ane_time
                         , send_site_info.first_site_name    as send_first_site_name
                         , send_site_info.province_area_name as send_province_area_name
                         , send_site_info.area_name          as send_area_name
                         , send_site_info.center_name        as send_center_name
                         , send_site_info.center_distance    as send_site2center_distance
                         , send_site_info.site_type          as send_site_type
                         , l.distance_range                  as distance_range
                         , l.order_source                    as order_source
                         , l.weight_range                    as weight_range
                         , l.calc_weight                     as calc_weight
                         , l.waybill_cnt                     as waybill_cnt
                         , l.total_piece                     as total_piece
                         , l.total_transfer_out_fees         as total_transfer_out_fees
                         , l.total_dispatch_out_fees         as total_dispatch_out_fees
                         , l.total_increment_out_fees        as total_increment_out_fees
                         , l.total_transfer_in_fees          as total_transfer_in_fees
                         , l.total_dispatch_in_fees          as total_dispatch_in_fees
                         , l.total_increment_in_fees         as total_increment_in_fees
                         , send_site_code
                         , dest_site_code
                         , order_source_id
                         , order_source                      as order_source_name
                         , send_center_code
                         , send_province_area_code
                         , dest_center_code
                         , dest_province_area_code
                         , dest_site_name
                         , dest_center_name
                         , dest_province_area_name
                         , l.distance_range_rn
                         , l.weight_range_rn
						, product_type_code
						, product_type_name
                    from basic_agg_tbl l
                             left join site_info_tbl as send_site_info
                                       on l.send_site_code = send_site_info.site_code),
     insert_tbl as (select send_site_code,
                           dest_site_code,
                           stats_date,
                           order_source_id,
                           order_source_name,
                           send_center_code,
                           send_province_area_code,
                           dest_center_code,
                           dest_province_area_code,
                           send_site_name,
                           send_center_name,
                           send_province_area_name,
                           dest_site_name,
                           dest_center_name,
                           dest_province_area_name,
                           area_property,
                           weight_range,
                           weight_range_rn,
                           distance_range,
                           distance_range_rn,
                           sum(calc_weight)              as calc_weight,
                           sum(waybill_cnt)              as waybill_cnt,
                           sum(total_piece)              as total_piece,
                           sum(total_transfer_out_fees)  as total_transfer_out_fees,
                           sum(total_dispatch_out_fees)  as total_dispatch_out_fees,
                           sum(total_increment_out_fees) as total_increment_out_fees,
                           sum(total_transfer_in_fees)   as total_transfer_in_fees,
                           sum(total_dispatch_in_fees)   as total_dispatch_in_fees,
                           sum(total_increment_in_fees)  as total_increment_in_fees,
                           stats_month,
                           now()                         as etl_time
						, product_type_code
						, product_type_name
                    from result_tbl
                    group by send_site_code,
                             dest_site_code,
                             stats_date,
                             order_source_id,
                             order_source_name,
                             send_center_code,
                             send_province_area_code,
                             dest_center_code,
                             dest_province_area_code,
                             send_site_name,
                             send_center_name,
                             send_province_area_name,
                             dest_site_name,
                             dest_center_name,
                             dest_province_area_name,
                             area_property,
                             weight_range,
                             weight_range_rn,
                             distance_range_rn,
                             distance_range,
                             stats_month,
                             etl_time
							 
						, product_type_code
						, product_type_name
							 )
select send_site_code,
       dest_site_code,
       stats_date,
       order_source_id,
       order_source_name,
       send_center_code,
       send_province_area_code,
       dest_center_code,
       dest_province_area_code,
       send_site_name,
       send_center_name,
       send_province_area_name,
       dest_site_name,
       dest_center_name,
       dest_province_area_name,
       area_property,
       weight_range,
       distance_range,
       calc_weight,
       waybill_cnt,
       total_piece,
       total_transfer_out_fees,
       total_dispatch_out_fees,
       total_increment_out_fees,
       total_transfer_in_fees,
       total_dispatch_in_fees,
       total_increment_in_fees,
       stats_month,
       etl_time,
       weight_range_rn,
       distance_range_rn,
       20 as data_type
       , product_type_code
       , product_type_name
from insert_tbl
where send_province_area_name is not null
;

-- [15] dws_oper_center_resign_cnt_i_d
-- 任务 dws_oper_center_resign_cnt_i_d | 任务实例 ID 4655596 | 任务定义编码 17878079798592
-- 工作流实例 离职率报表-5-20261110090001036 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682470
-- 执行结果 2026-11-10 09:00:09.054 +0800 | 日志第 2488 行 | JDBC 返回行数 6645
INSERT INTO dws_oper_center_resign_cnt_i_d (
                                        stats_date
                                        ,position_type
                                        ,org_operation_lvl
                                        ,org_operation_hid
                                        ,org_operation_name
                                        ,province_area_code
                                        ,province_area_name
                                        ,center_code
                                        ,center_name
                                        ,department_hid
                                        ,department_name
                                        ,team_hid
                                        ,team_name
                                        ,group_hid
                                        ,group_name
                                        ,resign_count
                                        ,employed_avg
                                    ) 
with dwd_region_position_status_count AS 
(
SELECT 
	 a.province_area_code
	,a.province_area_name
	,a.center_code
	,a.center_name
	,a.department_hid
	,a.department_name
	,a.team_hid
	,a.team_name
	,a.group_hid
	,a.group_name
	,a.position_name
	,sum(employed_count) employed_count
	,sum(resign_count) resign_count
FROM  dwd_oper_region_position_status_i_d a
where stats_date = '2026-11-09'
AND a.position_name in ('操作工岗', '电叉司机岗', '机叉司机岗', '电商分拣岗', '异常处理岗', '流水线操作工岗', '流水线分拣岗')
group by 
	 a.province_area_code
	,a.province_area_name
	,a.center_code
	,a.center_name
	,a.department_hid
	,a.department_name
	,a.team_hid
	,a.team_name
	,a.group_hid
	,a.group_name
	,a.position_name
) -- select * from dwd_region_position_status_count 
,
yc as 
(
SELECT 
	 bb.site_code as province_area_code
	,if(a.dept4_name = '广州电商分拨中心','广州电商管理区',a.company_name) as province_area_name 
	,b.site_code as center_code
	,a.dept4_name as center_name
	,CASE
		WHEN a.dept5_name LIKE '%部' THEN a.dept5_hid
		WHEN a.dept6_name LIKE '%部' THEN a.dept6_hid
		WHEN a.dept7_name LIKE '%部' THEN a.dept7_hid
		ELSE null
	END  as department_hid 		
	,CASE
		WHEN a.dept5_name LIKE '%部' THEN a.dept5_name
		WHEN a.dept6_name LIKE '%部' THEN a.dept6_name
		WHEN a.dept7_name LIKE '%部' THEN a.dept7_name
		ELSE null
	END  as department_name 
	,CASE
		WHEN a.dept5_name LIKE '%队' THEN a.dept5_hid
		WHEN a.dept6_name LIKE '%队' THEN a.dept6_hid
		WHEN a.dept7_name LIKE '%队' THEN a.dept7_hid
		ELSE null
	END  as team_hid
	,CASE
		WHEN a.dept5_name LIKE '%队' THEN a.dept5_name
		WHEN a.dept6_name LIKE '%队' THEN a.dept6_name
		WHEN a.dept7_name LIKE '%队' THEN a.dept7_name
		ELSE null
	END  as team_name
	,CASE
		WHEN a.dept5_name LIKE '%组' THEN a.dept5_hid
		WHEN a.dept6_name LIKE '%组' THEN a.dept6_hid
		WHEN a.dept7_name LIKE '%组' THEN a.dept7_hid
		WHEN a.dept8_name LIKE '%组' THEN a.dept8_hid
		ELSE null
	END  as group_hid
	,CASE
		WHEN a.dept5_name LIKE '%组' THEN a.dept5_name
		WHEN a.dept6_name LIKE '%组' THEN a.dept6_name
		WHEN a.dept7_name LIKE '%组' THEN a.dept7_name
		WHEN a.dept8_name LIKE '%组' THEN a.dept8_name
		ELSE null
	END  as group_name
	,a.position_name 
	,sum(if(a.labor_date <= STR_TO_DATE(DATE_FORMAT(a.data_date, '%Y-%m-01'), '%Y-%m-%d'),1,0)) as employed_early_count
FROM ods_ehr_tb_emp_adc_f_d a
inner join tx_bd_site_info b on a.dept4_name = b.site_name and b.site_type_code in ('900100','900200')
left join tx_bd_site_info bb on ((if(a.dept4_name = '广州电商分拨中心','广州电商管理区',a.company_name)) = bb.site_name and bb.site_type_code = '900800')
where a.data_date = STR_TO_DATE(DATE_FORMAT('2026-11-09', '%Y-%m-01'), '%Y-%m-%d') 
AND   a.position_name in ('操作工岗', '电叉司机岗', '机叉司机岗', '电商分拣岗', '异常处理岗', '流水线操作工岗', '流水线分拣岗')
and   a.status = 1
-- and   a.labor_date <= STR_TO_DATE(DATE_FORMAT(a.data_date, '%Y-%m-01'), '%Y-%m-%d')
GROUP BY 
	 bb.site_code 
	,if(a.dept4_name = '广州电商分拨中心','广州电商管理区',a.company_name) 
	,b.site_code 
	,a.dept4_name 
	,CASE
		WHEN a.dept5_name LIKE '%部' THEN a.dept5_hid
		WHEN a.dept6_name LIKE '%部' THEN a.dept6_hid
		WHEN a.dept7_name LIKE '%部' THEN a.dept7_hid
		ELSE null
	END   		
	,CASE
		WHEN a.dept5_name LIKE '%部' THEN a.dept5_name
		WHEN a.dept6_name LIKE '%部' THEN a.dept6_name
		WHEN a.dept7_name LIKE '%部' THEN a.dept7_name
		ELSE null
	END  
	,CASE
		WHEN a.dept5_name LIKE '%队' THEN a.dept5_hid
		WHEN a.dept6_name LIKE '%队' THEN a.dept6_hid
		WHEN a.dept7_name LIKE '%队' THEN a.dept7_hid
		ELSE null
	END  
	,CASE
		WHEN a.dept5_name LIKE '%队' THEN a.dept5_name
		WHEN a.dept6_name LIKE '%队' THEN a.dept6_name
		WHEN a.dept7_name LIKE '%队' THEN a.dept7_name
		ELSE null
	END  
	,CASE
		WHEN a.dept5_name LIKE '%组' THEN a.dept5_hid
		WHEN a.dept6_name LIKE '%组' THEN a.dept6_hid
		WHEN a.dept7_name LIKE '%组' THEN a.dept7_hid
		WHEN a.dept8_name LIKE '%组' THEN a.dept8_hid
		ELSE null
	END  
	,CASE
		WHEN a.dept5_name LIKE '%组' THEN a.dept5_name
		WHEN a.dept6_name LIKE '%组' THEN a.dept6_name
		WHEN a.dept7_name LIKE '%组' THEN a.dept7_name
		WHEN a.dept8_name LIKE '%组' THEN a.dept8_name
		ELSE null
	END  
	,a.position_name 
)  -- select * from yc;
,
z as 
(
SELECT 
	'2026-11-09' as stats_date
	,COALESCE(a.province_area_code,b.province_area_code) as province_area_code
	,COALESCE(a.province_area_name,b.province_area_name) as province_area_name
	,COALESCE(a.center_code,b.center_code) as center_code
	,COALESCE(a.center_name,b.center_name) as center_name
	,COALESCE(a.department_hid,b.department_hid) as department_hid
	,COALESCE(a.department_name,b.department_name) as department_name
	,COALESCE(a.team_hid,b.team_hid) as team_hid
	,COALESCE(a.team_name,b.team_name) as team_name
	,COALESCE(a.group_hid,b.group_hid) as group_hid
	,COALESCE(a.group_name,b.group_name) as group_name
	,COALESCE(a.position_name,b.position_name) as position_name
-- 	,sum(IF(a.position_name = '操作工岗',1,0)) '操作工'
-- 	,sum(IF(a.position_name in ('电叉司机岗', '机叉司机岗'),1,0)) '叉车岗'
-- 	,sum(IF(a.position_name in ('操作工岗', '电叉司机岗', '机叉司机岗', '电商分拣岗', '异常处理岗', '流水线操作工岗', '流水线分拣岗'),1,0)) '操作类'
	,COALESCE(a.resign_count,0) as resign_count
-- 	,sum( (b.employed_early_count + a.employed_count) / 2.0 ) as employed_avg
	,(COALESCE(b.employed_early_count,0) + COALESCE(a.employed_count,0)) / 2.0  as employed_avg
	-- ,sum(a.resign_count) / sum( (b.employed_early_count + a.employed_count) / 2.0 ) as turnover_rate
from dwd_region_position_status_count a
FULL join yc b 
		on CONCAT(a.province_area_name,a.center_name,a.department_name,a.team_name,a.group_name,a.position_name) 
		 = CONCAT(b.province_area_name,b.center_name,b.department_name,b.team_name,b.group_name,b.position_name)
)   -- select * from z where employed_avg <> employed_avg1
, x AS 
(
-- 组
SELECT 
	a.stats_date
	,a.group_hid as org_operation_hid
	,60 as org_operation_lvl
	,a.group_name as org_operation_name
	,a.province_area_code,a.province_area_name
	,a.center_code,a.center_name
	,a.department_hid,a.department_name
	,a.team_hid,a.team_name
	,a.group_hid,a.group_name 
	,a.position_name
	,sum(a.resign_count) resign_count
	,sum(a.employed_avg) employed_avg
from z a
where a.group_name is not null 
GROUP BY 
	a.stats_date
	,a.province_area_code,a.province_area_name
	,a.center_code,a.center_name
	,a.department_hid,a.department_name
	,a.team_hid,a.team_name
	,a.group_hid,a.group_name 
	,a.position_name
union all 
-- 队
SELECT 
	a.stats_date
	,a.team_hid as org_operation_hid
	,50 as org_operation_lvl
	,a.team_name as org_operation_name
	,a.province_area_code,a.province_area_name
	,a.center_code,a.center_name
	,a.department_hid,a.department_name
	,a.team_hid,a.team_name
	,NULL as group_hid,NULL as group_name 
	,a.position_name
	,sum(a.resign_count) resign_count
	,sum(a.employed_avg) employed_avg
from z a
where a.team_name is not null 
GROUP BY 
	a.stats_date
	,a.province_area_code,a.province_area_name
	,a.center_code,a.center_name
	,a.department_hid,a.department_name
	,a.team_hid,a.team_name
	,a.position_name
union all 
-- 部
SELECT 
	a.stats_date
	,a.department_hid as org_operation_hid
	,30 as org_operation_lvl	
	,a.department_name as org_operation_name
	,a.province_area_code,a.province_area_name
	,a.center_code,a.center_name
	,a.department_hid,a.department_name
	,NULL as team_hid,NULL as team_name
	,NULL as group_hid,NULL as group_name 
	,a.position_name
	,sum(a.resign_count) resign_count
	,sum(a.employed_avg) employed_avg
from z a
where a.department_name is not null 
GROUP BY 
	a.stats_date
	,a.province_area_code,a.province_area_name
	,a.center_code,a.center_name
	,a.department_hid,a.department_name
	,a.position_name
union all 
-- 分拨
SELECT 
	a.stats_date
	,a.center_code as org_operation_hid
	,20 as org_operation_lvl
	,a.center_name as org_operation_name
	,a.province_area_code,a.province_area_name
	,a.center_code,a.center_name
	,NULL as department_hid,NULL as department_name
	,NULL as team_hid,NULL as team_name
	,NULL as group_hid,NULL as group_name 
	,a.position_name
	,sum(a.resign_count) resign_count
	,sum(a.employed_avg) employed_avg
from z a
where a.center_name is not null 
GROUP BY 
	a.stats_date
	,a.province_area_code,a.province_area_name
	,a.center_code,a.center_name
	,a.position_name	
)
SELECT 
	stats_date
	,30 as position_type
	,org_operation_lvl
	,org_operation_hid
	,org_operation_name
	,province_area_code,province_area_name
	,center_code,center_name
	,department_hid,department_name
	,team_hid,team_name
	,group_hid,group_name 
	,sum(resign_count) resign_count
	,sum(employed_avg) employed_avg
from  x 
where position_name = '操作工岗'
group by 
	stats_date
	,org_operation_lvl
	,org_operation_hid
	,org_operation_name
	,province_area_code,province_area_name
	,center_code,center_name
	,department_hid,department_name
	,team_hid,team_name
	,group_hid,group_name 
UNION  ALL 
SELECT 
	stats_date
	,20 as position_type
	,org_operation_lvl
	,org_operation_hid
	,org_operation_name
	,province_area_code,province_area_name
	,center_code,center_name
	,department_hid,department_name
	,team_hid,team_name
	,group_hid,group_name 
	,sum(resign_count) resign_count
	,sum(employed_avg) employed_avg
from  x 
where position_name in ('电叉司机岗', '机叉司机岗')
group by 
	stats_date
	,org_operation_lvl
	,org_operation_hid
	,org_operation_name
	,province_area_code,province_area_name
	,center_code,center_name
	,department_hid,department_name
	,team_hid,team_name
	,group_hid,group_name 
UNION  ALL 
SELECT 
	stats_date
	,10 as position_type
	,org_operation_lvl
	,org_operation_hid
	,org_operation_name
	,province_area_code,province_area_name
	,center_code,center_name
	,department_hid,department_name
	,team_hid,team_name
	,group_hid,group_name 
	,sum(resign_count) resign_count
	,sum(employed_avg) employed_avg
from  x 
where position_name in ('操作工岗', '电叉司机岗', '机叉司机岗', '电商分拣岗', '异常处理岗', '流水线操作工岗', '流水线分拣岗')
group by 
	stats_date
	,org_operation_lvl
	,org_operation_hid
	,org_operation_name
	,province_area_code,province_area_name
	,center_code,center_name
	,department_hid,department_name
	,team_hid,team_name
	,group_hid,group_name 
;

-- [16] lbdb.dws_oper_waybill_retain_1d
-- 任务 dws_oper_waybill_retain_1d | 任务实例 ID 4655595 | 任务定义编码 18819121660736
-- 工作流实例 实时库存&分拨留仓-28-20261110090001036 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682475
-- 执行结果 2026-11-10 09:00:14.074 +0800 | 日志第 15283 行 | JDBC 返回行数 156125
INSERT INTO lbdb.dws_oper_waybill_retain_1d
WITH
site_base AS (
    SELECT site_code
         , site_name
         , first_site_code
         , send_center_code
         , province_area_code
         , province_area_name
    FROM tx_bd_site_info
    WHERE site_status IN (10, 20)
)
, site_name_map AS (
    SELECT site_code, site_name
    FROM tx_bd_site_info
)

, distance_class AS (
    SELECT site_code, center_code, distance
    FROM (
        SELECT site_code, center_code, distance
             , ROW_NUMBER() OVER (PARTITION BY site_code, center_code
                                  ORDER BY create_time DESC, distance) AS rn
        FROM tx_site_delivery_distance
    ) t
    WHERE t.rn = 1
)

, ec_dict AS (
    SELECT dict_code, dict_name
    FROM tx_basic_dict_entry
    WHERE type_code = 'platform_name'
)

, clock AS (
    SELECT scan_site_code, site_code, must_clock_time
    FROM (
        SELECT scan_site_code
             , site_code
             , must_clock_time
             , ROW_NUMBER() OVER (PARTITION BY site_code, scan_site_code
                                  ORDER BY classes DESC) AS rn
        FROM ods_tx_tx_site_clock_maintain_all
        WHERE clock_type = 20
          AND data_valid = 10
    ) t
    WHERE t.rn = 1
)

, path_team AS (
    SELECT scan_site_code, next_site_code, operate_team, operate_dept
    FROM (
        SELECT s.scan_site_code
             , s.next_site_code
             , s.operate_team
             , s.operate_dept
             , m.site_name
             , ROW_NUMBER() OVER (PARTITION BY s.scan_site_code, s.next_site_code
                                  ORDER BY s.scan_time DESC) AS rn
        FROM tx_oper_center_send s
       
        INNER JOIN site_base m
                ON s.scan_site_code = m.site_code
        WHERE s.create_time >= '2026-10-26 09:00:00'
          AND s.create_time <  '2026-11-10 09:00:00'
          -- 原：scan_time >= DATE_ADD(stat_time,-3 day) 12:00 且 < stat_time 12:00
         and  scan_time>=CAST(CONCAT(DATE(DATE_ADD('2026-11-10 09:00:00', INTERVAL -3 day)),' ','12:00:00' ) as datetime) and scan_time<CAST(CONCAT(DATE('2026-11-10 09:00:00'),' ','12:00:00' ) as datetime)
          AND (   s.operate_team LIKE '%装卸%'
               OR s.operate_team LIKE '%操作%'
               OR s.operate_team LIKE '%叉车%')
          AND s.next_arrive_flag IN (10, 20, 40)
          AND m.site_name NOT LIKE '%集散站'
         
    ) t
    WHERE t.rn = 1
      
      AND REPLACE(t.site_name, '分拨中心', '') NOT LIKE '%中心'
)


, temp1 AS (
    SELECT
         t1.waybill_no
       , t1.sub_waybill_no
       , t1.send_site_code
       , t1.instock_time
       , t1.outstock_time
       , t2.site_name                  AS send_site_name
       , t2.first_site_code            AS send_first_site_code
       , t3.site_name                  AS dest_site_name
       , t3.first_site_code            AS dest_first_site_code
       , t3.send_center_code           AS dest_center_code
      
       , dc3.site_name                 AS dest_center_name
       , t3.province_area_code         AS dest_province_area_code
       , t3.province_area_name         AS dest_province_area_name
       , t1.dest_site_code
       , t1.delivery_receipt_no
       , t1.out_delivery_receipt_no
       , t1.center_site_code           AS instock_center_code
       , t4.site_name                  AS instock_center_name
       , t4.province_area_code         AS instock_province_area_code
       , t4.province_area_name         AS instock_province_area_name
       , t1.next_site_code
       , t5.site_name                  AS next_site_name
       , t5.first_site_code            AS next_first_site_code
       , t5.province_area_code         AS next_province_area_code
       , t5.province_area_name         AS next_province_area_name
       , CASE WHEN t1.goods_flow_type = '10'                     THEN '始发'
              WHEN t1.goods_flow_type IN ('20', '40')            THEN '中转'
              WHEN t1.goods_flow_type = '30'                     THEN '末端'
         END                          AS stock_node_type
       , IF(t1.load_scan_time = '2000-01-01 00:00:00', NULL, t1.load_scan_time) AS load_scan_time
       , t1.sort_time
       , t1.goods_weight
       , t1.sub_calc_weight
       , t1.calc_weight
       , t1.goods_volume
       , t1.total_piece
       , t1.product_type_code
       , t1.product_type_name
       , w.order_source_id            AS ec_id
       , d.dict_name                  AS ec_name
       , CASE CAST(get_json_string(PARSE_JSON(t1.business_range), '$.lengthOfTime') AS INT)
              WHEN 10 THEN '产品时效'
              WHEN 20 THEN '次日达'
              WHEN 30 THEN '隔日达'
              ELSE ''
         END                          AS validity_require
       , t1.goods_name
       , t1.package_type
       , CASE t1.data_status
              WHEN 10 THEN '正常库区'
              WHEN 20 THEN '异常库区'
              WHEN 30 THEN '自提库区'
              WHEN 40 THEN '派送库区'
              ELSE ''
         END                          AS zone_type_name
       , t1.out_confirm_time
       , c.must_clock_time            AS last_rule_departure_time
       , t1.out_mode
       , t1.delivery_center_code
       , t6.site_name                 AS delivery_center_name
      
       , sf2.site_name                AS send_first_site_name
       , sf3.site_name                AS dest_first_site_name
    FROM tx_oper_stock_realtime_ext t1
    INNER JOIN tx_bd_organize_build tbob
            ON t1.center_site_code = tbob.site_code
           AND tbob.structure_code   = 20
           AND tbob.data_valid       = 10
           AND tbob.parent_code      = '999999'
           AND tbob.site_type_code IN ('900100', '900200')
    LEFT JOIN site_base t2 ON t1.send_site_code     = t2.site_code
    LEFT JOIN site_base t3 ON t1.dest_site_code     = t3.site_code
    LEFT JOIN site_base t4 ON t1.center_site_code   = t4.site_code
    LEFT JOIN site_base t5 ON t1.next_site_code     = t5.site_code
    LEFT JOIN site_base t6 ON t1.delivery_center_code = t6.site_code
    LEFT JOIN site_name_map sf2 ON t2.first_site_code = sf2.site_code
    LEFT JOIN site_name_map sf3 ON t3.first_site_code = sf3.site_code
    LEFT JOIN site_name_map dc3 ON t3.send_center_code = dc3.site_code
    LEFT JOIN ods_tx_tx_waybill_info_i_30mi w
           ON t1.waybill_no  = w.waybill_no
          AND w.create_time  >= '2026-10-10 09:00:00'
          AND w.create_time  <= '2026-11-10 09:00:00'
          AND w.total_weight > 0
        
   
    LEFT JOIN ec_dict d ON CAST(w.order_source_id AS VARCHAR(32)) = d.dict_code
    LEFT JOIN clock       c  ON t1.center_site_code = c.scan_site_code
                            AND t1.next_site_code   = c.site_code
    WHERE t1.create_time  >= '2026-10-10 09:00:00'
      AND t1.create_time  <  '2026-11-10 09:00:00'
      AND t1.instock_time >= '2026-10-26 09:00:00'
      AND t1.instock_time <  '2026-11-10 09:00:00'
      AND t1.outstock_time = '2000-01-01 00:00:00'
      AND t1.data_valid    = 10
      AND t1.is_cdc_delete = 0
   
)


, temp2 AS (
    SELECT waybill_no
         , instock_center_code
         , MAX(instock_center_name)                 AS instock_center_name
         , MAX(instock_province_area_code)          AS instock_province_area_code
         , MAX(instock_province_area_name)          AS instock_province_area_name
         , MAX(next_site_code)                      AS next_site_code
         , MAX(next_site_name)                      AS next_site_name
         , MAX(next_first_site_code)                AS next_first_site_code
         , MAX(next_province_area_code)             AS next_province_area_code
         , MAX(next_province_area_name)             AS next_province_area_name
         , MIN(instock_time)                        AS instock_time
         , MIN(outstock_time)                       AS outstock_time
         , MIN(out_mode)                            AS out_mode
         , MIN(CASE WHEN sort_time = '2000-01-01 00:00:00' THEN NULL ELSE sort_time END) AS sort_time
         , MIN(load_scan_time)                      AS load_scan_time
         , MIN(out_confirm_time)                    AS out_confirm_time
         , MAX(last_rule_departure_time)            AS last_rule_departure_time
         , MAX(product_type_code)                   AS product_type_code
         , MAX(product_type_name)                   AS product_type_name
         , COUNT(DISTINCT sub_waybill_no)           AS stock_qty
         , SUM(sub_calc_weight)                     AS stock_calc_weight
         , MAX(total_piece)                         AS order_entry_qty
         , MAX(goods_weight)                        AS goods_weight
         , MAX(calc_weight)                         AS calc_weight
         , ROUND(MAX(goods_volume), 2)              AS goods_volume
       
         , MIN_BY(delivery_receipt_no,  instock_time) AS instock_delivery_receipt_no
         , MAX(dest_site_code)                      AS dest_site_code
         , MAX(dest_site_name)                      AS dest_site_name
         , MAX(dest_first_site_code)                AS dest_first_site_code
         , MAX(dest_first_site_name)                AS dest_first_site_name
         , MAX(dest_center_code)                    AS dest_center_code
         , MAX(dest_center_name)                    AS dest_center_name
         , MAX(dest_province_area_code)             AS dest_province_area_code
         , MAX(dest_province_area_name)             AS dest_province_area_name
         , MAX(send_site_code)                      AS send_site_code
         , MAX(send_site_name)                      AS send_site_name
         , MAX(send_first_site_code)                AS send_first_site_code
         , MAX(send_first_site_name)                AS send_first_site_name

         , MAX_BY(delivery_center_code, instock_time) AS delivery_center_code
         , MAX_BY(delivery_center_name, instock_time) AS delivery_center_name
         , MAX(stock_node_type)                     AS stock_node_type
         , MAX(ec_id)                               AS ec_id
         , MAX(ec_name)                             AS ec_name
         , MAX(validity_require)                    AS validity_require
         , MAX(goods_name)                          AS goods_name
         , MAX(package_type)                        AS package_type
         , MAX(zone_type_name)                      AS zone_type_name
    FROM temp1
    GROUP BY waybill_no, instock_center_code
)

SELECT
     '2026-11-10 09:00:00'                     AS stats_time
   , waybill_no
   , instock_center_code
   , instock_center_name
   , instock_province_area_code
   , instock_province_area_name
   , next_site_code
   , next_site_name
   , next_province_area_code
   , next_province_area_name
   , IF(stock_node_type <> '末端' AND class_area IS NULL, '/', class_area)      AS class_area
   , IF(stock_node_type <> '末端' AND class_area_name IS NULL, '/', class_area_name) AS class_area_name
   , IF(stock_node_type = '末端' AND t3.distance IS NULL, '/', t3.distance)      AS distance
   , CASE WHEN stock_node_type = '末端' THEN
              CASE WHEN t3.distance <=  30 THEN '0-30km'
                   WHEN t3.distance <=  50 THEN '30-50km'
                   WHEN t3.distance <=  80 THEN '50-80km'
                   WHEN t3.distance <= 120 THEN '80-120km'
                   WHEN t3.distance <= 150 THEN '120-150km'
                   WHEN t3.distance <= 200 THEN '150-200km'
                   WHEN t3.distance <= 300 THEN '200-300km'
                   WHEN t3.distance <= 500 THEN '300-500km'
                   WHEN t3.distance >  500 THEN '500km以上'
                   ELSE '其他'
              END
          ELSE NULL
     END                                        AS kilometer_range
   , instock_time
   , stock_qty
   , stock_calc_weight
   , goods_weight
   , calc_weight
   , site_type_name
   , stock_node_type
   , goods_volume
   , send_site_code
   , send_site_name
   , send_first_site_code
   , send_first_site_name
   , delivery_center_code
   , delivery_center_name
   , dest_site_code
   , dest_site_name
   , dest_first_site_code
   , dest_first_site_name
   , dest_center_code
   , dest_center_name
   , dest_province_area_code
   , dest_province_area_name
   , product_type_code
   , product_type_name
   , ec_id
   , ec_name
   , validity_require
   , goods_name
   , package_type
   , zone_type_name
   , instock_delivery_receipt_no
   , sort_time                                  AS sort_scan_time
   , load_scan_time
   , out_confirm_time
   , stock_status
   , retain_duration
   , retain_time_slot
   , weight_range_3300
   , loader_team
   , loader_dept
   , departure_time
   , pre_departure_90min
  
   , CASE WHEN t3.stock_node_type <> '末端' THEN 0
          WHEN t3.instock_time <= t3.departure_time THEN 1
          ELSE 0
     END                                        AS detained_pre_departure
   , CASE WHEN t3.stock_node_type <> '末端' THEN 0
          WHEN t3.instock_time <= t3.pre_departure_90min THEN 1
          ELSE 0
     END                                        AS detained_pre_departure_90min
   , CASE WHEN t3.stock_node_type <> '末端' THEN 0
          WHEN t3.instock_time <= t3.pre_departure_90min
           AND t3.sort_time   <= t3.departure_time THEN 1
          ELSE 0
     END                                        AS sort_pre_departure
   , CURRENT_TIMESTAMP                          AS etl_time
FROM (
    SELECT
         t1.waybill_no
       , t1.instock_center_code
       , t1.instock_center_name
       , t1.instock_province_area_code
       , t1.instock_province_area_name
       , t1.next_site_code
       , t1.next_site_name
       -- 下一站营运区：末端且目的营运区相同则取目的营运区；始发且下一站为空则取入库营运区
       , CASE WHEN t1.stock_node_type = '末端'
               AND t1.instock_province_area_code = t1.dest_province_area_code
              THEN t1.dest_province_area_code
              WHEN t1.stock_node_type = '始发'
               AND t1.next_province_area_code IS NULL
              THEN t1.instock_province_area_code
              ELSE t1.next_province_area_code
         END                                   AS next_province_area_code
       , CASE WHEN t1.stock_node_type = '末端'
               AND t1.instock_province_area_code = t1.dest_province_area_code
              THEN t1.dest_province_area_name
              WHEN t1.stock_node_type = '始发'
               AND t1.next_province_area_code IS NULL
              THEN t1.instock_province_area_name
              ELSE t1.next_province_area_name
         END                                   AS next_province_area_name
       -- 类区（干线才有）
       , CASE WHEN t1.stock_node_type = '末端' THEN NULL
              ELSE LEFT(t5.class_area_name, 1)
         END                                   AS class_area
       , CASE WHEN t1.stock_node_type = '末端' THEN NULL
              ELSE t5.class_area_name
         END                                   AS class_area_name
       -- 距离（末端才有；两级降级：目的网点 → 目的所属一级网点）
       , CASE WHEN t1.stock_node_type = '末端'
              THEN COALESCE(t2d.distance, t3d.distance)
              ELSE CAST(NULL AS DECIMAL(16, 4))
         END                                   AS distance
       , t1.instock_time
       , IF(t1.stock_qty >= t1.order_entry_qty, t1.order_entry_qty, t1.stock_qty) AS stock_qty
       , t1.stock_calc_weight
       , t1.goods_weight
       , t1.calc_weight
       , CASE WHEN t1.instock_center_name LIKE '%分拨中心' THEN '分拨' ELSE '外包' END AS site_type_name
       , t1.stock_node_type
       , t1.goods_volume
       , t1.send_site_code
       , t1.send_site_name
       , t1.send_first_site_code
       , t1.send_first_site_name
       , t1.delivery_center_code
       , t1.delivery_center_name
       , t1.dest_site_code
       , t1.dest_site_name
       , t1.dest_first_site_code
       , t1.dest_first_site_name
       , t1.dest_center_code
       , t1.dest_center_name
       , t1.dest_province_area_code
       , t1.dest_province_area_name
       , t1.product_type_code
       , t1.product_type_name
       , t1.ec_id
       , t1.ec_name
       , t1.validity_require
       , t1.goods_name
       , t1.package_type
       , t1.zone_type_name
       , t1.instock_delivery_receipt_no
       , t1.sort_time
       , t1.load_scan_time
       , t1.out_confirm_time
      
       , CASE
             WHEN (t1.outstock_time = '2000-01-01 00:00:00' OR t1.outstock_time > '2026-11-10 09:00:00')
              AND (t1.sort_time IS NULL OR t1.sort_time > '2026-11-10 09:00:00')
             THEN '已卸未分'
             WHEN (t1.outstock_time = '2000-01-01 00:00:00' OR t1.outstock_time > '2026-11-10 09:00:00')
              AND t1.sort_time IS NOT NULL AND t1.sort_time <= '2026-11-10 09:00:00'
              AND (t1.load_scan_time IS NULL OR t1.load_scan_time > '2026-11-10 09:00:00')
             THEN '已分未装'
             WHEN (t1.outstock_time = '2000-01-01 00:00:00' OR t1.outstock_time > '2026-11-10 09:00:00')
              AND t1.load_scan_time IS NOT NULL AND t1.load_scan_time <= '2026-11-10 09:00:00'
              AND (t1.out_confirm_time = '2000-01-01 00:00:00' OR t1.out_confirm_time > '2026-11-10 09:00:00')
             THEN '已装未发'
             WHEN (t1.outstock_time <> '2000-01-01 00:00:00' OR t1.outstock_time < '2026-11-10 09:00:00')
              AND t1.load_scan_time IS NOT NULL AND t1.load_scan_time <= '2026-11-10 09:00:00'
              AND (t1.out_confirm_time = '2000-01-01 00:00:00' OR t1.out_confirm_time > '2026-11-10 09:00:00')
              AND t1.out_mode = 10
             THEN '已装未发'
             ELSE '已装未发'
         END                                   AS stock_status
       , ROUND((UNIX_TIMESTAMP('2026-11-10 09:00:00') - UNIX_TIMESTAMP(t1.instock_time)) / 3600, 4) AS retain_duration
       , CASE
             WHEN ROUND((UNIX_TIMESTAMP('2026-11-10 09:00:00') - UNIX_TIMESTAMP(t1.instock_time)) / 3600, 4) <= 6  THEN '0~6h'
             WHEN ROUND((UNIX_TIMESTAMP('2026-11-10 09:00:00') - UNIX_TIMESTAMP(t1.instock_time)) / 3600, 4) <= 24 THEN '6~24h'
             WHEN ROUND((UNIX_TIMESTAMP('2026-11-10 09:00:00') - UNIX_TIMESTAMP(t1.instock_time)) / 3600, 4) <= 48 THEN '24~48h'
             ELSE '>48h'
         END                                   AS retain_time_slot
       , IF(t1.calc_weight <= 300, '0~300kg', '>300kg') AS weight_range_3300
       , t2.operate_team                        AS loader_team
       , IF(t2.operate_dept = 0, NULL, t2.operate_dept) AS loader_dept
       , IF(t1.last_rule_departure_time IS NOT NULL
           , CAST(CONCAT(DATE('2026-11-10 09:00:00'), ' ', t1.last_rule_departure_time) AS DATETIME)
           , CAST(CONCAT(DATE('2026-11-10 09:00:00'), ' ', '18:00:00') AS DATETIME)
         )                                     AS departure_time
       , IF(t1.last_rule_departure_time IS NOT NULL
           , DATE_ADD(CAST(CONCAT(DATE('2026-11-10 09:00:00'), ' ', t1.last_rule_departure_time) AS DATETIME),
                      INTERVAL -90 MINUTE)
           , CAST(CONCAT(DATE('2026-11-10 09:00:00'), ' ', '16:30:00') AS DATETIME)
         )                                     AS pre_departure_90min
    FROM temp2 t1
    LEFT JOIN path_team t2
           ON t1.instock_center_code = t2.scan_site_code
          AND t1.next_site_code      = t2.next_site_code
    LEFT JOIN distance_class t2d
           ON t1.next_site_code        = t2d.site_code
          AND t1.instock_center_code  = t2d.center_code
          AND t1.stock_node_type      = '末端'
    LEFT JOIN distance_class t3d
           ON t1.next_first_site_code  = t3d.site_code
          AND t1.instock_center_code  = t3d.center_code
          AND t1.stock_node_type      = '末端'
    LEFT JOIN ods_eagle_t_basic_class_area_conf_new t5
           ON t1.instock_center_code = t5.send_allocate_code
          AND t1.next_site_code      = t5.dest_allocate_code
          AND t1.stock_node_type    <> '末端'
) t3
;

-- [17] lbdb.dwd_fin_wayb_sign_site_income_cost_i_m
-- 任务 dwd_dwd_fin_wayb_sign_site_income_cost_i_m | 任务实例 ID 4655587 | 任务定义编码 18433085625792
-- 工作流实例 3300特殊区域加收检视报表-32-20261110090001036 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682469
-- 执行结果 2026-11-10 09:00:09.958 +0800 | 日志第 465 行 | JDBC 返回行数 6028617
insert into lbdb.dwd_fin_wayb_sign_site_income_cost_i_m
SELECT
    DATE_FORMAT('2026-11-09', '%Y-%m-01') AS stats_date,
    waybill_no,
    sign_site_code,
    sign_time1 AS sign_date,
    calc_weight,
    fee_1412,
    fee_1403_standard,
    fee_1403,
    fee_1402_standard,
    fee_1402,
    fee_site_1405,
    fee_site_1406,
    fee_1420,
    fee_1421,
    fee_15_standard,
    fee_6_standard,
    fee_1_1_version,
    fee_1_2_version,
    fee_1_trunk,
    fee_1_operate,
    fee_1_shortbarge,
    sign_site_name,
    c.first_site_code,
    d.site_name AS first_site_name,
    scan_source_id,
    NOW() AS etl_time,
    send_site_name,
    ec_id,
    varchar_standby_2
FROM (
    SELECT
        a.*,
        CASE
            WHEN HOUR(sign_time) >= 9
            THEN DATE(sign_time)
            ELSE DATE_FORMAT(DATE(sign_time) - INTERVAL 1 DAY, '%Y-%m-%d')
        END AS sign_time1
    FROM bd_bill_center_send a
    WHERE
        a.sign_time >= DATE_FORMAT('2026-11-09', '%Y-%m-01')
        AND DATE_FORMAT(a.sign_time, '%Y-%m-%d') <= '2026-11-09'
        AND a.accounts_time >= DATE_FORMAT('2026-11-09' - INTERVAL 6 MONTH, '%Y-%m-%d %H:%i')
) b
LEFT JOIN tx_bd_site_info c
    ON b.sign_site_id = c.site_id  -- 所属网点
LEFT JOIN (
    SELECT
        site_id,
        site_code,
        site_name
    FROM tx_bd_site_info
) d
    ON c.first_site_code = d.site_code;

-- [18] dws_oper_org_send_arrival_diff_bf9_1d
-- 任务 dws_oper_org_send_arrival_diff_bf9_1d | 任务实例 ID 4655586 | 任务定义编码 21716365970784
-- 工作流实例 网点开单-卸车少货差异报表-44-20261110090001036 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682471
-- 执行结果 2026-11-10 09:00:07.283 +0800 | 日志第 4182 行 | JDBC 返回行数 34665
INSERT INTO dws_oper_org_send_arrival_diff_bf9_1d 
(
stats_date
,org_brnch_lvl
,org_brnch_code
,org_brnch_name
,send_site_code
,send_site_name
,send_first_site_code
,send_first_site_name
,send_area_code
,send_area_name
,delivery_center_code
,delivery_center_name
,send_province_area_code
,send_province_area_name
,send_ticket
,send_calc_weight
,center_arrival_ticket
,center_arrival_calc_weight
,send_arrival_weight_diff
)
with
tmp_tx_oper_stock_realtime_ext
as
(
select
	A.*
FROM 
	(
	select  
		waybill_no,calc_weight,instock_time,center_site_code
		,row_number() over(partition by center_site_code,waybill_no order by instock_time asc) rn
	from tx_oper_stock_realtime_ext
	where create_time >= '2026-11-09' - INTERVAL 2 DAY and create_time < '2026-11-09' + INTERVAL 2 DAY
	and instock_time < DATE_ADD(concat('2026-11-09', ' 09:00:00'),INTERVAL 1 DAY)
	and data_valid = 10 and is_cdc_delete = 0  -- and data_status = 10
	) A
where rn = 1
)
,
tmp_tx_waybill_info_ext
AS 
(
select
	a.send_site_code
	,s1.site_name as send_site_name
	,a.send_first_site_code
	,s2.site_name as send_first_site_name
	,s1.area_code as send_area_code
	,s1.area_name as send_area_name
	,a.send_center_code as delivery_center_code
	,s3.site_name as delivery_center_name
	,s1.province_area_code as send_province_area_code
	,s1.province_area_name as send_province_area_name
	,count(1) as send_ticket
	,(sum(ifnull(a.calc_weight,0.0))) / 1000 as send_calc_weight
	,sum(if(b.waybill_no is null, 0 ,1 )) as center_arrival_ticket
	,(sum(ifnull(b.calc_weight,0.0))) / 1000 as center_arrival_calc_weight
from tx_waybill_info_ext a
INNER JOIN tx_bd_site_info s1  ON a.send_site_code = s1.site_code
INNER JOIN tx_bd_site_info s2  ON a.send_first_site_code = s2.site_code
INNER JOIN tx_bd_site_info s3  ON a.send_center_code = s3.site_code
left join tmp_tx_oper_stock_realtime_ext b on a.waybill_no = b.waybill_no and a.send_center_code = b.center_site_code
where a.send_date_9to9_date >= '2026-11-09' - INTERVAL 2 DAY and a.send_date_9to9_date <= '2026-11-09' + INTERVAL 2 DAY
  and a.send_date >= concat('2026-11-09', ' 09:00:00') 
  and a.send_date < DATE_ADD(concat('2026-11-09', ' 09:00:00'),INTERVAL 1 DAY)
  and a.data_valid = 10 and a.site_received >= 20
group by 
	a.send_site_code
	,s1.site_name
	,a.send_first_site_code
	,s2.site_name
	,s1.area_code
	,s1.area_name
	,a.send_center_code
	,s3.site_name
	,s1.province_area_code
	,s1.province_area_name
) -- select * from tmp_tx_waybill_info_ext
SELECT 
	'2026-11-09' as stats_date
	,8 as org_brnch_lvl
	,send_site_code as org_brnch_code
	,send_site_name as org_brnch_name
	,send_site_code
	,send_site_name
	,send_first_site_code
	,send_first_site_name
	,send_area_code
	,send_area_name
	,delivery_center_code
	,delivery_center_name
	,send_province_area_code
	,send_province_area_name
	,send_ticket
	,send_calc_weight
	,center_arrival_ticket
	,center_arrival_calc_weight
	,send_calc_weight - center_arrival_calc_weight as send_arrival_weight_diff 
from tmp_tx_waybill_info_ext
union all
SELECT 
	'2026-11-09' as stats_date
	,7 as org_brnch_lvl
	,send_first_site_code as org_brnch_code
	,send_first_site_name as org_brnch_name
	,NULL as send_site_code
	,NULL as send_site_name
	,send_first_site_code
	,send_first_site_name
	,send_area_code
	,send_area_name
	,delivery_center_code
	,delivery_center_name
	,send_province_area_code
	,send_province_area_name
	,sum(send_ticket) as send_ticket
	,sum(send_calc_weight) as send_calc_weight
	,sum(center_arrival_ticket) as center_arrival_ticket
	,sum(center_arrival_calc_weight) as center_arrival_calc_weight
	,(sum(send_calc_weight) - sum(center_arrival_calc_weight)) as send_arrival_weight_diff 
from tmp_tx_waybill_info_ext
group by 
	send_first_site_code
	,send_first_site_name
	,send_area_code
	,send_area_name
	,delivery_center_code
	,delivery_center_name
	,send_province_area_code
	,send_province_area_name
union all
SELECT 
	'2026-11-09' as stats_date
	,6 as org_brnch_lvl
	,send_area_code as org_brnch_code
	,send_area_name as org_brnch_name
	,NULL as send_site_code
	,NULL as send_site_name
	,NULL as send_first_site_code
	,NULL as send_first_site_name
	,send_area_code
	,send_area_name
	,delivery_center_code
	,delivery_center_name
	,send_province_area_code
	,send_province_area_name
	,sum(send_ticket) as send_ticket
	,sum(send_calc_weight) as send_calc_weight
	,sum(center_arrival_ticket) as center_arrival_ticket
	,sum(center_arrival_calc_weight) as center_arrival_calc_weight
	,(sum(send_calc_weight) - sum(center_arrival_calc_weight)) as send_arrival_weight_diff 
from tmp_tx_waybill_info_ext
group by 
	send_area_code
	,send_area_name
	,delivery_center_code
	,delivery_center_name
	,send_province_area_code
	,send_province_area_name
union all
SELECT 
	'2026-11-09' as stats_date
	,5 as org_brnch_lvl
	,delivery_center_code as org_brnch_code
	,delivery_center_name as org_brnch_name
	,NULL as send_site_code
	,NULL as send_site_name
	,NULL as send_first_site_code
	,NULL as send_first_site_name
	,NULL as send_area_code
	,NULL as send_area_name
	,delivery_center_code
	,delivery_center_name
	,s.province_area_code as send_province_area_code
	,s.province_area_name as send_province_area_name
	,send_ticket
	,send_calc_weight
	,center_arrival_ticket
	,center_arrival_calc_weight
	,send_arrival_weight_diff 
from
	(
	SELECT 
		delivery_center_code
		,delivery_center_name
		,sum(send_ticket) as send_ticket
		,sum(send_calc_weight) as send_calc_weight
		,sum(center_arrival_ticket) as center_arrival_ticket
		,sum(center_arrival_calc_weight) as center_arrival_calc_weight
		,(sum(send_calc_weight) - sum(center_arrival_calc_weight)) as send_arrival_weight_diff 
	from tmp_tx_waybill_info_ext
	group by 
		delivery_center_code
		,delivery_center_name
	)a
INNER JOIN tx_bd_site_info s  ON a.delivery_center_code = s.site_code
union all
SELECT 
	'2026-11-09' as stats_date
	,3 as org_brnch_lvl
	,send_province_area_code as org_brnch_code
	,send_province_area_name as org_brnch_name
	,NULL as send_site_code
	,NULL as send_site_name
	,NULL as send_first_site_code
	,NULL as send_first_site_name
	,NULL as send_area_code
	,NULL as send_area_name
	,NULL as delivery_center_code
	,NULL as delivery_center_name
	,send_province_area_code
	,send_province_area_name
	,sum(send_ticket) as send_ticket
	,sum(send_calc_weight) as send_calc_weight
	,sum(center_arrival_ticket) as center_arrival_ticket
	,sum(center_arrival_calc_weight) as center_arrival_calc_weight
	,(sum(send_calc_weight) - sum(center_arrival_calc_weight)) as send_arrival_weight_diff 
from tmp_tx_waybill_info_ext
group by 
	send_province_area_code
	,send_province_area_name
union all
SELECT 
	'2026-11-09' as stats_date
	,1 as org_brnch_lvl
	,'999999' as org_brnch_code
	,'安能物流' as org_brnch_name
	,NULL as send_site_code
	,NULL as send_site_name
	,NULL as send_first_site_code
	,NULL as send_first_site_name
	,NULL as send_area_code
	,NULL as send_area_name
	,NULL as delivery_center_code
	,NULL as delivery_center_name
	,NULL as send_province_area_code
	,NULL as send_province_area_name
	,sum(send_ticket) as send_ticket
	,sum(send_calc_weight) as send_calc_weight
	,sum(center_arrival_ticket) as center_arrival_ticket
	,sum(center_arrival_calc_weight) as center_arrival_calc_weight
	,(sum(send_calc_weight) - sum(center_arrival_calc_weight)) as send_arrival_weight_diff 
from tmp_tx_waybill_info_ext;

-- [19] dwd_oper_send_arrival_diff_af9_i_d
-- 任务 dwd_oper_send_arrival_diff_af9_i_d | 任务实例 ID 4655585 | 任务定义编码 21716440975712
-- 工作流实例 网点开单-卸车少货差异报表-44-20261110090001036 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682471
-- 执行结果 2026-11-10 09:00:06.434 +0800 | 日志第 725 行 | JDBC 返回行数 90778
INSERT INTO dwd_oper_send_arrival_diff_af9_i_d 
(
stats_date
,send_time
,waybill_no
,send_site_code
,send_site_name
,send_first_site_code
,send_first_site_name
,send_area_code
,send_area_name
,delivery_center_code
,delivery_center_name
,send_province_area_code
,send_province_area_name
)
with
tmp_tx_oper_stock_realtime_ext
as
(
select
	A.*
FROM 
	(
	select  
		waybill_no,center_site_code  -- ,calc_weight,instock_time
		,row_number() over(partition by center_site_code,waybill_no order by instock_time asc) rn
	from tx_oper_stock_realtime_ext
	where create_time >= '2026-11-09' - INTERVAL 2 DAY and create_time < '2026-11-09' + INTERVAL 2 DAY
	and instock_time < DATE_ADD(concat('2026-11-09', ' 09:00:00'),INTERVAL 1 DAY)
	and data_valid = 10 and is_cdc_delete = 0  -- and data_status = 10
	) A
where rn = 1
)
select
	'2026-11-09' as stats_date
	,a.send_date as send_time
	,a.waybill_no
	,a.send_site_code
	,s1.site_name as send_site_name
	,a.send_first_site_code
	,s2.site_name as send_first_site_name
	,s1.area_code as send_area_code
	,s1.area_name as send_area_name
	,a.send_center_code as delivery_center_code
	,s3.site_name as delivery_center_name
	,s1.province_area_code as send_province_area_code
	,s1.province_area_name as send_province_area_name
from tx_waybill_info_ext a
INNER JOIN tx_bd_site_info s1  ON a.send_site_code = s1.site_code
INNER JOIN tx_bd_site_info s2  ON a.send_first_site_code = s2.site_code
INNER JOIN tx_bd_site_info s3  ON a.send_center_code = s3.site_code
left JOIN tmp_tx_oper_stock_realtime_ext b on a.waybill_no = b.waybill_no and a.send_center_code = b.center_site_code 
where a.send_date_9to9_date >= '2026-11-09' - INTERVAL 2 DAY and a.send_date_9to9_date <= '2026-11-09' + INTERVAL 2 DAY
  and a.send_date >= concat('2026-11-09', ' 09:00:00') 
  and a.send_date < DATE_ADD(concat('2026-11-09', ' 09:00:00'),INTERVAL 1 DAY)
  and a.data_valid = 10 and a.site_received >= 20
  and b.waybill_no is null
;

-- [20] dwd_oper_region_position_status_i_d
-- 任务 dwd_oper_region_position_status_i_d | 任务实例 ID 4655582 | 任务定义编码 17878043179072
-- 工作流实例 离职率报表-5-20261110090001036 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682470
-- 执行结果 2026-11-10 09:00:04.733 +0800 | 日志第 1092 行 | JDBC 返回行数 6184
INSERT INTO dwd_oper_region_position_status_i_d (
												 stats_date
												,org_operation_lvl
												,province_area_code
												,center_code
												,org_operation_hid
												,org_operation_name
												,province_area_name
												,center_name
												,department_hid
												,department_name
												,day_operate_dept_hid
												,day_operate_dept_name
												,team_hid
												,team_name
												,group_hid
												,group_name
												,sequence_hid
												,sequence_name
												,position_hid
												,position_name
												,employed_count
												,resign_count
												) 
SELECT 
     stats_date
    ,org_operation_lvl
    ,province_area_code
    ,center_code
    ,org_operation_hid
    ,org_operation_name
    ,province_area_name
    ,center_name
    ,department_hid
    ,department_name
    ,day_operate_dept_hid
    ,day_operate_dept_name
    ,team_hid
    ,team_name
    ,group_hid
    ,group_name
    ,sequence_hid
    ,sequence_name
    ,position_hid
    ,position_name
    ,employed_count
    ,resign_count
from 
	(
	SELECT 
		a.data_date as stats_date
		,CASE 
			WHEN b1.site_type_code = '900800' THEN 10
			WHEN b1.site_type_code in ('900100','900200') THEN 20
			WHEN a.dept_name LIKE '%部' THEN 30
			WHEN a.dept_name LIKE '%白班%' and dept_name LIKE '%部%' THEN 40
			WHEN a.dept_name LIKE '%队' THEN 50
			WHEN a.dept_name LIKE '%组' THEN 60
			ELSE NULL 
		END as org_operation_lvl
		,bb.site_code as province_area_code
		,b.site_code as center_code
		,CASE 
			WHEN b1.site_code is not null THEN b1.site_code 
			ELSE a.dept_hid 
		END as org_operation_hid		
		,a.dept_name as org_operation_name
		,if(a.dept4_name = '广州电商分拨中心','广州电商管理区',a.company_name) as province_area_name
		,a.dept4_name as center_name
		,CASE
			WHEN a.dept5_name LIKE '%部' THEN a.dept5_hid
			WHEN a.dept6_name LIKE '%部' THEN a.dept6_hid
			WHEN a.dept7_name LIKE '%部' THEN a.dept7_hid
			ELSE null
		END  as department_hid 		
		,CASE
			WHEN a.dept5_name LIKE '%部' THEN a.dept5_name
			WHEN a.dept6_name LIKE '%部' THEN a.dept6_name
			WHEN a.dept7_name LIKE '%部' THEN a.dept7_name
			ELSE null
		END  as department_name 
		,CASE 
		    WHEN dept5_name LIKE '%白班%' and dept5_name LIKE '%部%' THEN dept5_hid
		    WHEN dept6_name LIKE '%白班%' and dept6_name LIKE '%部%' THEN dept6_hid
		    WHEN dept7_name LIKE '%白班%' and dept7_name LIKE '%部%' THEN dept7_hid
		    ELSE NULL 
	 	END as day_operate_dept_hid 		
		,CASE 
		    WHEN dept5_name LIKE '%白班%' and dept5_name LIKE '%部%' THEN dept5_name
		    WHEN dept6_name LIKE '%白班%' and dept6_name LIKE '%部%' THEN dept6_name
		    WHEN dept7_name LIKE '%白班%' and dept7_name LIKE '%部%' THEN dept7_name
		    ELSE NULL 
		 END as day_operate_dept_name 
		,CASE
			WHEN a.dept5_name LIKE '%队' THEN a.dept5_hid
			WHEN a.dept6_name LIKE '%队' THEN a.dept6_hid
			WHEN a.dept7_name LIKE '%队' THEN a.dept7_hid
			ELSE null
		END  as team_hid
		,CASE
			WHEN a.dept5_name LIKE '%队' THEN a.dept5_name
			WHEN a.dept6_name LIKE '%队' THEN a.dept6_name
			WHEN a.dept7_name LIKE '%队' THEN a.dept7_name
			ELSE null
		END  as team_name
		,CASE
			WHEN a.dept5_name LIKE '%组' THEN a.dept5_hid
			WHEN a.dept6_name LIKE '%组' THEN a.dept6_hid
			WHEN a.dept7_name LIKE '%组' THEN a.dept7_hid
			WHEN a.dept8_name LIKE '%组' THEN a.dept8_hid
			ELSE null
		END  as group_hid
		,CASE
			WHEN a.dept5_name LIKE '%组' THEN a.dept5_name
			WHEN a.dept6_name LIKE '%组' THEN a.dept6_name
			WHEN a.dept7_name LIKE '%组' THEN a.dept7_name
			WHEN a.dept8_name LIKE '%组' THEN a.dept8_name
			ELSE null
		END  as group_name
		,a.sequence_hid
		,a.sequence_name		
		,a.position_hid
		,a.position_name 
		,sum(if(a.status = 1 ,1,0)) as employed_count
		,sum
			(
			case 
				when a.status = 0 and a.leave_date >= STR_TO_DATE(DATE_FORMAT(a.data_date, '%Y-%m-01'), '%Y-%m-%d')
				 AND a.leave_date <= a.data_date AND datediff(a.leave_date,a.labor_date) > 7
				THEN 1
				else 0
			end 
			) as resign_count	
	FROM ods_ehr_tb_emp_adc_f_d a
	inner join tx_bd_site_info b on a.dept4_name = b.site_name and b.site_type_code in ('900100','900200')
	left join tx_bd_site_info bb on ((if(a.dept4_name = '广州电商分拨中心','广州电商管理区',a.company_name)) = bb.site_name and bb.site_type_code = '900800')
	left join tx_bd_site_info b1 on a.dept_name = b1.site_name and b1.site_type_code in ('900800','900100','900200')
	where a.data_date = '2026-11-09'
	GROUP BY 
		a.data_date 
		,CASE 
			WHEN b1.site_type_code = '900800' THEN 10
			WHEN b1.site_type_code in ('900100','900200') THEN 20
			WHEN a.dept_name LIKE '%部' THEN 30
			WHEN a.dept_name LIKE '%白班%' and dept_name LIKE '%部%' THEN 40
			WHEN a.dept_name LIKE '%队' THEN 50
			WHEN a.dept_name LIKE '%组' THEN 60
			ELSE NULL 
		END 
		,bb.site_code 
		,b.site_code 
		,CASE 
			WHEN b1.site_code is not null THEN b1.site_code 
			ELSE a.dept_hid 
		END 		
		,a.dept_name 
		,if(a.dept4_name = '广州电商分拨中心','广州电商管理区',a.company_name) 
		,a.dept4_name 
		,CASE
			WHEN a.dept5_name LIKE '%部' THEN a.dept5_hid
			WHEN a.dept6_name LIKE '%部' THEN a.dept6_hid
			WHEN a.dept7_name LIKE '%部' THEN a.dept7_hid
			ELSE null
		END  		
		,CASE
			WHEN a.dept5_name LIKE '%部' THEN a.dept5_name
			WHEN a.dept6_name LIKE '%部' THEN a.dept6_name
			WHEN a.dept7_name LIKE '%部' THEN a.dept7_name
			ELSE null
		END  
		,CASE 
		    WHEN dept5_name LIKE '%白班%' and dept5_name LIKE '%部%' THEN dept5_hid
		    WHEN dept6_name LIKE '%白班%' and dept6_name LIKE '%部%' THEN dept6_hid
		    WHEN dept7_name LIKE '%白班%' and dept7_name LIKE '%部%' THEN dept7_hid
		    ELSE NULL 
	 	END  		
		,CASE 
		    WHEN dept5_name LIKE '%白班%' and dept5_name LIKE '%部%' THEN dept5_name
		    WHEN dept6_name LIKE '%白班%' and dept6_name LIKE '%部%' THEN dept6_name
		    WHEN dept7_name LIKE '%白班%' and dept7_name LIKE '%部%' THEN dept7_name
		    ELSE NULL 
		 END 
		,CASE
			WHEN a.dept5_name LIKE '%队' THEN a.dept5_hid
			WHEN a.dept6_name LIKE '%队' THEN a.dept6_hid
			WHEN a.dept7_name LIKE '%队' THEN a.dept7_hid
			ELSE null
		END  
		,CASE
			WHEN a.dept5_name LIKE '%队' THEN a.dept5_name
			WHEN a.dept6_name LIKE '%队' THEN a.dept6_name
			WHEN a.dept7_name LIKE '%队' THEN a.dept7_name
			ELSE null
		END  
		,CASE
			WHEN a.dept5_name LIKE '%组' THEN a.dept5_hid
			WHEN a.dept6_name LIKE '%组' THEN a.dept6_hid
			WHEN a.dept7_name LIKE '%组' THEN a.dept7_hid
			WHEN a.dept8_name LIKE '%组' THEN a.dept8_hid
			ELSE null
		END  
		,CASE
			WHEN a.dept5_name LIKE '%组' THEN a.dept5_name
			WHEN a.dept6_name LIKE '%组' THEN a.dept6_name
			WHEN a.dept7_name LIKE '%组' THEN a.dept7_name
			WHEN a.dept8_name LIKE '%组' THEN a.dept8_name
			ELSE null
		END  
		,a.sequence_hid
		,a.sequence_name		
		,a.position_hid
		,a.position_name		 
	)A
WHERE A.employed_count <> 0 OR A.resign_count <> 0;

-- [21] dwd_busi_waybill_today_due_detail_i_d
-- 任务 【update】dwd_busi_waybill_today_due_detail_i_d | 任务实例 ID 4655221 | 任务定义编码 21827934799072
-- 工作流实例 运单时效即将过期数据-7-20261110083000196 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682406
-- 执行结果 2026-11-10 08:30:04.570 +0800 | 日志第 172 行 | JDBC 返回行数 2851
insert into dwd_busi_waybill_today_due_detail_i_d
SELECT
	t.stats_date 
	,t.waybill_no
	,t.order_send_site_code 
	,t.bill_ec_id 
	,t.order_no 
	,t.bill_ec_name 
	,t.order_dest_site_code 
	,t.order_send_site_name 
	,t.order_dest_site_name 
	,t.order_dest_center_code 
	,t.order_dest_center_name 
	,t.order_send_city_name 
	,t.order_reach_city_name 
	,t.fixed_time
	,bbcs.accounts_time AS bill_accounts_time
	,bbcs.sign_time AS bill_sign_time
	,case when active = 0 then 99
        when date(bbcs.sign_time) = t.stats_date then 1
        else 2 end  order_is_delivery_timely
	,bbcs.is_sign
FROM
	dwd_busi_waybill_today_due_detail_i_d t
left join lbdb.bd_bill_center_send bbcs  
	on bbcs.ec_id IN (824, 810, 857)
	and bbcs.waybill_no = t.waybill_no 
	and accounts_time > '2026-04-01'
where t.is_sign = 0
and t.stats_date < '2026-11-10'
;

-- [22] dim_gis_tb_base_time_efficient
-- 任务 insert_dim_gis_tb_base_time_efficient | 任务实例 ID 4655191 | 任务定义编码 21838168706912
-- 工作流实例 8点20分_gis同步至sr-21-20261110082000582 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682387
-- 执行结果 2026-11-10 08:25:45.759 +0800 | 日志第 952 行 | JDBC 返回行数 72753
insert into dim_gis_tb_base_time_efficient
select
	date('2026-11-09')
	 , t.street_code
	 , t.province_name
	 , t.province_code
	 , t.city_name
	 , t.city_code
	 , t.district_name
	 , t.district_code
	 , t.street_name
	 , t.time_efficient
	 , t.status
	 , t.province_area_name
	 , t.big_area_name
	 , t.center_name
	 , t.administrative_unit
	 , t.is_second_delivery
	 , t.average_daily_cargo
	 , t.is_blind
	 , t.remark
	 , t.create_time
	 , t.create_user
	 , t.modify_time
	 , t.modify_user
	 , t.is_deleted
	 ,tx.site_code
	 ,tx.site_name
from
	tx_bd_site_info tx
	inner join (
		select
			bd.street_code,
			s.id,
			s.parent_id,
			s.link_persen,
			s.link_phone,
			s.address
		from
			ods_gis_tb_base_fence_division fd
			inner join ods_gis_tb_base_division bd on bd.`level` = 70
				and fd.division_code = bd.grid1_code
			inner join ods_gis_tb_base_site s on fd.fence_id = s.id
				and s.approve_status = 2
		where
			fd.fence_type = 10
		  AND fd.stats_date ='2026-11-09'
		  AND bd.stats_date ='2026-11-09'
		  AND s.stats_date ='2026-11-09'
		group by
			bd.street_code,
			s.id,
			s.parent_id,
			s.link_persen,
			s.link_phone,
			s.address
		union all
		select
			bd.street_code,
			s.id,
			s.parent_id,
			s.link_persen,
			s.link_phone,
			s.address
		from
			ods_gis_tb_base_fence_division fd
			inner join ods_gis_tb_base_division bd on bd.`level` = 80
				and fd.division_code = bd.grid2_code
			inner join ods_gis_tb_base_site s on fd.fence_id = s.id
				and s.approve_status = 2
		where
			fd.fence_type = 15
		  AND fd.stats_date ='2026-11-09'
		  AND bd.stats_date ='2026-11-09'
		  AND s.stats_date ='2026-11-09'
		group by
			bd.street_code,
			s.id,
			s.parent_id,
			s.link_persen,
			s.link_phone,
			s.address
	) r on cast(tx.site_id as varchar(32)) = r.id
	inner join tb_base_time_efficient t on r.street_code = t.street_code;

-- [23] dim_gis_tb_base_site_info_d
-- 任务 dim_gis_tb_base_site_info_d | 任务实例 ID 4655187 | 任务定义编码 22270464488800
-- 工作流实例 8点20分_gis同步至sr-21-20261110082000582 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682387
-- 执行结果 2026-11-10 08:25:39.658 +0800 | 日志第 192 行 | JDBC 返回行数 17481
INSERT INTO dim_gis_tb_base_site_info_d (
    stats_date,
    id,
    site_code,
    site_name,
    parent_id,
    site_type,
    site_nature,
    service_type,
    coordinate,
    electronic_fence,
    approve_status,
    bis_status,
    modified_time,
    etl_time
)
SELECT
    stats_date,
    id,
    site_code,
    site_name,
    parent_id,
    site_type,
    site_nature,
    service_type,
    coordinate,
    electronic_fence,
    approve_status,
    bis_status,
    modified_time,
    NOW()  AS etl_time
FROM ods_gis_tb_base_site
where stats_date = '2026-11-09'
and electronic_fence is not null
;

-- [24] lbdb.dws_oper_eff_delivery_rate_v
-- 任务 新增视图 | 任务实例 ID 4655168 | 任务定义编码 15100315341888
-- 工作流实例 妥投率报表和视图(慎动报表)-14-20261110082400827 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682397
-- 执行结果 2026-11-10 08:25:16.326 +0800 | 日志第 2285 行 | JDBC 返回行数 100187
INSERT INTO lbdb.dws_oper_eff_delivery_rate_v
(stats_date,
 create_time,
 site_code,
 first_site_code,
 province_area_code,
 center_code,
 area_code,
 province_code,
 city_code,
 district_code,
 street_code,
 site_name,
 first_site_name,
 province_area_name,
 center_name,
 area_name,
 province_name,
 city_name,
 district_name,
 street_name,
 weight_range_3300,
 sign_cnt,
 should_sign_cnt,
 week_sign_cnt,
 week_should_sign_cnt,
 month_sign_cnt,
 month_should_sign_cnt,
 month_sign_cnt_wd,
 month_should_sign_cnt_wd,
 month_sign_cnt_nwd,
 month_should_sign_cnt_nwd)
WITH DAY_DATA AS (
    SELECT t.receiver_province_code  AS province_code,
           t.receiver_city_code      AS city_code,
           t.receiver_district_code  AS district_code,
           t.receiver_street_code    AS street_code,
           t.next_site_code          AS site_code,
           t.next_first_site_code    AS first_site_code,
           t.next_area_code          AS area_code,
           t.next_center_code        AS center_code,
           t.next_province_area_code AS province_area_code,
           t.weight_range_3300       AS weight_range_3300,
           sum(t.day_sign_cnt)       AS sign_cnt,
           sum(t.should_sign_cnt)    AS should_sign_cnt
    FROM lbdb.dws_oper_eff_delivery_rate t
    where t.stats_date = '2026-11-10'
    group by t.receiver_province_code,
             t.receiver_city_code,
             t.receiver_district_code,
             t.receiver_street_code,
             t.next_site_code,
             t.next_first_site_code,
             t.next_area_code,
             t.next_center_code,
             t.next_province_area_code,
             t.weight_range_3300
),
     WEEK_DATA AS (
         SELECT t.receiver_province_code  AS province_code,
                t.receiver_city_code      AS city_code,
                t.receiver_district_code  AS district_code,
                t.receiver_street_code    AS street_code,
                t.next_site_code          AS site_code,
                t.next_first_site_code    AS first_site_code,
                t.next_area_code          AS area_code,
                t.next_center_code        AS center_code,
                t.next_province_area_code AS province_area_code,
                t.weight_range_3300       AS weight_range_3300,
                sum(t.day_sign_cnt)       AS sign_cnt,
                sum(t.should_sign_cnt)    AS should_sign_cnt
         FROM lbdb.dws_oper_eff_delivery_rate t
         where t.stats_date >= DATE_ADD('2026-11-10', 2 - CASE
                                                                       WHEN DAYOFWEEK('2026-11-10') = 1 then 8
                                                                       else DAYOFWEEK('2026-11-10') end)
           and t.stats_date <= '2026-11-10'
         group by t.receiver_province_code,
                  t.receiver_city_code,
                  t.receiver_district_code,
                  t.receiver_street_code,
                  t.next_site_code,
                  t.next_first_site_code,
                  t.next_area_code,
                  t.next_center_code,
                  t.next_province_area_code,
                  t.weight_range_3300
     ),
     MONTH_DATA AS (
         SELECT t.receiver_province_code                                          AS province_code,
                t.receiver_city_code                                              AS city_code,
                t.receiver_district_code                                          AS district_code,
                t.receiver_street_code                                            AS street_code,
                t.next_site_code                                                  AS site_code,
                t.next_first_site_code                                            AS first_site_code,
                t.next_area_code                                                  AS area_code,
                t.next_center_code                                                AS center_code,
                t.next_province_area_code                                         AS province_area_code,
                t.receiver_province_name                                          AS province_name,
                t.receiver_city_name                                              AS city_name,
                t.receiver_district_name                                          AS district_name,
                t.receiver_street                                                 AS street_name,
                t.next_site_name                                                  AS site_name,
                t.next_first_site_name                                            AS first_site_name,
                t.next_area_name                                                  AS area_name,
                t.next_center_name                                                AS center_name,
                t.next_province_area_name                                         AS province_area_name,
                t.weight_range_3300                                               AS weight_range_3300,
                sum(t.day_sign_cnt)                                               AS sign_cnt,
                sum(t.should_sign_cnt)                                            AS should_sign_cnt,
                sum(case when t1.workday = 'Y' then t.day_sign_cnt else 0 end)    AS month_sign_cnt_wd,
                sum(case when t1.workday = 'Y' then t.should_sign_cnt else 0 end) AS month_should_sign_cnt_wd,
                sum(case when t1.workday = 'N' then t.day_sign_cnt else 0 end)    AS month_sign_cnt_nwd,
                sum(case when t1.workday = 'N' then t.should_sign_cnt else 0 end) AS month_should_sign_cnt_nwd
         FROM lbdb.dws_oper_eff_delivery_rate t
                  left join lbdb.bd_work_day t1 on t.stats_date = t1.idate
         where t.stats_date >= date_trunc('MONTH', '2026-11-10')
           and t.stats_date <= '2026-11-10'
         group by t.receiver_province_code,
                  t.receiver_city_code,
                  t.receiver_district_code,
                  t.receiver_street_code,
                  t.next_site_code,
                  t.next_first_site_code,
                  t.next_area_code,
                  t.next_center_code,
                  t.next_province_area_code,
                  t.receiver_province_name,
                  t.receiver_city_name,
                  t.receiver_district_name,
                  t.receiver_street,
                  t.next_site_name,
                  t.next_first_site_name,
                  t.next_area_name,
                  t.next_center_name,
                  t.next_province_area_name,
                  t.weight_range_3300
     )
select '2026-11-10'        as stats_date,
       NOW()                        as create_time,
       m.site_code,
       m.first_site_code,
       m.province_area_code,
       m.center_code,
       m.area_code,
       m.province_code,
       m.city_code,
       m.district_code,
       m.street_code,
       m.site_name,
       m.first_site_name,
       m.province_area_name,
       m.center_name,
       m.area_name,
       m.province_name,
       m.city_name,
       m.district_name,
       m.street_name,
       m.weight_range_3300,
       ifnull(d.sign_cnt, 0)        as sign_cnt,
       ifnull(d.should_sign_cnt, 0) as should_sign_cnt,
       ifnull(w.sign_cnt, 0)        as week_sign_cnt,
       ifnull(w.should_sign_cnt, 0) as week_should_sign_cnt,
       ifnull(m.sign_cnt, 0)        as month_sign_cnt,
       ifnull(m.should_sign_cnt, 0) as month_should_sign_cnt,
       m.month_sign_cnt_wd,
       m.month_should_sign_cnt_wd,
       m.month_sign_cnt_nwd,
       m.month_should_sign_cnt_nwd
from MONTH_DATA m
         left join WEEK_DATA w on m.site_code = w.site_code
    and m.first_site_code = w.first_site_code
    and m.province_area_code = w.province_area_code
    and m.center_code = w.center_code
    and m.area_code = w.area_code
    and m.province_code = w.province_code
    and m.city_code = w.city_code
    and m.district_code = w.district_code
    and m.street_code = w.street_code
    and m.weight_range_3300 = w.weight_range_3300
         left join DAY_DATA d on m.site_code = d.site_code
    and m.first_site_code = d.first_site_code
    and m.province_area_code = d.province_area_code
    and m.center_code = d.center_code
    and m.area_code = d.area_code
    and m.province_code = d.province_code
    and m.city_code = d.city_code
    and m.district_code = d.district_code
    and m.street_code = d.street_code
    and m.weight_range_3300 = d.weight_range_3300
;

-- [25] lbdb.dws_oper_eff_delivery_rate_delivery
-- 任务 写入dws_oper_eff_delivery_rate_delivery | 任务实例 ID 4655160 | 任务定义编码 16242583404096
-- 工作流实例 妥投率报表和视图(慎动报表)-14-20261110082400827 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682397
-- 执行结果 2026-11-10 08:25:12.259 +0800 | 日志第 409 行 | JDBC 返回行数 64073
INSERT INTO lbdb.dws_oper_eff_delivery_rate_delivery
select '2026-11-10' as stats_date
     ,t.*
     ,CURRENT_TIMESTAMP etl_time
from
    (select   t.dest_site_code
              ,t.delivery_code
              ,t.delivery_name
              ,t.dest_site_name
              ,t.dest_first_site_code
              ,t.dest_first_site_name
              ,t.next_center_code as dest_center_code
              ,t.next_center_name as dest_center_name
              ,t.next_area_code as dest_area_code
              ,t.next_area_name as dest_area_name
              ,t.next_province_area_code as dest_province_area_code
              ,t.next_province_area_name as dest_province_area_name
              ,t.receiver_street
              ,t.receiver_district_name
              ,t.receiver_city_name
              ,t.receiver_province_name
              ,t.time_efficient_name1
              ,t.time_efficient_name2
              ,sum(t.day_sign_flag) AS day_sign_cnt
              ,sum(if(t.next_province_area_name='广州电商管理区',t.day_sign_flag,t.real_sign_flag)) AS real_sign_cnt
              ,sum(t.should_sign_flag) AS should_sign_cnt
     from dwd_oper_eff_delivery_rate_should_sign t -- 运营-时效-签收率（应签明细表）
     where scan_time >= adddate(TIMESTAMP ('2026-11-11'), INTERVAL - 16 DAY)
       AND scan_time < '2026-11-11'
       AND scan_time_0to0_last16_date = '2026-11-10'
     group by   t.dest_site_code
                ,t.delivery_code
                ,t.delivery_name
                ,t.dest_site_name
                ,t.dest_first_site_code
                ,t.dest_first_site_name
                ,t.next_center_code
                ,t.next_center_name
                ,t.next_area_code
                ,t.next_area_name
                ,t.next_province_area_code
                ,t.next_province_area_name
                ,t.receiver_street
                ,t.receiver_district_name
                ,t.receiver_city_name
                ,t.receiver_province_name
                ,t.time_efficient_name1
                ,t.time_efficient_name2
                ) t
;

-- [26] lbdb.dws_oper_eff_delivery_rate_dest_site
-- 任务 dws_oper_eff_delivery_rate_dest_site | 任务实例 ID 4655155 | 任务定义编码 16242582287040
-- 工作流实例 妥投率报表和视图(慎动报表)-14-20261110082400827 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682397
-- 执行结果 2026-11-10 08:25:08.660 +0800 | 日志第 513 行 | JDBC 返回行数 116452
INSERT INTO lbdb.dws_oper_eff_delivery_rate_dest_site
select '2026-11-10' as stats_date
     ,t.*
     ,CURRENT_TIMESTAMP etl_time
from
    (select   t.dest_site_code
              ,t.dest_site_name
              ,t.dest_first_site_code
              ,t.dest_first_site_name
              ,t.next_center_code as dest_center_code
              ,t.next_center_name as dest_center_name
              ,t.next_area_code as dest_area_code
              ,t.next_area_name as dest_area_name
              ,t.next_province_area_code as dest_province_area_code
              ,t.next_province_area_name as dest_province_area_name
              ,t.receiver_street_code
              ,t.receiver_street
              ,t.receiver_district_code
              ,t.receiver_district_name
              ,t.receiver_city_code
              ,t.receiver_city_name
              ,t.receiver_province_code
              ,t.receiver_province_name
              ,t.weight_range
              ,t.weight_range_3300
              ,t.time_efficient_name1
              ,t.time_efficient_name2
              ,t.site_to_center_distance_range
              ,t.district_to_center_distance_range
              ,t.district_to_street_distance_range
              ,sum(t.day_sign_flag) AS day_sign_cnt
              ,sum(if(t.next_province_area_name='广州电商管理区',t.day_sign_flag,t.real_sign_flag)) AS real_sign_cnt
              ,sum(t.should_sign_flag) AS should_sign_cnt
     from dwd_oper_eff_delivery_rate_should_sign t -- 运营-时效-签收率（应签明细表）
     where scan_time >= adddate(TIMESTAMP ('2026-11-11'), INTERVAL - 16 DAY)
       AND scan_time < '2026-11-11'
       AND scan_time_0to0_last16_date = '2026-11-10'
     group by   t.dest_site_code
                ,t.dest_site_name
                ,t.dest_first_site_code
                ,t.dest_first_site_name
                ,t.next_center_code
                ,t.next_center_name
                ,t.next_area_code
                ,t.next_area_name
                ,t.next_province_area_code
                ,t.next_province_area_name
                ,t.receiver_street_code
                ,t.receiver_street
                ,t.receiver_district_code
                ,t.receiver_district_name
                ,t.receiver_city_code
                ,t.receiver_city_name
                ,t.receiver_province_code
                ,t.receiver_province_name
                ,t.weight_range
                ,t.weight_range_3300
                ,t.time_efficient_name1
                ,t.time_efficient_name2
                ,t.site_to_center_distance_range
                ,t.district_to_center_distance_range
                ,t.district_to_street_distance_range) t
;

-- [27] lbdb.dws_oper_eff_delivery_rate
-- 任务 dws_oper_eff_delivery_rate | 任务实例 ID 4655150 | 任务定义编码 16242579640640
-- 工作流实例 妥投率报表和视图(慎动报表)-14-20261110082400827 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682397
-- 执行结果 2026-11-10 08:25:05.195 +0800 | 日志第 553 行 | JDBC 返回行数 100206
INSERT INTO lbdb.dws_oper_eff_delivery_rate
select '2026-11-10' as stats_date
     ,t.*
     ,CURRENT_TIMESTAMP etl_time
from
    (select   t.next_site_code
              ,t.next_site_name
              ,t.next_parent_site_code
              ,t.next_parent_site_name
              ,t.next_first_site_code
              ,t.next_first_site_name
              ,t.next_center_code
              ,t.next_center_name
              ,t.next_area_code
              ,t.next_area_name
              ,t.next_province_area_code
              ,t.next_province_area_name
              ,t.receiver_street_code
              ,t.receiver_street
              ,t.receiver_district_code
              ,t.receiver_district_name
              ,t.receiver_city_code
              ,t.receiver_city_name
              ,t.receiver_province_code
              ,t.receiver_province_name
              ,t.weight_range
              ,t.weight_range_3300
              ,t.time_efficient_name1
              ,t.time_efficient_name2
              ,t.site_to_center_distance_range
              ,t.district_to_center_distance_range
              ,t.district_to_street_distance_range
              ,sum(t.day_sign_flag) AS day_sign_cnt
              ,sum(t.real_sign_flag) AS real_sign_cnt
              ,sum(t.should_sign_flag) AS should_sign_cnt
     from dwd_oper_eff_delivery_rate_should_sign t -- 运营-时效-签收率（应签明细表）
     where scan_time >= adddate(TIMESTAMP ('2026-11-11'), INTERVAL - 16 DAY)
       AND scan_time < '2026-11-11'
       AND scan_time_0to0_last16_date = '2026-11-10'
     group by   t.next_site_code
                ,t.next_site_name
                ,t.next_parent_site_code
                ,t.next_parent_site_name
                ,t.next_first_site_code
                ,t.next_first_site_name
                ,t.next_center_code
                ,t.next_center_name
                ,t.next_area_code
                ,t.next_area_name
                ,t.next_province_area_code
                ,t.next_province_area_name
                ,t.receiver_street_code
                ,t.receiver_street
                ,t.receiver_district_code
                ,t.receiver_district_name
                ,t.receiver_city_code
                ,t.receiver_city_name
                ,t.receiver_province_code
                ,t.receiver_province_name
                ,t.weight_range
                ,t.weight_range_3300
                ,t.time_efficient_name1
                ,t.time_efficient_name2
                ,t.site_to_center_distance_range
                ,t.district_to_center_distance_range
                ,t.district_to_street_distance_range) t
;

-- [28] lbdb.dwd_oper_eff_delivery_rate_should_sign
-- 任务 dwd_oper_eff_delivery_rate_should_sign | 任务实例 ID 4655147 | 任务定义编码 16242570032576
-- 工作流实例 妥投率报表和视图(慎动报表)-14-20261110082400827 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682397
-- 执行结果 2026-11-10 08:25:01.981 +0800 | 日志第 3037 行 | JDBC 返回行数 556776
INSERT INTO lbdb.dwd_oper_eff_delivery_rate_should_sign
WITH basic_data AS
         (SELECT IF ((sign_time > scan_time
             OR sign_time IS NULL)
                         AND scan_time >= adddate(TIMESTAMP (concat_ws(' ', '2026-11-11', must_clock_time)), INTERVAL -(( COALESCE(time_efficient,0) + 2 )* 1440+90 ) MINUTE)
                         AND scan_time < adddate(TIMESTAMP (concat_ws(' ', '2026-11-11', must_clock_time)), INTERVAL -(( COALESCE(time_efficient,0) + 1 )* 1440+90 ) MINUTE),
                     1,
                     0) AS should_sign_flag
                  ,IF (sign_time > scan_time
                         AND is_sign = 1
                         AND scan_time >= adddate(TIMESTAMP (concat_ws(' ', '2026-11-11', must_clock_time)), INTERVAL -(( COALESCE(time_efficient,0) + 2 )* 1440+90 ) MINUTE)
                         AND scan_time < adddate(TIMESTAMP (concat_ws(' ', '2026-11-11', must_clock_time)), INTERVAL -(( COALESCE(time_efficient,0) + 1 )* 1440+90 ) MINUTE),
                     1,
                     0) AS day_sign_flag
                  ,IF (sign_time > scan_time
                         AND real_sign = 20
                         AND is_sign = 1
                         AND scan_time >= adddate(TIMESTAMP (concat_ws(' ', '2026-11-11', must_clock_time)), INTERVAL -(( COALESCE(time_efficient,0) + 2 )* 1440+90 ) MINUTE)
                         AND scan_time < adddate(TIMESTAMP (concat_ws(' ', '2026-11-11', must_clock_time)), INTERVAL -(( COALESCE(time_efficient,0) + 1 )* 1440+90 ) MINUTE),
                     1,
                     0) AS real_sign_flag
                  ,waybill_no
                  ,scan_time_0to0_last16_date
                  ,weight_range
                  ,weight_range_3300
                  ,receiver_street_code
                  ,receiver_province_code
                  ,receiver_province_name
                  ,receiver_city_code
                  ,receiver_city_name
                  ,receiver_district_code
                  ,receiver_district_name
                  ,receiver_street
                  ,time_efficient
                  ,time_efficient_name1
                  ,time_efficient_name2
                  ,site_to_center_distance
                  ,site_to_center_distance_range
                  ,district_to_center_distance
                  ,district_to_center_distance_range
                  ,district_to_street_distance
                  ,district_to_street_distance_range
                  ,next_parent_site_code
                  ,next_parent_site_name
                  ,next_first_site_code
                  ,next_first_site_name
                  ,next_province_area_code
                  ,next_province_area_name
                  ,next_center_code
                  ,next_center_name
                  ,next_area_code
                  ,next_area_name
                  ,next_site_code
                  ,next_site_name
                  ,scan_time
                  ,sign_time
                  ,must_clock_time
                  ,is_change_waybill
                  ,center_arrive_id
                  ,product_type_code
                  ,scan_site_code
                  ,dest_site_code
                  ,receiver_address
                  ,delivery_code
                  ,delivery_name
                  ,delivery_site_code
                  ,calc_weight
          FROM dwd_oper_eff_delivery_rate_next_site -- 运营-时效-签收率（下一站明细表）
          WHERE scan_time >= adddate(TIMESTAMP ('2026-11-11'), INTERVAL - 16 DAY)
            AND scan_time < '2026-11-11'
            AND scan_time_0to0_last16_date = '2026-11-10' ),
     used_waybill AS
         (SELECT DISTINCT waybill_no
          FROM dwd_oper_eff_delivery_rate_should_sign -- 运营-时效-签收率（应签明细表）
          WHERE scan_time >= adddate(TIMESTAMP ('2026-11-11'), INTERVAL - 106 DAY)
            AND scan_time < '2026-11-11'
            AND scan_time_0to0_last16_date >= adddate(TIMESTAMP ('2026-11-10'), INTERVAL - 90 DAY)
            AND scan_time_0to0_last16_date < '2026-11-10'
            ),
     change_waybill AS
         (SELECT waybill_no,
                 operate_site_code
          FROM dwd_oper_eff_delivery_rate_waybill_change -- 运营-时效-签收率（末端分拨操作后的改单记录表）
          WHERE scan_time >= adddate(TIMESTAMP ('2026-11-11'), INTERVAL - 16 DAY)
            AND scan_time < '2026-11-11'
            AND scan_time_0to0_last16_date = '2026-11-10' )
SELECT t.*
FROM
    (SELECT   t1.scan_time
              ,t1.scan_time_0to0_last16_date
              ,t1.next_site_code
              ,t1.next_site_name
              ,t1.waybill_no
              ,t1.product_type_code
              ,t1.calc_weight
              ,t1.weight_range
              ,t1.weight_range_3300
              ,t1.scan_site_code
              ,t1.dest_site_code
              ,t1.receiver_address
              ,t1.receiver_province_code
              ,t1.receiver_province_name
              ,t1.receiver_city_code
              ,t1.receiver_city_name
              ,t1.receiver_district_code
              ,t1.receiver_district_name
              ,t1.receiver_street_code
              ,t1.receiver_street
              ,t1.next_parent_site_code
              ,t1.next_parent_site_name
              ,t1.next_first_site_code
              ,t1.next_first_site_name
              ,t1.next_province_area_code
              ,t1.next_province_area_name
              ,t1.next_center_code
              ,t1.next_center_name
              ,t1.next_area_code
              ,t1.next_area_name
              ,t1.delivery_code
              ,t1.delivery_name
              ,t1.delivery_site_code
              ,t1.must_clock_time
              ,t1.site_to_center_distance
              ,t1.site_to_center_distance_range
              ,t1.district_to_center_distance
              ,t1.district_to_center_distance_range
              ,t1.district_to_street_distance
              ,t1.district_to_street_distance_range
              ,t1.sign_time
              ,t1.day_sign_flag
              ,t1.real_sign_flag
              ,t1.should_sign_flag
              ,t1.is_change_waybill
              ,t1.center_arrive_id
              ,t1.time_efficient
              ,t1.time_efficient_name1
              ,t1.time_efficient_name2
              ,CURRENT_TIMESTAMP as etl_time
              ,destSite.site_name as dest_site_name
              ,destFirstSite.site_code as dest_first_site_code
              ,destFirstSite.site_name as dest_first_site_name
     FROM basic_data t1
              LEFT JOIN used_waybill t2 ON t1.waybill_no = t2.waybill_no
              LEFT JOIN change_waybill t3 ON t1.waybill_no = t3.waybill_no
              left join tx_bd_site_info destSite
                        on t1.dest_site_code = destSite.site_code
              left join tx_bd_site_info destFirstSite
                        on destSite.first_site_code = destFirstSite.site_code
     WHERE t1.should_sign_flag = 1
       AND t3.operate_site_code IS NULL
       AND t2.waybill_no IS NULL ) t
;

-- [29] lbdb.dwd_oper_eff_delivery_rate_waybill_change
-- 任务 dwd_oper_eff_delivery_rate_waybill_change | 任务实例 ID 4655145 | 任务定义编码 16242557725504
-- 工作流实例 妥投率报表和视图(慎动报表)-14-20261110082400827 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682397
-- 执行结果 2026-11-10 08:24:55.795 +0800 | 日志第 523 行 | JDBC 返回行数 1061
INSERT INTO lbdb.dwd_oper_eff_delivery_rate_waybill_change
WITH should_sign_data AS
         (SELECT IF ((sign_time > scan_time
             OR sign_time IS NULL)
                         AND scan_time >= adddate(TIMESTAMP (concat_ws(' ', '2026-11-11', must_clock_time)), INTERVAL -(( COALESCE(time_efficient,0) + 2 )* 1440+90 ) MINUTE)
                         AND scan_time < adddate(TIMESTAMP (concat_ws(' ', '2026-11-11', must_clock_time)), INTERVAL -(( COALESCE(time_efficient,0) + 1 )* 1440+90 ) MINUTE),
                     1,
                     0) AS should_sign_flag
                  ,waybill_no
                  ,scan_time_0to0_last16_date
                  ,scan_time
                  ,center_arrive_id
                  ,dest_site_code
                  ,scan_site_code
          FROM dwd_oper_eff_delivery_rate_next_site -- 运营-时效-签收率（下一站明细表）
          WHERE scan_time >= adddate(TIMESTAMP ('2026-11-11'), INTERVAL - 16 DAY)
            AND scan_time < '2026-11-11'
            AND scan_time_0to0_last16_date = '2026-11-10' )
SELECT  t.scan_time
        ,t.scan_time_0to0_last16_date
        ,t.waybill_no
        ,t.scan_site_code
        ,t.center_arrive_id
        ,tt.operate_time
        ,tt.operate_site_code
        ,tt.operate_content
        ,10 AS change_type -- 改单类型;10:改目的网点且不是签收动作改的,20:其它
        ,CURRENT_TIMESTAMP etl_time
FROM
    (SELECT *
     FROM should_sign_data
     WHERE should_sign_flag = 1 ) t
        INNER JOIN
    (SELECT  waybill_no
             ,operate_content
             ,operate_time
             ,operate_site_code
     FROM ods_tx_tx_waybill_operate_record_1y -- 天象改单记录表
     WHERE create_time >= '2026-10-25'
       AND operate_type = 10
       AND data_valid = 10
       AND operate_time < '2026-11-11'
       AND operate_content LIKE '%"alias": "目的网点"%'
       AND operate_content NOT LIKE '%"alias": "签收类型"%' ) tt ON t.waybill_no = tt.waybill_no
WHERE tt.operate_time > t.scan_time
;

-- [30] lbdb.dwd_oper_eff_delivery_rate_next_site
-- 任务 dwd_oper_eff_delivery_rate_next_site | 任务实例 ID 4655143 | 任务定义编码 16242538856512
-- 工作流实例 妥投率报表和视图(慎动报表)-14-20261110082400827 | http://10.32.5.43:12345/dolphinscheduler/ui/projects/8655547341888/workflow/instances/1682397
-- 执行结果 2026-11-10 08:24:51.119 +0800 | 日志第 8501 行 | JDBC 返回行数 10137830
INSERT INTO lbdb.dwd_oper_eff_delivery_rate_next_site
WITH base_site as (select site_code
                        ,site_name
                        ,site_type_code
                        ,case when get_json_string ( site_bind, "$.upgradeBindSite" ) <>0
	                              then get_json_string ( site_bind, "$.upgradeBindSite" )
                              else parent_site_code
                         end as parent_site_code
                   from tx_bd_site_info),
     lbdb4my_tx_oper_center_mreach AS
	     (SELECT mreach.*
	           ,sign.sign_time-- 签收扫描时间
	           ,sign.is_my_sign-- 签收扫描表，签收状态为正常签收、异常签收，是否实时签收为是
	           ,sign.real_sign-- 是否实时签收 20：是
	           ,sign.sign_status
	           ,sign.delivery_code
	           ,sign.delivery_name
	           ,sign.delivery_site_code
	           ,site.site_code-- 网点编号
	           ,site.site_type_code-- 网点类型
	           ,site.parent_site_code AS parent_site_code-- 上级网点编号
	           ,site.parent_site_type_code AS parent_site_type_code-- 上级网点类型
	           ,site.parent_parent_site_code AS parent_parent_site_code-- 上级上级网点编号
	           ,site.parent_parent_site_type_code AS parent_parent_site_type_code -- 上级上级网点类型
	      FROM
		      (SELECT t1.*
		            ,t2.site_type_code as scan_site_type_code
		            ,t3.site_type_code as next_site_type_code
		            ,t4.site_type_code as dest_site_type_code
		            ,10 as is_change_waybill -- 是否作日改单数据 10:不是,20:是
		       FROM
			       (SELECT t1.waybill_no
			             ,t1.scan_site_code-- 操作网点编号
			             ,t1.dest_site_code-- 目的网点编号
			             ,t1.next_site_code-- 下一站网点编码
			             ,t1.calc_weight-- 结算重量
			             ,t1.dest_first_site_code-- 目的网点所属一级网点编号
			             ,t1.scan_time-- 操作时间
			             ,t1.product_type_code-- 产品类型名称
			             ,t1.dest_center_code
			             ,t1.update_time
			             ,t1.center_arrive_id
			             ,ROW_NUMBER() OVER(PARTITION BY t1.scan_site_code, t1.waybill_no
                   ORDER BY t1.scan_time ASC) AS id_rn
			        FROM lbdb.tx_oper_center_reach t1 -- 天象中心到港主单表
			             left join
			             (select waybill_no
			              from lbdb.dwd_oper_eff_delivery_rate_waybill_change -- 运营-时效-签收率（末端分拨操作后的改单记录表）
			              where scan_time_0to0_last16_date = adddate(TIMESTAMP ('2026-11-11'), INTERVAL -2 day)) t2 on t1.waybill_no = t2.waybill_no
			             left join tx_bd_site_info t3 on t1.dest_center_code = t3.site_code
			        WHERE t1.create_time >= adddate(TIMESTAMP ('2026-11-11'), INTERVAL -21 day)
				      and t1.create_time < adddate(TIMESTAMP ('2026-11-11'), INTERVAL 5 day)
				      and t1.scan_time >= adddate(TIMESTAMP ('2026-11-11'), INTERVAL -16 day)
				      AND t1.scan_time < '2026-11-11'
				      and t2.waybill_no is null
				      AND t1.scan_type = 10 -- 扫描类型 10：卸车扫描
				      and t1.scan_site_code = if(t3.site_type_code in (900200,900100),t3.site_code,t3.send_center_code)  -- 限制扫描网点=目的分拨的首发分拨，解决集散站问题
			       ) t1
			       left join lbdb.tx_bd_site_info t2 on t1.scan_site_code = t2.site_code
			       left join lbdb.tx_bd_site_info t3 on t1.next_site_code = t3.site_code
			       left join lbdb.tx_bd_site_info t4 on t1.dest_site_code = t4.site_code
		       where id_rn = 1
			     and t2.site_type_code in (900100,
			                               900200)
		       union all SELECT t1.*
		                      ,t2.site_type_code as scan_site_type_code
		                      ,t3.site_type_code as next_site_type_code
		                      ,t4.site_type_code as dest_site_type_code
		                      ,20 as is_change_waybill -- 是否作日改单数据 10:不是,20:是
		       FROM
			       (SELECT t1.waybill_no
			             ,t1.scan_site_code-- 操作网点编号
			             ,t1.dest_site_code-- 目的网点编号
			             ,t1.next_site_code-- 下一站网点编码
			             ,t1.calc_weight-- 结算重量
			             ,t1.dest_first_site_code-- 目的网点所属一级网点编号
			             ,t1.scan_time-- 操作时间
			             ,t1.product_type_code-- 产品类型名称
			             ,t1.dest_center_code
			             ,t1.update_time
			             ,t1.center_arrive_id
			             ,ROW_NUMBER() OVER(PARTITION BY t1.scan_site_code, t1.waybill_no
                   ORDER BY t1.scan_time ASC) AS id_rn
			        FROM lbdb.tx_oper_center_reach t1 -- 天象中心到港主单表
			             inner join
			             (select waybill_no
			              from lbdb.dwd_oper_eff_delivery_rate_waybill_change -- 运营-时效-签收率（末端分拨操作后的改单记录表）
			              where scan_time_0to0_last16_date = adddate(TIMESTAMP ('2026-11-11'), INTERVAL -2 day)) t2 on t1.waybill_no = t2.waybill_no
			             left join tx_bd_site_info t3 on t1.dest_center_code = t3.site_code
			        WHERE t1.create_time >= adddate(TIMESTAMP ('2026-11-11'), INTERVAL -21 day)
				      and t1.create_time < adddate(TIMESTAMP ('2026-11-11'), INTERVAL 5 day)
				      and t1.scan_time >= adddate(TIMESTAMP ('2026-11-11'), INTERVAL -16 day)
				      AND t1.scan_time < '2026-11-11'
				      AND t1.scan_type = 10 -- 扫描类型 10：卸车扫描
				      and t1.scan_site_code = if(t3.site_type_code in (900200,900100),t3.site_code,t3.send_center_code)  -- 限制扫描网点=目的分拨的首发分拨，解决集散站问题
			       ) t1
			       left join lbdb.tx_bd_site_info t2 on t1.scan_site_code = t2.site_code
			       left join lbdb.tx_bd_site_info t3 on t1.next_site_code = t3.site_code
			       left join lbdb.tx_bd_site_info t4 on t1.dest_site_code = t4.site_code
		       where id_rn = 1
			     and t2.site_type_code in (900100,
			                               900200) ) mreach
		      INNER JOIN
		      (SELECT waybill_no
		            ,delivery_type_code
		       FROM lbdb.tx_waybill_info_ext
		       WHERE delivery_type_code NOT IN (40,
		                                        45,
		                                        50)
			     and send_date_9to9_date  > date_sub('2026-11-11', INTERVAL 2 MONTH)
			     and send_date_9to9_date <= '2026-11-11'
		      ) waybill ON mreach.waybill_no = waybill.waybill_no
		      LEFT JOIN
		      (SELECT s1.site_code-- 网点编号
		            ,s1.site_type_code-- 网点类型
		            ,s2.site_code AS parent_site_code-- 上级网点编号
		            ,s2.site_type_code AS parent_site_type_code-- 上级网点类型
		            ,s3.site_code AS parent_parent_site_code-- 上级网点编号
		            ,s3.site_type_code AS parent_parent_site_type_code -- 上级网点类型

		       FROM base_site s1
		            LEFT JOIN base_site s2 ON s1.parent_site_code = s2.site_code
		            LEFT JOIN base_site s3 ON s2.parent_site_code = s3.site_code) site ON mreach.dest_site_code = site.site_code
		      left JOIN
		      (select t.* from (SELECT waybill_no
		                             ,delivery_code
		                             ,delivery_name
		                             ,delivery_site_code
		                             ,IF (sign_time >= TIMESTAMP ('8888-12-31 23:59:59'), NULL,sign_time) AS sign_time
		                             ,sign_status
		                             ,real_sign-- 实时签收 10 :否20 :是
		                             ,IF (sign_status IN (20,25) and sign_time < TIMESTAMP ('2026-11-11'), TRUE,FALSE) AS is_my_sign
		                             ,update_time
		                             ,ROW_NUMBER() OVER(PARTITION BY waybill_no ORDER BY update_time DESC,sign_time DESC) AS rn
		                        FROM lbdb.ods_tx_tx_oper_site_sign_1y --天象网点派签表
		                        WHERE create_time > adddate(TIMESTAMP ('2026-11-11'), INTERVAL -180 day)
			                      and sign_time<timestamp('2026-11-11')) t where t.rn = 1) sign ON mreach.waybill_no = sign.waybill_no
               ),
     lbdb4my_tx_site_clock_maintain2max AS
         (SELECT *
FROM
	(SELECT *
		,ROW_NUMBER() OVER (PARTITION BY scan_site_code,site_code ORDER BY classes DESC) AS rn
	FROM
	(SELECT  scan_site_code-- 打卡扫描网点编码
		,site_code
		,must_clock_time-- 应打卡时间
		,clock_type
		,classes --班次

	FROM lbdb.dim_site_clock_maintain_s_d -- 天象网点应打卡时间表
	WHERE clock_type = 20 -- 打卡类型（ 10：接货到港，20：接货离港，30：发货）

	AND data_valid = 10 -- 数据状态（ 10：有效，20：无效）
	and dt = '2026-11-10'
	) tmp) t
WHERE rn = 1 ),
	tx_bd_site_to_county_distance as (
SELECT
	site_to_county_distance
		,case
	when site_to_county_distance > 0
	and site_to_county_distance <=50 then '0-50km'
	when site_to_county_distance > 50
	and site_to_county_distance <=100 then '50-100km'
	when site_to_county_distance > 100
	and site_to_county_distance <=150 then '100-150km'
	when site_to_county_distance > 150
	and site_to_county_distance <=300 then '150-300km'
	when site_to_county_distance > 300 then '300km以上'
	else '其它'
	end as district_to_center_distance_range --网点所属区县-分拨距离段（0-50km、50-100km、100-150km、150-300km、300km以上、其它）
		,case
	when site_to_county_distance > 0
	and site_to_county_distance <=10 then '0-10km'
	when site_to_county_distance > 10
	and site_to_county_distance <=20 then '10-20km'
	when site_to_county_distance > 20
	and site_to_county_distance <=30 then '20-30km'
	when site_to_county_distance > 30
	and site_to_county_distance <=40 then '30-40km'
	when site_to_county_distance > 40
	and site_to_county_distance <=50 then '40-50km'
	when site_to_county_distance > 50 then '50km以上'
	else '其它'
	end as district_to_street_distance_range -- 网点所属区县-派件乡镇/街道距离段（0-10km、10-20km、20-30km、30-40km、40-50km、50km以上、其它）
		,site_code -- 分拨或乡镇编码
		,county_code -- 区县编码
FROM
	( SELECT  round(site_to_county_distance/1000,2) as site_to_county_distance
		, site_code
		, county_code
		, ROW_NUMBER() OVER ( PARTITION BY site_code, county_code ORDER BY update_time DESC ) AS id_rn
	FROM ods_tx_tx_bd_site_to_county_distance_f_d -- 天象乡镇区县/区县分拨距离表
	) t
WHERE
	id_rn = 1
	)
SELECT t.scan_time
	 ,'2026-11-10' AS scan_time_0to0_last16_date
	 ,t.next_site_code
	 ,site.site_name AS next_site_name-- 网点名称
	 ,waybill.waybill_no
	 ,t.product_type_code
	 ,waybill.calc_weight-- 结算重量
	 ,waybill.weight_range-- 公斤段
	 ,case
	when waybill.calc_weight > 0
		and waybill.calc_weight <=3 then '0-3kg'
	when waybill.calc_weight > 3
		and waybill.calc_weight <=300 then '3-300kg'
	when waybill.calc_weight > 300 then '300kg以上'
	else '其它'
	  end as weight_range_3300
	 ,t.scan_site_code
	 ,t.dest_site_code-- 目的网点编号
	 ,if(waybill.receiver_address='N/A'
		     or waybill.receiver_address is null, '其它', waybill.receiver_address) receiver_address-- 详细收件地址
	 ,if(waybill.receiver_province_code='N/A'
		     or waybill.receiver_province_code is null, '其它', waybill.receiver_province_code) receiver_province_code-- 省份编码
	 ,if(waybill.receiver_province_name='N/A'
		     or waybill.receiver_province_name is null, '其它', waybill.receiver_province_name) receiver_province_name-- 省份名称
	 ,if(waybill.receiver_city_code='N/A'
		     or waybill.receiver_city_code is null, '其它', waybill.receiver_city_code) receiver_city_code-- 城市编码
	 ,if(waybill.receiver_city_name='N/A'
		     or waybill.receiver_city_name is null, '其它', waybill.receiver_city_name) receiver_city_name-- 城市名称
	 ,if(waybill.receiver_district_code='N/A'
		     or waybill.receiver_district_code is null, '其它', waybill.receiver_district_code) receiver_district_code-- 区县编码
	 ,if(waybill.receiver_district_name='N/A'
		     or waybill.receiver_district_name is null, '其它', waybill.receiver_district_name) receiver_district_name-- 区县名称
	 ,if(waybill.receiver_street_code='N/A'
		     or waybill.receiver_street_code is null, '其它', waybill.receiver_street_code) receiver_street_code-- 街道编码
	 ,if(waybill.receiver_street='N/A'
		     or waybill.receiver_street is null, '其它', waybill.receiver_street) as receiver_street-- 街道名称
	 ,site.parent_site_code AS next_parent_site_code-- 上级网点编号
	 ,coalesce(site.parent_site_name,'N/A')  AS next_parent_site_name
	 ,site.first_site_code AS next_first_site_code-- 所属一级网点（知识点：二级网点的所属网点=二级网点的所属一级网点，一级网点无其所属一级网点）
	 ,site.first_site_name AS next_first_site_name
	 ,site.province_area_code AS next_province_area_code-- 省区编码
	 ,site.province_area_name AS next_province_area_name-- 省区名称
	 ,site.center_code AS next_center_code-- 分拔编码
	 ,site.center_name AS next_center_name-- 分拨名称
	 ,site.area_code AS next_area_code-- 大区编码
	 ,site.area_name AS next_area_name-- 大区名称
	 ,if(t.delivery_code='N/A'
		     or t.delivery_code is null, '其它', t.delivery_code) as delivery_code
	 ,if(t.delivery_name='N/A'
		     or t.delivery_name is null, '其它', t.delivery_name) as delivery_name
	 ,t.delivery_site_code
	 ,t.must_clock_time-- 应打卡时间
	 ,distance as site_to_center_distance-- 距离
	 ,case
	when distance > 0
		and distance <=50 then '0-50km'
	when distance > 50
		and distance <=100 then '50-100km'
	when distance > 100
		and distance <=150 then '100-150km'
	when distance > 150
		and distance <=300 then '150-300km'
	when distance > 300 then '300km以上'
	else '其它'
	  end as site_to_center_distance_range
	 ,district2center.site_to_county_distance as district_to_center_distance
	 ,COALESCE(district2center.district_to_center_distance_range,'其它') as district_to_center_distance_range
	 ,district2street.site_to_county_distance as district_to_street_distance
	 ,COALESCE(district2street.district_to_street_distance_range,'其它') as district_to_street_distance_range
	 ,t.sign_time-- 签收时间
	 ,t.sign_status
	 ,COALESCE(t.is_sign, 2) as is_sign
	 ,t.real_sign-- 是否实时签收 20：是
	 ,t.is_change_waybill
	 ,t.center_arrive_id
	 ,efficient.time_efficient-- 时效
	 ,if(efficient.time_efficient >=0, CONCAT('T+', efficient.time_efficient), '默认T+0') as time_efficient_name1
	 ,if(efficient.time_efficient >=0, CONCAT('T+', efficient.time_efficient), 'T+0') as time_efficient_name2
	 ,CURRENT_TIMESTAMP etl_time
FROM
	(SELECT mreach.waybill_no
	      ,mreach.product_type_code-- 产品类型名称
	      ,mreach.scan_site_code
	      ,mreach.scan_time
	      ,mreach.dest_site_code-- 目的网点编号
	      ,mreach.is_my_sign
	      ,mreach.sign_status
	      ,mreach.delivery_code
	      ,mreach.is_change_waybill
	      ,mreach.center_arrive_id
	      ,mreach.delivery_name
	      ,mreach.delivery_site_code
	      ,CASE
			WHEN destSiteClock.must_clock_time IS NULL THEN CASE
				WHEN mreach.dest_site_type_code = 710200 THEN CASE
					WHEN destParentSiteClock.must_clock_time IS NOT NULL THEN mreach.parent_site_code
					ELSE CASE
						WHEN mreach.parent_parent_site_type_code = 800200 THEN CASE
							WHEN destParentParentSiteClock.must_clock_time IS NOT NULL THEN mreach.parent_parent_site_code
							ELSE mreach.dest_site_code
						                                                       END
						ELSE mreach.dest_site_code
					     END
				                                              END
				ELSE CASE
					WHEN mreach.parent_site_type_code = 800200 THEN CASE
						WHEN destParentSiteClock.must_clock_time IS NOT NULL THEN mreach.parent_site_code
						ELSE mreach.dest_site_code
					                                                END
					ELSE mreach.dest_site_code
				     END
			                                                END
			ELSE mreach.dest_site_code
	       END AS next_site_code-- 下一站
	      ,CASE
			WHEN destSiteClock.must_clock_time IS NULL THEN CASE
				WHEN mreach.dest_site_type_code = 710200 THEN CASE
					WHEN destParentSiteClock.must_clock_time IS NOT NULL THEN destParentSiteClock.must_clock_time
					ELSE CASE
						WHEN mreach.parent_parent_site_type_code = 800200 THEN CASE
							WHEN destParentParentSiteClock.must_clock_time IS NOT NULL THEN destParentParentSiteClock.must_clock_time
							ELSE '18:00:00'
						                                                       END
						ELSE '18:00:00'
					     END
				                                              END
				ELSE CASE
					WHEN mreach.parent_site_type_code = 800200 THEN CASE
						WHEN destParentSiteClock.must_clock_time IS NOT NULL THEN destParentSiteClock.must_clock_time
						ELSE '18:00:00'
					                                                END
					ELSE '18:00:00'
				     END
			                                                END
			ELSE destSiteClock.must_clock_time
	       END AS must_clock_time-- 应打卡时间
	      ,sign_time-- 签收时间
	      ,is_my_sign AS is_sign
	      ,real_sign -- 是否实时签收 20：是

	 FROM lbdb4my_tx_oper_center_mreach mreach
	      LEFT JOIN lbdb4my_tx_site_clock_maintain2max destSiteClock ON mreach.scan_site_code = destSiteClock.scan_site_code
		 AND mreach.dest_site_code = destSiteClock.site_code
	      LEFT JOIN lbdb4my_tx_site_clock_maintain2max destParentSiteClock ON mreach.scan_site_code = destParentSiteClock.scan_site_code
		 AND mreach.parent_site_code = destParentSiteClock.site_code
	      LEFT JOIN lbdb4my_tx_site_clock_maintain2max destParentParentSiteClock ON mreach.scan_site_code = destParentParentSiteClock.scan_site_code
		 AND mreach.parent_parent_site_code = destParentParentSiteClock.site_code) t
	LEFT JOIN
	(	SELECT waybill_no
	          ,receiver_address-- 详细收件地址
	          ,receiver_province_code-- 省份编码
	          ,receiver_province_name-- 省份名称
	          ,receiver_city_code-- 城市编码
	          ,receiver_city_name-- 城市名称
	          ,receiver_district_code-- 区县编码
	          ,receiver_district_name-- 区县名称
	          ,receiver_street_code-- 街道编码
	          ,receiver_street-- 街道名称
	          ,calc_weight-- 结算重量
	          ,coalesce(weight_range,'N/A') as weight_range-- 公斤段
	          ,ROW_NUMBER () over(PARTITION BY waybill_no ORDER BY send_date_9to9_date DESC ) rn1
	     FROM lbdb.tx_waybill_info_ext --天象运单表
	) waybill ON t.waybill_no = waybill.waybill_no and waybill.rn1=1
	LEFT JOIN
	(SELECT center_code-- 分拨
	      ,site_code-- 网点
	      ,distance
	      ,ROW_NUMBER() OVER (PARTITION BY center_code,site_code ORDER BY update_time DESC) AS rn -- 业务库此表存在物理删
	 FROM lbdb.tx_site_delivery_distance -- 天象网点交件距离表
	) distance ON t.scan_site_code = distance.center_code
		AND t.next_site_code = distance.site_code
		AND distance.rn = 1
	LEFT JOIN tx_bd_site_to_county_distance district2center
	          on t.scan_site_code = district2center.site_code and waybill.receiver_district_code = district2center.county_code
	LEFT JOIN tx_bd_site_to_county_distance district2street
	          on waybill.receiver_street_code = district2street.site_code and waybill.receiver_district_code = district2street.county_code
	LEFT JOIN
	(SELECT province_name -- 省名称
	      ,city_name -- 市名称
	      ,district_name -- 区县名称
	      ,street_name -- 乡镇街道名称
	      ,time_efficient -- 时效
	 FROM lbdb.tb_base_time_efficient -- gis网点时效表
	 WHERE is_deleted = 0 -- 是否删除[0:否, 1:是]
	) efficient ON waybill.receiver_province_name = efficient.province_name
		AND waybill.receiver_city_name = efficient.city_name
		AND waybill.receiver_district_name = efficient.district_name
		AND waybill.receiver_street = efficient.street_name
	LEFT JOIN
	(SELECT s1.site_code-- 网点编号
	      ,s1.site_id-- 网点ID
	      ,s1.site_name-- 网点名称
	      ,s1.parent_site_code-- 上级网点编号
	      ,s2.site_name AS parent_site_name-- 上级网点名称
	      ,s3.site_type_code AS parent_site_type_code-- 上级网点类型
	      ,s1.first_site_code-- 所属一级网点（知识点：二级网点的所属网点=二级网点的所属一级网点，一级网点无其所属一级网点）
	      ,s3.site_name AS first_site_name
	      ,s1.province_area_code-- 省区编码
	      ,s1.province_area_name-- 省区名称
	      ,s1.area_code-- 大区编码
	      ,s1.area_name-- 大区名称
	      ,s1.center_code-- 分拔编码
	      ,s1.center_name-- 分拨名称
	 FROM lbdb.tx_bd_site_info s1
	      LEFT JOIN lbdb.tx_bd_site_info s2 ON s1.parent_site_code = s2.site_code
	      LEFT JOIN lbdb.tx_bd_site_info s3 ON s1.first_site_code = s3.site_code) site ON t.next_site_code = site.site_code
;
