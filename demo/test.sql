CREATE table sch_group_rela
(
    school_id VARCHAR(20) not null COMMENT '学校id',
    group_id  VARCHAR(20) default null COMMENT '分组id',
    utime     timestamp   DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (school_id)
)SHARD_ROW_ID_BITS = 4 ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin;