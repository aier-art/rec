`seen_click_fav` 表的主键是 cid,rid ，插入的时候，如果冲突就会改为累加，然后更新 ts，根据 ts，更新权重。
