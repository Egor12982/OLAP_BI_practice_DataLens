CREATE TABLE reports.dq_writeoffs ENGINE = ReplacingMergeTree ORDER BY dt_hour AS 
select toStartOfHour(dt_status)                 dt_hour
    , count()                                   cnt
    , uniq(office_id)                           uniq_office
    , uniq(shk_id)                              uniq_shk
    , uniq(employee_id)                         uniq_empl
    , countIf(lostreason_id, lostreason_id = 0) cnt_0_lostreason
    , countIf(lostreason_id, lostreason_id > 0) cnt_more_than_0_lostreason
    , countIf(is_found, is_found = 0)           cnt_0_is_found
    , countIf(is_found, is_found = 1)           cnt_1_is_found
from reports.writeoffs_by_customers
group by dt_hour
order by dt_hour desc;