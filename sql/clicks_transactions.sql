select user_id, created_at,category,is_buy  from (
    select
    ci.*,
    m_c.category,
    t_new.is_buy,
    row_number() over (partition by ci.user_id order by created_at desc) seqnum 
    from affiliates.clicks_id ci
    
    right join(
        select ci_new.user_id from affiliates.clicks_id ci_new
        where ci_new.created_at >= %(start_date)s
        group by ci_new.user_id
    
    ) users on users.user_id = ci.user_id
    
    left join(
        select m.id, m.category from affiliates.merchants m 
    ) m_c on m_c.id = ci.merchant_id
    left join (
        select id  as is_buy,click_uuid from affiliates.transactions t 
        where t.network in ('cj','awin')
    ) t_new on t_new.click_uuid = ci.click_uuid
    where ci.merchant_id is not null and ci.created_at >= %(start_date)s
) mc
where seqnum <= %(clicks_count)s