with customer_effective_to as (
    select 
        sat_customer_details.customer_pk,
        sat_customer_details.load_date,
        sat_customer_details.first_name,
        sat_customer_details.last_name,
        sat_customer_details.email,
        sat_customer_details.effective_from,
        lead(sat_customer_details.effective_from, 1, date '9999-12-31') over(partition by sat_customer_details.customer_pk order by sat_customer_details.effective_from) as effective_to
    from  {{ref('sat_customer_details')}} as sat_customer_details
),
customer_effective_crm_to as (
    select 
        sat_customer_details_crm.customer_pk,
        sat_customer_details_crm.load_date,
        sat_customer_details_crm.country,
        sat_customer_details_crm.age,        
        sat_customer_details_crm.effective_from,
        lead(
        sat_customer_details_crm.effective_from, 
        1, date '9999-12-31')
         over(partition by sat_customer_details_crm.customer_pk 
         order by sat_customer_details_crm.effective_from) as effective_to
    from  {{ref('sat_customer_details_crm')}} as sat_customer_details_crm
)


select 
    pcd.as_of_date,
    pcd.customer_pk,
    cft.first_name,
    cft.last_name,
    cft.email,
    cft.effective_from as details_effective_from,
    cft.effective_to as details_effective_to,
    cftm.country,
    cftm.age,
    cftm.effective_from as crm_effective_from,
    cftm.effective_to as crm_effective_to

from 
    {{ref('pit_customer')}} as pcd 
    left join customer_effective_to as cft on 
        pcd.sat_customer_details_pk = cft.customer_pk
        and  pcd.sat_customer_details_ldts = cft.load_date
    left join customer_effective_crm_to as cftm 
        on pcd.sat_customer_details_crm_pk = cftm.customer_pk
         and  pcd.sat_customer_details_crm_ldts = cftm.load_date
order by 
    pcd.customer_pk,
    pcd.as_of_date