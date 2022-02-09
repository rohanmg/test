unload
(
'
select salestage.sale_stage_name,datetable.date,
      factopt.customer_id,lead.sfdc_lead_id,
      opp.sfdc_opportunity_id,product.business_name,
      buyer.buyer_type,
      product.project_marketing_name,
      product.project_name,
      product.project_status,
      product.stage_status,
      lead.lead_status,
      opp.sales_step_name,
      address.address_usage,
      address.street_address,
      address.suburb,
      address.state_code,
      address.postcode,
      address.country_code,
      address.main_address_indicator,
      address.customer_key,
      customer.sfdc_account_id
From pbi_v.vw_current_lead as lead
left join pbi_v.vw_current_factopportunity as factopt on lead.lead_id=factopt.lead_id
left join pbi_v.vw_current_opportunity as opp on factopt.opportunity_id=opp.opportunity_id
left join pbi_v.vw_current_customer as customer on factopt.customer_id=customer.customer_id
left join pbi_v.vw_current_product as product on factopt.product_id=product.product_id
left join pbi_v.vw_buyergroup as buyer on factopt.buyer_group_id=buyer.buyer_group_id
left join pbi_v.vw_current_customeraddress address ON customer.customer_key = address.customer_key
join pbi_v.vw_salestage as salestage on factopt.sale_stage_id=salestage.sale_stage_id
join pbi_v.vw_dimdate as datetable on factopt.date_id=datetable.date_id
where (salestage.sale_stage_name=''Settlement'' or salestage.sale_stage_name=''Lead'' or salestage.sale_stage_name=''Enquiry'' or salestage.sale_stage_name=''Sale Recorded'')
and (product.business_name=''Residential'' or product.business_name=''Retirement Living'')
and buyer.buyer_type not in (''Investor - Retail'',''Builder - Retail'',''Builder'',''Private/Business Investor'')
and (address.main_address_indicator=''Y''or address.address_usage=''First Known'')
and product.project_status=''Active''
and datetable.date>Convert(datetime, ''<StartDate>'' )
'
)
to 's3://s3-stockland-sy-dl-d-ds-projects/catchment-analysis/data/interim/active_projects_quarter_all' 
iam_role 'arn:aws:iam::704984167266:role/DataLakeWarehouseRoleRedshift'
HEADER 
ALLOWOVERWRITE
CSV DELIMITER AS '|'
parallel off;

