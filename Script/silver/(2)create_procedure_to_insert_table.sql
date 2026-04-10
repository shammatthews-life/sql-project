create or alter procedure silver.load_silver as
begin
begin try
   declare @starttime datetime,@endtime datetime
    set @starttime=GETDATE()
        print '>> inserting table silver.crm_cust_info ';
        truncate table silver.crm_cust_info;
        insert into silver.crm_cust_info (cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
        select 
        cst_id,cst_key,
        trim(cst_firstname) as cst_firstname,
        trim(cst_lastname) as cst_lastname,
        case when upper(trim(cst_marital_status))='S' then 'Single'
             when upper(trim(cst_marital_status) )='M' then 'Married'
             else 'unknown'
        end cst_marital_status ,
        case when upper(trim(cst_gndr))='F' then 'Female'
             when upper(trim(cst_gndr) )='M' then 'Male'
             else 'unknown'
        end cst_gndr,cst_create_date
        from (
        select *,ROW_NUMBER() over(partition by cst_id order by cst_create_date desc ) as lastfag from bronze.crm_cust_info where cst_id is not null 
        )t where lastfag=1 



         print '>> inserting table silver.crm_prd_info';

        truncate table silver.crm_prd_info;
        insert into silver.crm_prd_info(
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt 
        )
        select 
        prd_id,
        replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
        SUBSTRING(prd_key,7,len(prd_key) ) as prd_key,
        prd_nm,
        isnull(prd_cost,0)as prd_cost,
        case when upper(Trim(prd_line)) ='M' then 'Mountain'
             when upper(trim(prd_line)) ='R' then 'Road'
             when upper(trim(prd_line)) ='S' then 'Other Sales'
             when upper(trim(prd_line))='T' then 'Touring'
             else 'unknown'
          end as prd_line,
        cast(prd_start_dt as date) as prd_start_dt ,
        cast(lead (prd_start_dt) over(partition by prd_key order by prd_start_dt ) -1 as date)as prd_end_dt
        from bronze.crm_prd_info


        print '>> inserting table silver.crm_sales_info';

        truncate table silver.crm_sales_info;
        insert into silver.crm_sales_info (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        select 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        case when sls_order_dt = 0 then  null
             when len(sls_order_dt)!=8 then null
             else cast(cast(sls_order_dt as Varchar(8)) as date)
             end sls_order_dt,                                      --****************************************
        case when sls_ship_dt = 0 then  null                        -- just in case for future purpose
             when len(sls_ship_dt)!=8 then null                     -- case is added for both ship and due date 
             else cast(cast(sls_ship_dt as Varchar(8)) as date)     --************************************
             end sls_ship_dt,
        case when sls_due_dt = 0 then  null
             when len(sls_due_dt)!=8 then null
             else cast(cast(sls_due_dt as Varchar(8)) as date)
             end sls_due_dt,
        case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity*abs(sls_price) then sls_quantity * abs(sls_price)
             else sls_sales
             end  as  sls_sales,
        sls_quantity, -- no issue in quantity 
        case when sls_price <=0 or sls_price is null then abs(sls_sales)/nullif(sls_quantity,0)
         else sls_price end as sls_price
        from bronze.crm_sales_info


        print '>> inserting table silver.erp_cust_az12';

         truncate table silver.erp_cust_az12
         insert into silver.erp_cust_az12(cid,bdate,gen)
          select 

            case when cid like 'NAS%' then  substring(cid,4,len(cid))
            else cid 
            end as cid,

           case when bdate > GETDATE() then cast(getdate() as date)
                 else bdate
                 end as bdate,
           case when upper(trim(gen)) in ('FEMALE','F') then 'Female'
                     when upper(trim(gen)) in ('MALE','M')   then 'Male'
                     else 'unknown'
                     end as gen
           from bronze.erp_cust_az12

         print '>> inserting table silver.erp_loc_a101';

        truncate table silver.erp_loc_a101
        insert into silver.erp_loc_a101(cid,cntry)
        select
        replace(cid,'-','') as cid ,
        case when upper(Trim(cntry)) ='DE' then 'Germany'
             when upper(Trim(cntry)) in ('US','USA') then 'United States'
             when trim(cntry)='' or cntry is null then 'Unknown'
             else cntry
             end as cntry
        from bronze.erp_loc_a101


         print '>> inserting table silver.erp_px_cat_g1v2';


        truncate table silver.erp_px_cat_g1v2 
        insert into silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
        select
        id,
        cat,
        subcat,
        maintenance
        from bronze.erp_px_cat_g1v2


        set @endtime=GETDATE()
        print 'time taken to insert all table in  '+cast(datediff(second,@starttime,@endtime) as nvarchar) +'Seconds'
  end try
  begin catch
  print 'error occured'
  print 'error message '+cast( error_message() as nvarchar)
  print 'error number'+ cast(error_number() as nvarchar)
  end catch
end
