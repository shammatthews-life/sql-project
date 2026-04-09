/*
note
we are inseting data to the files with truncate to avoid dublicate when the code has executed twice or already has a values a
warning
  if it already has values it will be deleted from these table names
  make sure the path in the from is correct while executing the code 

*/use warehouse;

truncate table bronze.crm_cust_info;
bulk insert bronze.crm_cust_info
from  'D:\sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (firstrow = 2,
      fieldterminator=',' ,
      tablock
      );


truncate table bronze.crm_prd_info;
bulk insert bronze.crm_prd_info
from  'D:\sql\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with (firstrow = 2,
      fieldterminator=',' ,
      tablock
      );

truncate table bronze.crm_sales_info;
bulk insert bronze.crm_sales_info
from  'D:\sql\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with (firstrow = 2,
      fieldterminator=',' ,
      tablock
      );

truncate table bronze.erp_cust_az12;
bulk insert bronze.erp_cust_az12
from  'D:\sql\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with (firstrow = 2,
      fieldterminator=',' ,
      tablock
      );

truncate table bronze.erp_loc_a101;
bulk insert bronze.erp_loc_a101
from  'D:\sql\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with (firstrow = 2,
      fieldterminator=',' ,
      tablock
      );



truncate table bronze.erp_px_cat_g1v2;
bulk insert bronze.erp_px_cat_g1v2
from  'D:\sql\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with (firstrow = 2,
      fieldterminator=',' ,
      tablock
      );
