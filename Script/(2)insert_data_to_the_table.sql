/*
note
we are inseting data to the files with truncate to avoid dublicate when the code has executed twice or already has a values 
used try and catch to handle error 
can calculate the time taken 
warning
  if it already has values it will be deleted from these table names
  make sure the path in the from is correct while executing the code 

*/
-- to check the message and confirm the insert of the data exec bronze.load_bronze;
use warehouse;
go
create or alter procedure bronze.load_bronze as
begin 
 begin try
   declare @starttime datetime,@endtime datetime;
   set @starttime = GETDATE();
     print'=====================';
     print 'loading bronze data ';
     print '=====================';

     print '------------------';
     print 'loading crm tables';
     print '-------------------';

        truncate table bronze.crm_cust_info;
        print 'Inserting date : bronze.crm_cust_info ';
        bulk insert bronze.crm_cust_info
        from  'D:\sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        with (firstrow = 2,
              fieldterminator=',' ,
              tablock
              );


        truncate table bronze.crm_prd_info;
        print 'Inserting date : bronze.crm_prd_info ';
        bulk insert bronze.crm_prd_info
        from  'D:\sql\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        with (firstrow = 2,
              fieldterminator=',' ,
              tablock
              );

        truncate table bronze.crm_sales_info;
        print 'Inserting date : bronze.crm_sales_info ';
        bulk insert bronze.crm_sales_info
        from  'D:\sql\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        with (firstrow = 2,
              fieldterminator=',' ,
              tablock
              );


        print '------------------';
        print 'loading ERP tables';
        print '-------------------';        


        truncate table bronze.erp_cust_az12;
        print 'Inserting date : bronze.erp_cust_az12 ';
        bulk insert bronze.erp_cust_az12
        from  'D:\sql\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        with (firstrow = 2,
              fieldterminator=',' ,
              tablock
              );

        truncate table bronze.erp_loc_a101;
        print 'Inserting date : bronze.erp_loc_a101 ';
        bulk insert bronze.erp_loc_a101
        from  'D:\sql\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        with (firstrow = 2,
              fieldterminator=',' ,
              tablock
              );



        truncate table bronze.erp_px_cat_g1v2;
        print 'Inserting date : bronze.erp_px_cat_g1v2 ';
        bulk insert bronze.erp_px_cat_g1v2
        from  'D:\sql\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        with (firstrow = 2,
              fieldterminator=',' ,
              tablock
              );
        set @endtime =GETDATE();
        print 'time taken in seconds  : '+  cast(datediff(second,@starttime,@endtime) as nvarchar)
  end try
  begin catch
     print 'error occured';
     print 'error message'+cast(error_message() as nvarchar);
     print 'error number' +cast(error_number() as nvarchar);
   end catch
end;
go
