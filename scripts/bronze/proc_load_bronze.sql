/*this script to create stored procedure to load data into bronze layer
with calculation of duration time using(variables datetime and datediff to calc the difference between start and end)
for each load and whole batch 
and using try and catch to handle errors*/

CREATE or ALTER PROCEDURE Bronze.load_bronze
as
begin
		begin try
		        declare @start_time datetime,@end_time datetime,@batch_start_time datetime,@batch_end_time datetime;
				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				print'loading bronze layer'
				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				set @batch_start_time =getdate();
				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				print'loading CRM'
				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				--------------------------------------------------load1 cust_info
				set @start_time =getdate();
				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				print'loading cust_info'
				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				truncate table Bronze.crm_cust_info;

				bulk insert Bronze.crm_cust_info
				from 'C:\Users\adel mohamedll\Desktop\sql data warehouse project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
				with(
				firstrow=2,
				fieldterminator=',',
				tablock
				);
				set @end_time=GETDATE();
				print'load duration: '+cast(datediff(second,@start_time,@end_time) as nvarchar);
				print'--------------------------';
		

				-----------------------------------------------------load2  prd_info
				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				print'loading prd_info'
				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				set @start_time =getdate();
				truncate table Bronze.crm_prd_info;

				bulk insert Bronze.crm_prd_info
				from 'C:\Users\adel mohamedll\Desktop\sql data warehouse project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
				with(
				firstrow=2,
				fieldterminator=',',
				tablock
				);
				set @end_time=GETDATE();
				print'load duration: '+cast(datediff(second,@start_time,@end_time) as nvarchar);
				print'--------------------------';
		
				---------------------------------------------------load3  sales_details

				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				print'loading sales_details'
				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				set @start_time =getdate();
				truncate table Bronze.crm_sales_details;

				bulk insert Bronze.crm_sales_details
				from 'C:\Users\adel mohamedll\Desktop\sql data warehouse project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
				with(
				firstrow=2,
				fieldterminator=',',
				tablock
				);
				set @end_time=GETDATE();
				print'load duration: '+cast(datediff(second,@start_time,@end_time) as nvarchar);
				print'--------------------------';
		

				--------------------------------------------------------load4  cust_az12

				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				print'loading cust_az12'
				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				set @start_time =getdate();
				truncate table Bronze.erp_cust_az12;

				bulk insert Bronze.erp_cust_az12
				from 'C:\Users\adel mohamedll\Desktop\sql data warehouse project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erpp\CUST_AZ12.csv'
				with(
				firstrow=2,
				fieldterminator=',',
				tablock
				);
				set @end_time=GETDATE();
				print'load duration: '+cast(datediff(second,@start_time,@end_time) as nvarchar);
				print'--------------------------';
		


				---------------------------------------------------------load   loc_a101

				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				print'loading loc_a101'
				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				set @start_time =getdate();
				truncate table Bronze.erp_loc_a101;

				bulk insert Bronze.erp_loc_a101
				from 'C:\Users\adel mohamedll\Desktop\sql data warehouse project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erpp\LOC_A101.csv'
				with(
				firstrow=2,
				fieldterminator=',',
				tablock
				);
				set @end_time=GETDATE();
				print'load duration: '+cast(datediff(second,@start_time,@end_time) as nvarchar);
				print'--------------------------';
		


				---------------------------------------------------------load      ex_g1v2

				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				print'loading px_g1v2'
				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				set @start_time =getdate();
				truncate table Bronze.erp_px_g1v2;

				bulk insert Bronze.erp_px_g1v2
				from 'C:\Users\adel mohamedll\Desktop\sql data warehouse project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erpp\PX_CAT_G1V2.csv'
				with(
				firstrow=2,
				fieldterminator=',',
				tablock
				);
				set @end_time=GETDATE();
				print'load duration: '+cast(datediff(second,@start_time,@end_time) as nvarchar);
				print'--------------------------';

				set @batch_end_time=GETDATE();
				print'load duration of bronze layer whole batch: '+cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar);
				print'--------------------------'

		end try
		begin catch
		           
				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				print'error in loading bronze layer'
				print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
				print'error message'+error_message();
				print'error message'+cast(error_number() as nvarchar);
				print'error message'+cast(error_state() as nvarchar);
		end catch
		   
		
end;
