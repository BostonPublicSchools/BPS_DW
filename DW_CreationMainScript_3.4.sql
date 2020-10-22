USE EdFiDW
GO

--Dim School
--------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimSchool_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	
		TRUNCATE TABLE Staging.[School]
		INSERT INTO Staging.[School]
				   ([_sourceKey]
				   ,[DistrictSchoolCode]
				   ,[StateSchoolCode]
				   ,[UmbrellaSchoolCode]
				   ,[ShortNameOfInstitution]
				   ,[NameOfInstitution]
				   ,[SchoolCategoryType]
				   ,[SchoolCategoryType_Elementary_Indicator]
				   ,[SchoolCategoryType_Middle_Indicator]
				   ,[SchoolCategoryType_HighSchool_Indicator]
				   ,[SchoolCategoryType_Combined_Indicator]       
				   ,[SchoolCategoryType_Other_Indicator]
				   ,[TitleIPartASchoolDesignationTypeCodeValue]
				   ,[TitleIPartASchoolDesignation_Indicator]
				   ,OperationalStatusTypeDescriptor_CodeValue
				   ,OperationalStatusTypeDescriptor_Description		   

				   ,SchoolNameModifiedDate
 				   ,SchoolOperationalStatusTypeModifiedDate
				   ,SchoolCategoryModifiedDate 
				   ,SchoolTitle1StatusModifiedDate

				   ,[ValidFrom]
				   ,[ValidTo]
				   ,[IsCurrent])
        --declare @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate()
		SELECT  DISTINCT 
			    CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),s.SchoolId)) AS [_sourceKey],
				eoic_sch.IdentificationCode AS DistrictSchoolCode,
				eoic.IdentificationCode AS StateSchoolCode,
				CASE
					WHEN eoic_sch.IdentificationCode IN (1291, 1292, 1293, 1294) THEN '1290'
					when eoic_sch.IdentificationCode IN (1440,1441) THEN '1440' 
					WHEN eoic_sch.IdentificationCode IN (4192,4192) THEN '4192' 
					WHEN eoic_sch.IdentificationCode IN (4031,4033) THEN '4033' 
					WHEN eoic_sch.IdentificationCode IN (1990,1991) THEN '1990' 
					WHEN eoic_sch.IdentificationCode IN (1140,4391) THEN '1140' 
					ELSE eoic_sch.IdentificationCode
				END AS UmbrellaSchoolCode,
				edorg.ShortNameOfInstitution, 
				edorg.NameOfInstitution,
				sc_d.CodeValue AS SchoolCategoryType, 
				CASE  WHEN sc_d.CodeValue  IN ('Elementary School') THEN 1 ELSE 0 END  [SchoolCategoryType_Elementary_Indicator],
				CASE  WHEN sc_d.CodeValue  IN ('Middle School') THEN 1 ELSE 0 END  [SchoolCategoryType_Middle_Indicator],
				CASE  WHEN sc_d.CodeValue  IN ('High School') THEN 1 ELSE 0 END  [SchoolCategoryType_HighSchool_Indicator],
				CASE  WHEN sc_d.CodeValue  NOT IN ('Elementary School','Middle School','High School') THEN 1 ELSE 0 END  [SchoolCategoryType_Combined_Indicator],
				0  [SchoolCategoryType_Other_Indicator],
				COALESCE(t1_d.CodeValue,'N/A') AS TitleIPartASchoolDesignationTypeCodeValue,
				CASE WHEN t1_d.CodeValue NOT IN ('Not designated as a Title I Part A school','N/A') THEN 1 ELSE 0 END AS TitleIPartASchoolDesignation_Indicator,
				COALESCE(os_d.CodeValue,'N/A') AS OperationalStatusTypeDescriptor_CodeValue,	
				COALESCE(os_d.[Description],'N/A') AS OperationalStatusTypeDescriptor_Description,
				 
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(edorg.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS SchoolNameModifiedDate,
 				CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(os_d.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS SchoolOperationalStatusTypeModifiedDate,
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(sc_d.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS SchoolCategoryModifiedDate,
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(t1_d.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS SchoolTitle1StatusModifiedDate,

				--Making sure the first time, the ValidFrom is set to beginning of time 
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN
				           (SELECT MAX(t) FROM
                             (VALUES
                               (edorg.LastModifiedDate)
                             , (os_d.LastModifiedDate)
                             , (sc_d.LastModifiedDate)
                             , (t1_d.LastModifiedDate)                             
                             ) AS [MaxLastModifiedDate](t)
                           )
					ELSE 
					      '07/01/2015' -- setting the validFrom to beggining of time during thre first load. 
				END AS ValidFrom,
				'12/31/9999' AS ValidTo,
				CASE WHEN COALESCE(os_d.CodeValue,'N/A') IN ('Active','Added','Changed Agency','Continuing','New','Reopened') THEN 1  ELSE 0  END AS IsCurrent		
		--SELECT distinct *
		FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.School s
		     INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.EducationOrganization edorg on s.SchoolId = edorg.EducationOrganizationId
		     INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor os_d ON edorg.OperationalStatusDescriptorId = os_d.DescriptorId
		     LEFT JOIN  [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.SchoolCategory sc on s.SchoolId = sc.SchoolId
		     LEFT JOIN  [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor sc_d on sc.SchoolCategoryDescriptorId = sc_d.DescriptorId
		     LEFT JOIN  [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor t1_d on s.TitleIPartASchoolDesignationDescriptorId = t1_d.DescriptorId
		     LEFT JOIN  [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.EducationOrganizationIdentificationCode eoic ON edorg.EducationOrganizationId = eoic.EducationOrganizationId 
		     																			   AND eoic.EducationOrganizationIdentificationSystemDescriptorId = 433 --state code
		     LEFT JOIN  [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.EducationOrganizationIdentificationCode eoic_sch ON edorg.EducationOrganizationId = eoic_sch.EducationOrganizationId 
																					   AND eoic_sch.EducationOrganizationIdentificationSystemDescriptorId = 428 --district code
		WHERE 
			(edorg.LastModifiedDate > @LastLoadDate AND edorg.LastModifiedDate <= @NewLoadDate) OR
			(os_d.LastModifiedDate > @LastLoadDate AND os_d.LastModifiedDate <= @NewLoadDate) OR
			(sc_d.LastModifiedDate > @LastLoadDate AND sc_d.LastModifiedDate <= @NewLoadDate) OR
			(t1_d.LastModifiedDate > @LastLoadDate AND t1_d.LastModifiedDate <= @NewLoadDate) 	
			
			
			
		--loading legacy data if it has not been loaded.
		--load types are ignored as this data will only be loaded once.
		IF NOT EXISTS(SELECT 1 
		              FROM dbo.DimSchool 
					  WHERE CHARINDEX('LegacyDW',_sourceKey,1) > 0)
			BEGIN
			   INSERT INTO Staging.[School]
				   ([_sourceKey]
				   ,[DistrictSchoolCode]
				   ,[StateSchoolCode]
				   ,[UmbrellaSchoolCode]
				   ,[ShortNameOfInstitution]
				   ,[NameOfInstitution]
				   ,[SchoolCategoryType]
				   ,[SchoolCategoryType_Elementary_Indicator]
				   ,[SchoolCategoryType_Middle_Indicator]
				   ,[SchoolCategoryType_HighSchool_Indicator]
				   ,[SchoolCategoryType_Combined_Indicator]       
				   ,[SchoolCategoryType_Other_Indicator]
				   ,[TitleIPartASchoolDesignationTypeCodeValue]
				   ,[TitleIPartASchoolDesignation_Indicator]
				   ,OperationalStatusTypeDescriptor_CodeValue
				   ,OperationalStatusTypeDescriptor_Description		   

				   ,SchoolNameModifiedDate
 				   ,SchoolOperationalStatusTypeModifiedDate
				   ,SchoolCategoryModifiedDate 
				   ,SchoolTitle1StatusModifiedDate

				   ,[ValidFrom]
				   ,[ValidTo]
				   ,[IsCurrent])
			 SELECT DISTINCT 
				    CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),LTRIM(RTRIM(sd.sch)))) AS [_sourceKey],
					LTRIM(RTRIM(sd.sch)) AS [DistrictSchoolCode],
					CASE WHEN ISNULL(LTRIM(RTRIM(statecd)),'N/A') IN ('','N/A') THEN 'N/A' ELSE ISNULL(LTRIM(RTRIM(statecd)),'N/A') END AS StateSchoolCode,
					CASE
						WHEN LTRIM(RTRIM(sd.sch)) IN ('1291', '1292', '1293', '1294') THEN '1290'
						when LTRIM(RTRIM(sd.sch)) IN ('1440','1441') THEN '1440' 
						WHEN LTRIM(RTRIM(sd.sch)) IN ('4192','4192') THEN '4192' 
						WHEN LTRIM(RTRIM(sd.sch)) IN ('4031','4033') THEN '4033' 
						WHEN LTRIM(RTRIM(sd.sch)) IN ('1990','1991') THEN '1990' 
						WHEN LTRIM(RTRIM(sd.sch)) IN ('1140','4391') THEN '1140' 
						ELSE LTRIM(RTRIM(sd.sch))
					END AS UmbrellaSchoolCode,
					LTRIM(RTRIM(sd.[schname_f]))  AS ShortNameOfInstitution, 
					LTRIM(RTRIM(sd.[schname_f])) AS NameOfInstitution,
					'Combined' AS SchoolCategoryType, 
					0  [SchoolCategoryType_Elementary_Indicator],
					0  [SchoolCategoryType_Middle_Indicator],
					0  [SchoolCategoryType_HighSchool_Indicator],
					1  [SchoolCategoryType_Combined_Indicator],
					0  [SchoolCategoryType_Other_Indicator],
					'N/A' AS TitleIPartASchoolDesignationTypeCodeValue,
					0 AS TitleIPartASchoolDesignation_Indicator,
					'Inactive' AS OperationalStatusTypeDescriptor_CodeValue,	
					'Inactive' AS OperationalStatusTypeDescriptor_Description,

					'07/01/2015' AS SchoolNameModifiedDate,
 				    '07/01/2015' AS SchoolOperationalStatusTypeModifiedDate,
				    '07/01/2015' AS SchoolCategoryModifiedDate,
				    '07/01/2015' AS SchoolTitle1StatusModifiedDate,

					'07/01/2015' AS ValidFrom,
					GETDATE() AS ValidTo,
					0 AS IsCurrent
				--SELECT *
				FROM [Raw_LegacyDW].[SchoolData] sd
				WHERE NOT EXISTS(SELECT 1 
									FROM Staging.[School] ds 
									WHERE 'Ed-Fi|' + Convert(NVARCHAR(MAX),LTRIM(RTRIM(sd.sch))) = ds._sourceKey);
			END

		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

	
	END CATCH;
END;
GO

-- Dimn Time
-------------------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimTime_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY

		  

		TRUNCATE TABLE Staging.[Time]

		IF CONVERT(date, @LastLoadDate) < CONVERT(date, getdate())
		 BEGIN

			--non-school
			DECLARE @NonSchoolTime TABLE
			(
			  SchoolDate DATE NOT NULL , -- 9/1/2019
			  SchoolDate_MMYYYY CHAR(6) NOT NULL,
			  SchoolDate_Fomat1 CHAR(10) NOT NULL,
			  SchoolDate_Fomat2 CHAR(8) NOT NULL,
			  SchoolDate_Fomat3 CHAR(10) NOT NULL,
    
			  SchoolYear SMALLINT NOT NULL, -- ex: 9/1/2019 = 2020  
			  SchoolYearDescription NVARCHAR(50) NOT NULL, -- '2019-2020 or SchoolYear 2019 - 2020'
			  CalendarYear SMALLINT NOT NULL, -- ex: 9/1/2019 = 2020
  
			  DayOfMonth TINYINT NOT NULL, -- 1 - 30|31
			  DaySuffix CHAR(2) NOT NULL , -- 1st, 2nd, 3rd
			  DayName  NVARCHAR(15) NOT NULL, -- Monday, Tuesday    
			  DayNameShort NVARCHAR(15) NOT NULL, -- Mon, Tue
			  DayOfWeek TINYINT NOT NULL, -- 1 - 7
  
			  WeekInMonth TINYINT NOT NULL, -- 1 - 4 or 5 -- this counts 7 days starting on the 1st of the month
			  WeekOfMonth TINYINT NOT NULL, -- 1 - 4 or 5 -- this is the actual week of the month (starting on sunday)
			  Weekend_Indicator BIT NOT NULL,
			  WeekOfYear TINYINT NOT NULL, -- 1 - 53   
			  FirstDayOfWeek DATE NOT NULL,
			  LastDayOfWeek DATE NOT NULL,
			  WeekBeforeChristmas_Indicator BIT NOT NULL, --  True,False
  
  
			  [Month] TINYINT NOT NULL, -- 1..12
			  MonthName  NVARCHAR(10) NOT NULL, --January,February,December
			  MonthNameShort CHAR(3) NOT NULL, --Jan,Feb,Dec  
			  FirstDayOfMonth DATE NOT NULL,
			  LastDayOfMonth DATE NOT NULL,
			  FirstDayOfNextMonth DATE NOT NULL,
			  LastDayOfNextMonth DATE NOT NULL,
    
			  DayOfYear SMALLINT NOT NULL, -- 1 - 365 or 366 (Leap Year Every Four Years)  
			  LeapYear_Indicator BIT NOT NULL,  
    
			  FederalHolidayName NVARCHAR(20) NULL, -- Memorial Day
			  FederalHoliday_Indicator BIT NOT NULL --  True,False  
			);




			DECLARE @startDate DATE = @LastLoadDate;
			DECLARE @endDate DATE = @NewLoadDate;

			--generating time fields that don't vary by school;
			WITH daySeqs (daySeqNumber)
			AS (SELECT 0
				UNION ALL
				SELECT daySeqNumber + 1
				FROM daySeqs
				WHERE daySeqNumber < DATEDIFF(DAY, @startDate, @endDate)),
				 theDates (theDate)
			AS (SELECT DATEADD(DAY, daySeqNumber, @startDate)
				FROM daySeqs),
				 src
			AS (SELECT TheDate = CONVERT(DATE, theDate),
					   TheDay = DATEPART(DAY, theDate),
					   TheDayName = DATENAME(WEEKDAY, theDate),
					   TheWeek = DATEPART(WEEK, theDate),
					   TheDayOfWeek = DATEPART(WEEKDAY, theDate),
					   TheMonth = DATEPART(MONTH, theDate),
					   TheMonthName = DATENAME(MONTH, theDate),
					   TheQuarter = DATEPART(QUARTER, theDate),
					   TheYear = DATEPART(YEAR, theDate),
					   TheFirstOfMonth = DATEFROMPARTS(YEAR(theDate), MONTH(theDate), 1),
					   TheLastOfYear = DATEFROMPARTS(YEAR(theDate), 12, 31),
					   TheDayOfYear = DATEPART(DAYOFYEAR, theDate)
				FROM theDates),
				 timeDim
			AS (SELECT [SchoolDate] = TheDate,
					   SchoolDate_MMYYYY = CONVERT(CHAR(2), CONVERT(CHAR(8), TheDate, 101)) + CONVERT(CHAR(4), TheYear),
					   SchoolDate_Fomat1 = CONVERT(CHAR(10), TheDate, 101),
					   SchoolDate_Fomat2 = CONVERT(CHAR(8), TheDate, 112),
					   SchoolDate_Fomat3 = CONVERT(CHAR(10), TheDate, 120),
					   SchoolYear = dbo.Func_ETL_GetSchoolYear(TheDate),
					   SchoolYearDescription = CONVERT(NVARCHAR(MAX), dbo.Func_ETL_GetSchoolYear(TheDate) - 1) + ' - '
											   + CONVERT(NVARCHAR(MAX), dbo.Func_ETL_GetSchoolYear(TheDate)),
					   CalendarYear = TheYear,
					   [DayOfMonth] = TheDay,
					   DaySuffix = CONVERT(   CHAR(2),
											  CASE
												  WHEN TheDay / 10 = 1 THEN
													  'th'
												  ELSE
													  CASE RIGHT(TheDay, 1)
														  WHEN '1' THEN
															  'st'
														  WHEN '2' THEN
															  'nd'
														  WHEN '3' THEN
															  'rd'
														  ELSE
															  'th'
													  END
											  END
										  ),
					   [DayName] = TheDayName,
					   DayNameShort = FORMAT(TheDate, 'ddd'),
					   [DayOfWeek] = TheDayOfWeek,
					   WeekInMonth = CONVERT(
												TINYINT,
												ROW_NUMBER() OVER (PARTITION BY TheFirstOfMonth, TheDayOfWeek ORDER BY TheDate)
											),
					   WeekOfMonth = CONVERT(TINYINT, DENSE_RANK() OVER (PARTITION BY TheYear, TheMonth ORDER BY TheWeek)),
					   Weekend_Indicator = CASE
											   WHEN TheDayOfWeek IN (   CASE @@DATEFIRST
																			WHEN 1 THEN
																				6
																			WHEN 7 THEN
																				1
																		END, 7
																	) THEN
												   1
											   ELSE
												   0
										   END,
					   WeekOfYear = TheWeek,
					   FirstDayOfWeek = DATEADD(DAY, 1 - TheDayOfWeek, TheDate),
					   LastDayOfWeek = DATEADD(DAY, 6, DATEADD(DAY, 1 - TheDayOfWeek, TheDate)),
					   WeekBeforeChristmas_Indicator = CASE
														   WHEN DATEPART(WEEK, TheDate) = DATEPART(WEEK, '12-25-' + CONVERT(NVARCHAR(4),TheYear)) - 1 THEN
															   1
														   ELSE
															   0
													   END,
					   [Month] = TheMonth,
					   [MonthName] = TheMonthName,
					   MonthNameShort = FORMAT(TheDate, 'MMM'),
					   FirstDayOfMonth = TheFirstOfMonth,
					   LastDayOfMonth = MAX(TheDate) OVER (PARTITION BY TheYear, TheMonth),
					   FirstDayOfNextMonth = DATEADD(MONTH, 1, TheFirstOfMonth),
					   LastDayOfNextMonth = DATEADD(DAY, -1, DATEADD(MONTH, 2, TheFirstOfMonth)),
					   [DayOfYear] = TheDayOfYear,
					   DayOfSchoolYear = TheDayOfYear, --change this 
					   LeapYear_Indicator = CONVERT(   BIT,
													   CASE
														   WHEN (TheYear % 400 = 0)
																OR
																(
																	TheYear % 4 = 0
																	AND TheYear % 100 <> 0
																) THEN
															   1
														   ELSE
															   0
													   END
												   ),
					   FederalHolidayName = [dbo].[Func_ETL_GetHolidayFromDate](TheDate), -- Memorial Day, 4th of July
					   FederalHoliday_Indicator = (CASE
														WHEN [dbo].[Func_ETL_GetHolidayFromDate](TheDate) = 'Non-Holiday' THEN
															0
														ELSE
															1
													END
												   )                                   --  True,False			  


				FROM src)

			INSERT INTO @NonSchoolTime ([SchoolDate]
									   ,[SchoolDate_MMYYYY]
									   ,[SchoolDate_Fomat1]
									   ,[SchoolDate_Fomat2]
									   ,[SchoolDate_Fomat3]
									   ,[SchoolYear]
									   ,[SchoolYearDescription]
									   ,[CalendarYear]
									   ,[DayOfMonth]
									   ,[DaySuffix]
									   ,[DayName]
									   ,[DayNameShort]
									   ,[DayOfWeek]
									   ,[WeekInMonth]
									   ,[WeekOfMonth]
									   ,[Weekend_Indicator]
									   ,[WeekOfYear]
									   ,[FirstDayOfWeek]
									   ,[LastDayOfWeek]
									   ,[WeekBeforeChristmas_Indicator]
									   ,[Month]
									   ,[MonthName]
									   ,[MonthNameShort]
									   ,[FirstDayOfMonth]
									   ,[LastDayOfMonth]
									   ,[FirstDayOfNextMonth]
									   ,[LastDayOfNextMonth]
									   ,[DayOfYear]
									   ,[LeapYear_Indicator]
									   ,[FederalHolidayName]
									   ,[FederalHoliday_Indicator])
			SELECT [SchoolDate]
				  ,[SchoolDate_MMYYYY]
				  ,[SchoolDate_Fomat1]
				  ,[SchoolDate_Fomat2]
				  ,[SchoolDate_Fomat3]
				  ,[SchoolYear]
				  ,[SchoolYearDescription]
				  ,[CalendarYear]
				  ,[DayOfMonth]
				  ,[DaySuffix]
				  ,[DayName]
				  ,[DayNameShort]
				  ,[DayOfWeek]
				  ,[WeekInMonth]
				  ,[WeekOfMonth]
				  ,[Weekend_Indicator]
				  ,[WeekOfYear]
				  ,[FirstDayOfWeek]
				  ,[LastDayOfWeek]
				  ,[WeekBeforeChristmas_Indicator]
				  ,[Month]
				  ,[MonthName]
				  ,[MonthNameShort]
				  ,[FirstDayOfMonth]
				  ,[LastDayOfMonth]
				  ,[FirstDayOfNextMonth]
				  ,[LastDayOfNextMonth]
				  ,[DayOfYear]
				  ,[LeapYear_Indicator]
				  ,[FederalHolidayName]
				  ,[FederalHoliday_Indicator]
			FROM timeDim
			ORDER BY [SchoolDate]
			OPTION (MAXRECURSION 0);

			--;WITH EdFiSchools AS
			--( 
				--DECLARE @startDate DATE = '20150701';    
				SELECT DISTINCT 
						cd.Date as SchoolDate, 	
					   CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),s.SchoolId)) AS [_sourceKey],
					   --ses.SessionName,
					   td.CodeValue TermDescriptorCodeValue, 
					   td.Description TermDescriptorDescription,       
					   cedv.CodeValue CalendarEventTypeCodeValue,
					   cedv.Description CalendarEventTypeDescription, 
					   ses.LastModifiedDate AS SchoolSessionModifiedDate, -- school sessions changes are ignored for BPS
					   cedv.LastModifiedDate AS CalendarEventTypeModifiedDate,
					   DENSE_RANK() OVER (PARTITION BY ses.SchoolYear, s.SchoolId ORDER BY cd.Date) AS DayOfSchoolYear  INTO #EdFiSchools
				--select *
				FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.School s
					INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.EducationOrganization edOrg  ON s.SchoolId = edOrg.EducationOrganizationId
					INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.CalendarDate cd ON s.SchoolId = cd.SchoolId
					INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.CalendarDateCalendarEvent cdce ON cd.SchoolId = cdce.SchoolId
																										    AND cd.Date = cdce.Date
					INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.CalendarEventDescriptor ced  ON cdce.CalendarEventDescriptorId = ced.CalendarEventDescriptorId
					INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor cedv  ON ced.CalendarEventDescriptorId = cedv.DescriptorId					
					INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Session ses ON s.SchoolId = ses.SchoolId
																					     AND cd.Date BETWEEN ses.BeginDate AND ses.EndDate
					INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor td ON ses.TermDescriptorId = td.DescriptorId

				WHERE  cd.Date >= @startDate AND 
					  (
					   (ses.LastModifiedDate > @LastLoadDate AND ses.LastModifiedDate <= @NewLoadDate) OR 
					   (cedv.LastModifiedDate > @LastLoadDate AND cedv.LastModifiedDate <= @NewLoadDate)
					  )
			--)
			
		
		

			INSERT INTO Staging.[Time]
					   ([SchoolDate]
					   ,[SchoolDate_MMYYYY]
					   ,[SchoolDate_Fomat1]
					   ,[SchoolDate_Fomat2]
					   ,[SchoolDate_Fomat3]
					   ,[SchoolYear]
					   ,[SchoolYearDescription]
					   ,[CalendarYear]
					   ,[DayOfMonth]
					   ,[DaySuffix]
					   ,[DayName]
					   ,[DayNameShort]
					   ,[DayOfWeek]
					   ,[WeekInMonth]
					   ,[WeekOfMonth]
					   ,[Weekend_Indicator]
					   ,[WeekOfYear]
					   ,[FirstDayOfWeek]
					   ,[LastDayOfWeek]
					   ,[WeekBeforeChristmas_Indicator]
					   ,[Month]
					   ,[MonthName]
					   ,[MonthNameShort]
					   ,[FirstDayOfMonth]
					   ,[LastDayOfMonth]
					   ,[FirstDayOfNextMonth]
					   ,[LastDayOfNextMonth]
					   ,[DayOfYear]
					   ,[LeapYear_Indicator]
					   ,[FederalHolidayName]
					   ,[FederalHoliday_Indicator]
                   
					   ,SchoolSourceKey
					   ,DayOfSchoolYear
					   ,SchoolCalendarEventType_CodeValue
					   ,SchoolCalendarEventType_Description
					   ,SchoolTermDescriptor_CodeValue
					   ,SchoolTermDescriptor_Description
		           
					   ,SchoolSessisonModifiedDate
					   ,CalendarEventTypeModifiedDate

					   ,[ValidFrom]
					   ,[ValidTo]
					   ,[IsCurrent])
			select nst.[SchoolDate]
				  ,nst.[SchoolDate_MMYYYY]
				  ,nst.[SchoolDate_Fomat1]
				  ,nst.[SchoolDate_Fomat2]
				  ,nst.[SchoolDate_Fomat3]
				  ,nst.[SchoolYear]
				  ,nst.[SchoolYearDescription]
				  ,nst.[CalendarYear]
				  ,nst.[DayOfMonth]
				  ,nst.[DaySuffix]
				  ,nst.[DayName]
				  ,nst.[DayNameShort]
				  ,nst.[DayOfWeek]
				  ,nst.[WeekInMonth]
				  ,nst.[WeekOfMonth]
				  ,nst.[Weekend_Indicator]
				  ,nst.[WeekOfYear]
				  ,nst.[FirstDayOfWeek]
				  ,nst.[LastDayOfWeek]
				  ,nst.[WeekBeforeChristmas_Indicator]
				  ,nst.[Month]
				  ,nst.[MonthName]
				  ,nst.[MonthNameShort]
				  ,nst.[FirstDayOfMonth]
				  ,nst.[LastDayOfMonth]
				  ,nst.[FirstDayOfNextMonth]
				  ,nst.[LastDayOfNextMonth]
				  ,nst.[DayOfYear]
				  ,nst.[LeapYear_Indicator]
				  ,nst.[FederalHolidayName]
				  ,nst.[FederalHoliday_Indicator]

				  ,es.[_sourceKey] AS SchoolSourceKey
				  ,es.DayOfSchoolYear
				  ,es.CalendarEventTypeCodeValue
				  ,es.CalendarEventTypeDescription
				  ,es.TermDescriptorCodeValue
				  ,es.TermDescriptorDescription	  

				  ,COALESCE(es.SchoolSessisonModifiedDate,'07/01/2015') AS SchoolSessisonModifiedDate -- school sessions are ignore for BPS
				  ,COALESCE(es.CalendarEventTypeModifiedDate,'07/01/2015')  AS CalendarEventTypeModifiedDate
 
				  ,CASE WHEN @LastLoadDate <> '07/01/2015' THEN
							  COALESCE(
							  (SELECT MAX(t) FROM
								 (VALUES
								   (es.SchoolSessionModifiedDate)
								 , (es.CalendarEventTypeModifiedDate)                             
								 ) AS [MaxLastModifiedDate](t)
							   ),'07/01/2015')
						ELSE 
							  '07/01/2015' -- setting the validFrom to beginning of time during thre first load. 
					END AS ValidFrom
				   ,'12/31/9999'   AS ValidTo
				   , 1 AS IsCurrent
			FROM @NonSchoolTime nst
				 LEFT JOIN #EdFiSchools es ON nst.SchoolDate = es.SchoolDate			 
			DROP table #EdFiSchools
		 END

		


			
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		
	END CATCH;
END;
GO

--Dim Student
--------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimStudent_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY

		--BEGIN TRANSACTION;   

		TRUNCATE TABLE Staging.[Student]

		SELECT DISTINCT 
			   s.StudentUSI, 
			   COUNT(sr.StudentUSI) AS RaceCount,
			   STRING_AGG(rt.CodeValue,',') AS RaceCodes,
			   STRING_AGG(rt.Description,',') AS RaceDescriptions,
			   CASE WHEN EXISTS (SELECT 1 
								 FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentRace sr
									   WHERE s.StudentUSI = sr.StudentUSI
										 AND sr.RaceTypeId = 1) THEN 1
			   ELSE 
				   0	             
			   END AS Race_AmericanIndianAlaskanNative_Indicator,
			   CASE WHEN EXISTS (SELECT 1 
								 FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentRace sr
									   WHERE s.StudentUSI = sr.StudentUSI
										 AND sr.RaceTypeId = 2) THEN 1
			   ELSE 
				   0	             
			   END AS Race_Asian_Indicator,
			   CASE WHEN EXISTS (SELECT 1 
								 FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentRace sr
									   WHERE s.StudentUSI = sr.StudentUSI
										 AND sr.RaceTypeId = 3) THEN 1
			   ELSE 
				   0	             
			   END AS Race_BlackAfricaAmerican_Indicator,
			   CASE WHEN EXISTS (SELECT 1 
								 FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentRace sr
									   WHERE s.StudentUSI = sr.StudentUSI
										 AND sr.RaceTypeId = 5) THEN 1
			   ELSE 
				   0	             
			   END AS Race_NativeHawaiianPacificIslander_Indicator,
			   CASE WHEN EXISTS (SELECT 1 
								 FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentRace sr
									   WHERE s.StudentUSI = sr.StudentUSI
										 AND sr.RaceTypeId = 7) THEN 1
			   ELSE 
				   0	             
			   END AS Race_White_Indicator,
			   CASE WHEN EXISTS (SELECT 1 
								 FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentRace sr
									   WHERE s.StudentUSI = sr.StudentUSI
										 AND sr.RaceTypeId = 4) THEN 1
			   ELSE 
				   0	             
			   END AS Race_ChooseNotRespond_Indicator,
			   CASE WHEN EXISTS (SELECT 1 
								 FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentRace sr
									   WHERE s.StudentUSI = sr.StudentUSI
										 AND sr.RaceTypeId = 6) THEN 1
			   ELSE 
				   0	             
			   END AS Race_Other_Indicator into #StudentRaces    
        --select * 
		FROM  [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Student s
			  LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentEducationOrganizationAssociationRace sr ON s.StudentUSI = sr.StudentUSI		
			  LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor d ON sr.RaceDescriptorId = d.DescriptorId
	    WHERE (s.LastModifiedDate > @LastLoadDate AND s.LastModifiedDate <= @NewLoadDate) --OR
			  --(rt.LastModifiedDate > @LastLoadDate AND rt.LastModifiedDate <= @NewLoadDate)
		GROUP BY s.StudentUSI, s.HispanicLatinoEthnicity
				
		

		--;WITH StudentHomeRooomByYear AS
		--(
			SELECT DISTINCT std_sa.StudentUSI, 
							std_sa.SchoolYear, 
							std_sa.SchoolId,  
							std_sa.LocalCourseCode AS HomeRoom,
							dbo.Func_ETL_GetFullName(staff.FirstName,staff.MiddleName,staff.LastSurname) AS HomeRoomTeacher,
							ROW_NUMBER() OVER (PARTITION BY std_sa.StudentUSI, 
															std_sa.SchoolYear, 
															std_sa.SchoolId ORDER BY staff_sa.BeginDate DESC) AS RowRankId INTO #StudentHomeRooomByYear
			FROM  [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Student s
			INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentSectionAssociation std_sa ON s.StudentUSI = std_sa.StudentUSI			
				 INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StaffSectionAssociation staff_sa  ON std_sa.SectionIdentifier = staff_sa.SectionIdentifier
																										AND std_sa.SchoolYear = staff_sa.SchoolYear
				 INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Staff staff on staff_sa.StaffUSI = staff.StaffUSI
			WHERE std_sa.HomeroomIndicator = 1
				 AND std_sa.SchoolYear >= 2019
				 AND std_sa.EndDate > GETDATE()
				 --AND s.StudentUniqueId = 269159 
				 AND (
				       (s.LastModifiedDate > @LastLoadDate AND s.LastModifiedDate <= @NewLoadDate) 
				     )
					 
        --)
			

		

		
		INSERT INTO Staging.[Student]
				   ([_sourceKey]
				   ,[PrimaryElectronicMailAddress]
				   ,[PrimaryElectronicMailTypeDescriptor_CodeValue]
				   ,[PrimaryElectronicMailTypeDescriptor_Description]
				   ,[StudentUniqueId]
				   ,[StateId]
				   ,[SchoolKey]
				   ,[ShortNameOfInstitution]
				   ,[NameOfInstitution]
				   ,[GradeLevelDescriptor_CodeValue]
				   ,[GradeLevelDescriptor_Description]
				   ,[FirstName]
				   ,[MiddleInitial]
				   ,[MiddleName]
				   ,[LastSurname]
				   ,[FullName]
				   ,[BirthDate]
				   ,[StudentAge]
				   ,[GraduationSchoolYear]
				   ,[Homeroom]
				   ,[HomeroomTeacher]
				   ,[SexType_Code]
				   ,[SexType_Description]
				   ,[SexType_Male_Indicator]
				   ,[SexType_Female_Indicator]
				   ,[SexType_NotSelected_Indicator]
				   ,[RaceCode]
				   ,[RaceDescription]
				   ,[StateRaceCode]
				   ,[Race_AmericanIndianAlaskanNative_Indicator]
				   ,[Race_Asian_Indicator]
				   ,[Race_BlackAfricaAmerican_Indicator]
				   ,[Race_NativeHawaiianPacificIslander_Indicator]
				   ,[Race_White_Indicator]
				   ,[Race_MultiRace_Indicator]
				   ,[Race_ChooseNotRespond_Indicator]
				   ,[Race_Other_Indicator]
				   ,[EthnicityCode]
				   ,[EthnicityDescription]
				   ,[EthnicityHispanicLatino_Indicator]
				   ,[Migrant_Indicator]
				   ,[Homeless_Indicator]
				   ,[IEP_Indicator]
				   ,[English_Learner_Code_Value]
				   ,[English_Learner_Description]
				   ,[English_Learner_Indicator]
				   ,[Former_English_Learner_Indicator]
				   ,[Never_English_Learner_Indicator]
				   ,[EconomicDisadvantage_Indicator]
				   ,[EntryDate]
				   ,[EntrySchoolYear]
				   ,[EntryCode]
				   ,[ExitWithdrawDate]
				   ,[ExitWithdrawSchoolYear]
				   ,[ExitWithdrawCode]	   

				   
				   ,StudentMainInfoModifiedDate
	               ,StudentSchoolAssociationModifiedDate

				   ,[ValidFrom]
				   ,[ValidTo]
				   ,[IsCurrent])
        SELECT distinct
			   CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),s.StudentUSI)) AS [_sourceKey],
			   sem.ElectronicMailAddress AS [PrimaryElectronicMailAddress],
			   emt.CodeValue AS [PrimaryElectronicMailTypeDescriptor_CodeValue],
			   emt.Description AS [PrimaryElectronicMailTypeDescriptor_Description],
			   s.StudentUniqueId,       
			   sic.IdentificationCode AS StateId,
       
			   dschool.SchoolKey,
			   edorg.ShortNameOfInstitution,
			   edorg.NameOfInstitution,
			   gld.CodeValue GradeLevelDescriptor_CodeValue,
			   gld.Description AS GradeLevelDescriptor_Description,

			   s.FirstName,
			   LEFT(LTRIM(s.MiddleName),1) AS MiddleInitial,
			   s.MiddleName,	   
			   s.LastSurname,
			   dbo.Func_ETL_GetFullName(s.FirstName,s.MiddleName,s.LastSurname) AS FullName,
			   s.BirthDate,
			   DATEDIFF(YEAR, s.BirthDate, GetDate()) AS StudentAge,
			   ssa.GraduationSchoolYear,

			   COALESCE(shrby.Homeroom,'N/A') AS Homeroom,
			   COALESCE(shrby.HomeroomTeacher,'N/A') AS HomerHomeroomTeacheroom,
			   
			   CASE 
					WHEN sex.CodeValue  = 'Male' THEN 'M'
					WHEN sex.CodeValue  = 'Female' THEN 'F'
					ELSE 'NS' -- not selected
			   END AS SexType_Code,
			   sex.Description AS SexType_Description,
			   CASE WHEN sex.CodeValue  = 'Male' THEN 1 ELSE 0 END AS SexType_Male_Indicator,
			   CASE WHEN sex.CodeValue  = 'Female' THEN 1 ELSE 0 END AS SexType_Female_Indicator,
			   CASE WHEN sex.CodeValue  = 'Not Selected' THEN 1 ELSE 0 END AS SexType_NotSelected_Indicator, 
       
			   COALESCE(sr.RaceCodes,'N/A') AS RaceCode,	   
			   COALESCE(sr.RaceDescriptions,'N/A') AS RaceDescription,
			   CASE WHEN sr.RaceCount > 1 AND s.HispanicLatinoEthnicity = 0 THEN 'Multirace' 
					WHEN s.HispanicLatinoEthnicity = 1 THEN 'Latinx'
					ELSE COALESCE(sr.RaceCodes,'N/A')
			   END AS StateRaceCode,
			   sr.Race_AmericanIndianAlaskanNative_Indicator,
			   sr.Race_Asian_Indicator ,

			   sr.Race_BlackAfricaAmerican_Indicator,
			   sr.Race_NativeHawaiianPacificIslander_Indicator,
			   sr.Race_White_Indicator,
			   CASE WHEN sr.RaceCount > 1 AND s.HispanicLatinoEthnicity = 0 THEN 1 ELSE 0 END AS Race_MultiRace_Indicator, 
			   sr.Race_ChooseNotRespond_Indicator,
			   sr.Race_Other_Indicator,

			   CASE WHEN s.HispanicLatinoEthnicity = 1 THEN 'L' ELSE 'Non-L' END  AS EthnicityCode,
			   CASE WHEN s.HispanicLatinoEthnicity = 1 THEN 'Latinx' ELSE 'Non Latinx' END  AS EthnicityDescription,
			   s.HispanicLatinoEthnicity AS EthnicityHispanicLatino_Indicator,

			   CASE WHEN EXISTS (
							   SELECT 1
							   FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentProgramAssociation spa
							   WHERE CHARINDEX('Migrant', spa.ProgramName,1) > 1
									 AND spa.StudentUSI = s.StudentUSI
									 AND spa.EndDate IS NULL
						   ) THEN 1 ELSE 0 End AS Migrant_Indicator,
			   CASE WHEN EXISTS (
							   SELECT 1
							   FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentProgramAssociation spa
							   WHERE CHARINDEX('Homeless', spa.ProgramName,1) > 1
									 AND spa.StudentUSI = s.StudentUSI
									 AND spa.EndDate IS NULL
						   ) THEN 1 ELSE 0 End AS Homeless_Indicator,
				CASE WHEN EXISTS (
							   SELECT 1
							   FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentSpecialEducationProgramAssociation spa
							   WHERE CHARINDEX('IEP', spa.ProgramName,1) > 1
									 AND spa.StudentUSI = s.StudentUSI
									 AND spa.IEPEndDate IS NULL
						   ) THEN 1 ELSE 0 End AS IEP_Indicator,
	   
			   COALESCE(lepd.CodeValue,'N/A') AS LimitedEnglishProficiencyDescriptor_CodeValue,
			   COALESCE(lepd.CodeValue,'N/A') AS LimitedEnglishProficiencyDescriptor_Description,
			   CASE WHEN COALESCE(lepd.CodeValue,'N/A') = 'Limited' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_EnglishLearner_Indicator,
			   CASE WHEN COALESCE(lepd.CodeValue,'N/A') = 'Formerly Limited' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_Former_Indicator,
			   CASE WHEN COALESCE(lepd.CodeValue,'N/A') = 'NotLimited' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_NotEnglisLearner_Indicator,

			   COALESCE(s.EconomicDisadvantaged,0) AS EconomicDisadvantage_Indicator,
	   
			   --entry
			   ssa.EntryDate,
			   dbo.Func_ETL_GetSchoolYear((ssa.EntryDate)) AS EntrySchoolYear, 
			   COALESCE(eglrt.CodeValue,'N/A') AS EntryCode,
       
			   --exit
			   ssa.ExitWithdrawDate,
			   dbo.Func_ETL_GetSchoolYear((ssa.ExitWithdrawDate)) AS ExitWithdrawSchoolYear, 
			   ewt.CodeValue ExitWithdrawCode,              

			   CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(s.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS SchoolCategoryModifiedDate,
			   CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(ssa.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS SchoolTitle1StatusModifiedDate,

				
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN
				           (SELECT MAX(t) FROM
                             (VALUES
                               (s.LastModifiedDate)
                             , (ssa.LastModifiedDate)                                                    
                             ) AS [MaxLastModifiedDate](t)
                           )
					ELSE 
					      ssa.EntryDate
				END AS ValidFrom,
			   CASE when ssa.ExitWithdrawDate is null then '12/31/9999'  else ssa.ExitWithdrawDate END  AS ValidTo,
			   case when ssa.ExitWithdrawDate is null then 1 else 0 end AS IsCurrent
		--select *  
		FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Student s
			INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa ON s.StudentUSI = ssa.StudentUSI
			INNER JOIN dbo.DimSchool dschool ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ssa.SchoolId)   = dschool._sourceKey
			INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor gld  ON ssa.EntryGradeLevelDescriptorId = gld.DescriptorId			
			LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.EntryGradeLevelReasonType eglrt ON ssa.EntryGradeLevelReasonTypeId = eglrt.EntryGradeLevelReasonTypeId
			LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.ExitWithdrawTypeDescriptor ewtd ON ssa.ExitWithdrawTypeDescriptorId = ewtd.ExitWithdrawTypeDescriptorId
			LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor ewtdd ON ewtd.ExitWithdrawTypeDescriptorId = ewtdd.DescriptorId
			LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.ExitWithdrawType ewt ON ewtd.ExitWithdrawTypeId = ewt.ExitWithdrawTypeId
			LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentElectronicMail sem ON s.StudentUSI = sem.StudentUSI
																		   AND sem.PrimaryEmailAddressIndicator = 1
			LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.ElectronicMailType emt ON sem.ElectronicMailTypeId = emt.ElectronicMailTypeId
			INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.EducationOrganization edorg ON ssa.SchoolId = edorg.EducationOrganizationId

			--lunch
			LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor food ON s.SchoolFoodServicesEligibilityDescriptorId = food.DescriptorId
			--sex
			INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.SexType sex ON s.SexTypeId = sex.SexTypeId
			--state id
			LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentIdentificationCode sic ON s.StudentUSI = sic.StudentUSI
																							   AND sic.AssigningOrganizationIdentificationCode = 'State' 
			--lep
			LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor lepd ON s.LimitedEnglishProficiencyDescriptorId = lepd.DescriptorId
	
			--races
			LEFT JOIN #StudentRaces sr ON s.StudentUSI = sr.StudentUsi
			
			--homeroom
			LEFT JOIN #StudentHomeRooomByYear shrby ON  s.StudentUSI = shrby.StudentUSI
												   AND ssa.SchoolId = shrby.SchoolId
												   AND ssa.SchoolYear = shrby.SchoolYear
												   AND shrby.RowRankId = 1
	
		WHERE ssa.SchoolYear >= 2019 AND
		     (
			   (s.LastModifiedDate > @LastLoadDate AND s.LastModifiedDate <= @NewLoadDate) --OR
			   --(ssa.LastModifiedDate > @LastLoadDate AND ssa.LastModifiedDate <= @NewLoadDate)			 
			 )
			 
		DROP TABLE #StudentRaces, #StudentHomeRooomByYear;
				
			
		--loading legacy data if it has not been loaded.
		--load types are ignored as this data will only be loaded once.
		IF NOT EXISTS(SELECT 1 
		              FROM dbo.DimStudent 
					  WHERE CHARINDEX('LegacyDW',_sourceKey,1) > 0)
			BEGIN
			    ;WITH HomelessStudentsByYear AS (
				--Sch year 2015:
				  SELECT studentno, 2016 AS schyear
				  FROM [BPSGranary02].[RMUStudentBackup].[dbo].[Homeless2015Final] 
				  WHERE McKinneyVento = 'Y'
				  UNION ALL 
				--Sch year 2016:
				  SELECT studentno, 2017 AS schyear
				  FROM [BPSGranary02].[RMUStudentBackup].[dbo].[Homeless2016Final] 
				  WHERE McKinneyVento = 'Y'  
				  UNION ALL
				--Sch year 2017:
				  SELECT studentno, 2018 AS schyear
				  FROM [BPSGranary02].[RMUStudentBackup].[dbo].[Homeless2017Final] 
				  WHERE McKinneyVento = 'Y'  
				)
			   INSERT INTO Staging.[Student]
				   ([_sourceKey]
				   ,[PrimaryElectronicMailAddress]
				   ,[PrimaryElectronicMailTypeDescriptor_CodeValue]
				   ,[PrimaryElectronicMailTypeDescriptor_Description]
				   ,[StudentUniqueId]
				   ,[StateId]
				   ,[SchoolKey]
				   ,[ShortNameOfInstitution]
				   ,[NameOfInstitution]
				   ,[GradeLevelDescriptor_CodeValue]
				   ,[GradeLevelDescriptor_Description]
				   ,[FirstName]
				   ,[MiddleInitial]
				   ,[MiddleName]
				   ,[LastSurname]
				   ,[FullName]
				   ,[BirthDate]
				   ,[StudentAge]
				   ,[GraduationSchoolYear]
				   ,[Homeroom]
				   ,[HomeroomTeacher]
				   ,[SexType_Code]
				   ,[SexType_Description]
				   ,[SexType_Male_Indicator]
				   ,[SexType_Female_Indicator]
				   ,[SexType_NotSelected_Indicator]
				   ,[RaceCode]
				   ,[RaceDescription]
				   ,[StateRaceCode]
				   ,[Race_AmericanIndianAlaskanNative_Indicator]
				   ,[Race_Asian_Indicator]
				   ,[Race_BlackAfricaAmerican_Indicator]
				   ,[Race_NativeHawaiianPacificIslander_Indicator]
				   ,[Race_White_Indicator]
				   ,[Race_MultiRace_Indicator]
				   ,[Race_ChooseNotRespond_Indicator]
				   ,[Race_Other_Indicator]
				   ,[EthnicityCode]
				   ,[EthnicityDescription]
				   ,[EthnicityHispanicLatino_Indicator]
				   ,[Migrant_Indicator]
				   ,[Homeless_Indicator]
				   ,[IEP_Indicator]
				   ,[English_Learner_Code_Value]
				   ,[English_Learner_Description]
				   ,[English_Learner_Indicator]
				   ,[Former_English_Learner_Indicator]
				   ,[Never_English_Learner_Indicator]
				   ,[EconomicDisadvantage_Indicator]
				   ,[EntryDate]
				   ,[EntrySchoolYear]
				   ,[EntryCode]
				   ,[ExitWithdrawDate]
				   ,[ExitWithdrawSchoolYear]
				   ,[ExitWithdrawCode]	   

				   
				   ,StudentMainInfoModifiedDate
	               ,StudentSchoolAssociationModifiedDate

				   ,[ValidFrom]
				   ,[ValidTo]
				   ,[IsCurrent])
			   SELECT DISTINCT
						CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),s.StudentNo)) AS [_sourceKey],
						null AS [PrimaryElectronicMailAddress],
						null AS [PrimaryElectronicMailTypeDescriptor_CodeValue],
						null AS [PrimaryElectronicMailTypeDescriptor_Description],

						s.StudentNo AS [StudentUniqueId],       
						s.sasid AS StateId,
       
						dschool.SchoolKey,
						dschool.ShortNameOfInstitution,
						dschool.NameOfInstitution,
						s.Grade as GradeLevelDescriptor_CodeValue,
						s.Grade as GradeLevelDescriptor_Description,

						s.FirstName,
						LEFT(LTRIM(s.MiddleName),1) AS MiddleInitial,
						s.MiddleName,	   
						s.LastName AS LastSurname,
						dbo.Func_ETL_GetFullName(s.FirstName,s.MiddleName,s.LastName) AS FullName,
						s.DOB AS BirthDate,
						DATEDIFF(YEAR, s.DOB, GetDate()) AS StudentAge,
						s.YOG AS GraduationSchoolYear,

						s.Homeroom,
						NULL AS HomeroomTeacher,

						CASE 
							WHEN s.Sex = 'M' THEN 'M'
							WHEN s.Sex = 'F' THEN 'F'
							ELSE 'NS' -- not selected
						END AS SexType_Code,
						CASE 
							WHEN s.Sex = 'M' THEN 'Male'
							WHEN s.Sex = 'F' THEN 'Female'
							ELSE 'Not Selected' -- not selected
						END AS SexType_Description,
						CASE WHEN s.Sex = 'M' THEN 1 ELSE 0 END AS SexType_Male_Indicator,
						CASE WHEN s.Sex = 'F' THEN 1 ELSE 0 END AS SexType_Female_Indicator,
						CASE WHEN s.Sex not in ( 'M','F') THEN 1 ELSE 0 END AS SexType_NotSelected_Indicator, -- NON BINARY

						CASE WHEN sdir.IsNatAmer = 1 THEN 'American Indian - Alaskan Native'
							WHEN sdir.IsAsian = 1 THEN 'Asian'
							WHEN sdir.IsBlack = 1 THEN 'Black - African American'
							WHEN sdir.IsPacIsland = 1 THEN 'Native Hawaiian - Pacific Islander'
							WHEN sdir.IsWhite = 1 THEN 'White'
							WHEN sdir.IsHispanic = 1 THEN 'Hispanic'
							ELSE 'Choose Not Respond'
						END AS RaceCode,
						CASE WHEN sdir.IsNatAmer = 1 THEN 'American Indian - Alaskan Native'
							WHEN sdir.IsAsian = 1 THEN 'Asian'
							WHEN sdir.IsBlack = 1 THEN 'Black - African American'
							WHEN sdir.IsPacIsland = 1 THEN 'Native Hawaiian - Pacific Islander'
							WHEN sdir.IsWhite = 1 THEN 'White'
							WHEN sdir.IsHispanic = 1 THEN 'Hispanic'
							ELSE 'Choose Not Respond'
						END AS RaceDescription,
						CASE WHEN sdir.IsNatAmer = 1 THEN 'American Indian - Alaskan Native'
							WHEN sdir.IsAsian = 1 THEN 'Asian'
							WHEN sdir.IsBlack = 1 THEN 'Black - African American'
							WHEN sdir.IsPacIsland = 1 THEN 'Native Hawaiian - Pacific Islander'
							WHEN sdir.IsWhite = 1 THEN 'White'
							WHEN sdir.IsHispanic = 1 THEN 'Hispanic'
							ELSE 'Choose Not Respond'
						END AS StateRaceCode,
						sdir.IsNatAmer AS Race_AmericanIndianAlaskanNative_Indicator,
						sdir.IsAsian AS Race_Asian_Indicator,
						sdir.IsBlack AS Race_BlackAfricaAmerican_Indicator,
						sdir.IsPacIsland AS Race_NativeHawaiianPacificIslander_Indicator,
						sdir.IsWhite AS Race_White_Indicator,
						0 AS Race_MultiRace_Indicator, 
						CASE WHEN sdir.IsNatAmer = 0 AND
									sdir.IsAsian = 0 AND 
									sdir.IsBlack = 0 AND 
									sdir.IsPacIsland = 0 AND
									sdir.IsWhite = 0 AND 
									sdir.IsHispanic = 0 THEN 1 
									ELSE 0
						END AS Race_ChooseNotRespond_Indicator,
						0 AS Race_Other_Indicator,

						CASE WHEN sdir.IsHispanic = 1 THEN 'H' ELSE 'Non-H' END  AS EthnicityCode,
						CASE WHEN sdir.IsHispanic = 1 THEN 'Hispanic' ELSE 'Non Hispanic' END  AS EthnicityDescription,
						sdir.IsHispanic AS EthnicityHispanicLatino_Indicator,
	   
						0 AS Migrant_Indicator,
						CASE WHEN hsby.studentno IS NULL THEN 0 ELSE 1 END AS Homeless_Indicator,	   
						case WHEN COALESCE(s.SnCode,'None') <> 'None' THEN 1  ELSE 0 END  AS IEP_Indicator,
	   
						COALESCE(s.Lep_Status,'N/A') AS LimitedEnglishProficiencyDescriptor_CodeValue,
						COALESCE(s.Lep_Status,'N/A') AS LimitedEnglishProficiencyDescriptor_Description,
						CASE WHEN COALESCE(s.Lep_Status,'N/A') = 'L' THEN 1 ELSE 0 END [LimitedEnglishProficiency_EnglishLearner_Indicator],
						CASE WHEN COALESCE(s.Lep_Status,'N/A') = 'F' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_Former_Indicator,
						CASE WHEN COALESCE(s.Lep_Status,'N/A') = 'N' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_NotEnglisLearner_Indicator,
						CASE WHEN COALESCE(s.foodgroup,'None') <> 'None' THEN 1 ELSE 0 END AS EconomicDisadvantage_Indicator,
       
						--entry	   
						CASE WHEN MONTH(s.entdate) >= 7 THEN 
								DATEADD(YEAR,s.schyear  - YEAR(s.entdate),s.entdate)
							ELSE 
								DATEADD(YEAR,s.schyear + 1  - YEAR(s.entdate),s.entdate)
						END AS EntryDate,

						s.schyear + 1 AS EntrySchoolYear, 
						COALESCE(s.entcode,'N/A') AS EntryCode,
       
						--exit
						CASE WHEN s.schyearsequenceno =  999999 AND s.withdate IS null   THEN '6/30/' + CAST(s.schyear AS NVARCHAR(max)) 
							ELSE s.withdate
						END AS ExitWithdrawDate,
						s.schyear + 1 AS ExitWithdrawSchoolYear, 
						COALESCE(s.withcode,'N/A') AS ExitWithdrawCode,
				
						'07/01/2015' AS SchoolCategoryModifiedDate,
						'07/01/2015' AS SchoolTitle1StatusModifiedDate

						,s.entdate AS ValidFrom
						,COALESCE(s.withdate,s.entdate) AS ValidTo
						,0 IsCurrent
				--select distinct top 1000 *
				FROM [BPSGranary02].[BPSDW].[dbo].[student] s 
					--WHERE schyear IN (2017,2016,2015) AND s.StudentNo = '210191' ORDER BY s.StudentNo, s.entdate
						INNER JOIN [BPSGranary02].[RAEDatabase].[dbo].[studentdir] sdir ON s.StudentNo = sdir.studentno
						INNER JOIN dbo.DimSchool dschool ON  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),s.sch))  = dschool._sourceKey	 
						LEFT JOIN HomelessStudentsByYear hsby ON s.StudentNo = hsby.studentno 
															and s.schyear = hsby.schyear
				WHERE s.schyear IN (2017,2016,2015)
						and s.sch between '1000' and '4700'
				ORDER BY s.StudentNo;

			END

		--COMMIT TRANSACTION;		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		---- Test whether the transaction is uncommittable.
		--IF XACT_STATE( ) = -1
		--	BEGIN
		--		--The transaction is in an uncommittable state. Rolling back transaction
		--		ROLLBACK TRANSACTION;
		--	END;

		---- Test whether the transaction is committable.
		--IF XACT_STATE( ) = 1
		--	BEGIN
		--		--The transaction is committable. Committing transaction
		--		COMMIT TRANSACTION;
		--	END;
	END CATCH;
END;
GO


--Dim AttendanceEventCategory
--------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimAttendanceEventCategory_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY

		TRUNCATE TABLE Staging.AttendanceEventCategory
		INSERT INTO Staging.AttendanceEventCategory
				   ([_sourceKey]
					,[AttendanceEventCategoryDescriptor_CodeValue]
					,[AttendanceEventCategoryDescriptor_Description]
					,[InAttendance_Indicator]
					,[UnexcusedAbsence_Indicator]
					,[ExcusedAbsence_Indicator]
					,[Tardy_Indicator]
					,[EarlyDeparture_Indicator]
					,[CategoryModifiedDate]
					,[ValidFrom]
					,[ValidTo]
					,[IsCurrent])
        --declare @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate()
		SELECT  DISTINCT 
			    CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),d.DescriptorId)) AS [_sourceKey],
				COALESCE(d.CodeValue,'In Attendance') as AttendanceEventCategoryDescriptor_CodeValue,
				COALESCE(d.CodeValue,'In Attendance') as AttendanceEventCategoryDescriptor_Description,
				case when COALESCE(d.CodeValue,'In Attendance') in ('In Attendance','Tardy','Early departure') then 1 else 0 end as [InAttendance_Indicator], -- not used
				case when COALESCE(d.CodeValue,'In Attendance') in ('Unexcused Absence') then 1 else 0 end as [UnexcusedAbsence_Indicator],
				case when COALESCE(d.CodeValue,'In Attendance') in ('Excused Absence') then 1 else 0 end as [ExcusedAbsence_Indicator],
				case when COALESCE(d.CodeValue,'In Attendance') in ('Tardy') then 1 else 0 end as [Tardy_Indicator],	   
				case when COALESCE(d.CodeValue,'In Attendance') in ('Early departure') then 1 else 0 end as [EarlyDeparture_Indicator],	  
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(d.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS [CategoryModifiedDate],
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN
				           (SELECT MAX(t) FROM
                             (VALUES
                               (d.LastModifiedDate)                             
                             ) AS [MaxLastModifiedDate](t)
                           )
					ELSE 
					      '07/01/2015' -- setting the validFrom to beggining of time during thre first load. 
				END AS ValidFrom,
				'12/31/9999' AS ValidTo,
				1  AS IsCurrent				
		--select *  
		FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor d
		WHERE d.Namespace IN ('http://ed-fi.org/Descriptor/AttendanceEventCategoryDescriptor.xml',
		                      'http://ed-fi.org/Descriptor/Follett/Aspen/AttendanceEventCategoryDescriptor.xml')	
			  AND (d.LastModifiedDate > @LastLoadDate AND d.LastModifiedDate <= @NewLoadDate);
		
			
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		
	END CATCH;
END;
GO

--Dim DisciplineIncident
--------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimDisciplineIncident_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY

		
		TRUNCATE TABLE Staging.DisciplineIncident
		INSERT INTO Staging.DisciplineIncident
				   (_sourceKey
				    ,[SchoolKey]
				    ,[ShortNameOfInstitution]
				    ,[NameOfInstitution]
				    ,[SchoolYear]
				    ,[IncidentDate]
				    ,[IncidentTime]
				    ,[IncidentDescription]
				    ,[BehaviorDescriptor_CodeValue]
				    ,[BehaviorDescriptor_Description]
				    ,[LocationDescriptor_CodeValue]
				    ,[LocationDescriptor_Description]
				    ,[DisciplineDescriptor_CodeValue]
				    ,[DisciplineDescriptor_Description]
				    ,DisciplineDescriptor_ISS_Indicator
				    ,DisciplineDescriptor_OSS_Indicator
				    ,[ReporterDescriptor_CodeValue]
				    ,[ReporterDescriptor_Description]
			 	    
				    ,[IncidentReporterName]
				    ,[ReportedToLawEnforcement_Indicator]
				    ,[IncidentCost]

					,IncidentModifiedDate

				    ,[ValidFrom]
				    ,[ValidTo]
				    ,[IsCurrent])
        --declare @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate()
		SELECT DISTINCT 
				CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),di.IncidentIdentifier)) AS [_sourceKey],
				dschool.SchoolKey,
				dschool.ShortNameOfInstitution,
				dschool.NameOfInstitution,
				dbo.Func_ETL_GetSchoolYear(di.IncidentDate),
				di.IncidentDate,
				COALESCE(di.IncidentTime,'00:00:00.0000000') AS IncidentTime,
				di.IncidentDescription,
				COALESCE(d_dib.CodeValue,'N/A') as [BehaviorDescriptor_CodeValue],
				COALESCE(d_dib.Description,'N/A') as [BehaviorDescriptor_Description],
	  
				COALESCE(d_dil.CodeValue,'N/A') as [LocationDescriptor_CodeValue],
				COALESCE(d_dil.Description,'N/A') as [LocationDescriptor_Description],

				COALESCE(d_dia.CodeValue,'N/A') as [DisciplineDescriptor_CodeValue],
				COALESCE(d_dia.Description,'N/A') as [DisciplineDescriptor_Description],
				CASE WHEN  COALESCE(d_dia.CodeValue,'N/A') IN ('In School Suspension','In-School Suspension') THEN 1 ELSE 0 END as DisciplineDescriptor_ISS_Indicator,
				CASE WHEN  COALESCE(d_dia.CodeValue,'N/A') IN ('Out of School Suspension','Out-Of-School Suspension') THEN 1 ELSE 0 END as DisciplineDescriptor_OSS_Indicator,
	  
				COALESCE(d_dirt.CodeValue,'N/A') as ReporterDescriptor_CodeValue,
				COALESCE(d_dirt.Description,'N/A') as ReporterDescriptor_Description,

				COALESCE(di.ReporterName,'N/A'),
				COALESCE(di.ReportedToLawEnforcement,0) AS ReportedToLawEnforcement,
				COALESCE(di.IncidentCost,0) AS IncidentCost,
				
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(di.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS IncidentModifiedDate,

				--Making sure the first time, the ValidFrom is set to beginning of time 
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN
				           (SELECT MAX(t) FROM
                             (VALUES
                               (di.LastModifiedDate)                             
                             ) AS [MaxLastModifiedDate](t)
                           )
					ELSE 
					      '07/01/2015' -- setting the validFrom to beggining of time during thre first load. 
				END AS ValidFrom,
				'12/31/9999' AS ValidTo,
				1 AS IsCurrent		
		FROM  [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.DisciplineIncident di       
				LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.DisciplineIncidentBehavior dib ON di.IncidentIdentifier = dib.IncidentIdentifier
				LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.DisciplineActionDisciplineIncident dadi ON di.IncidentIdentifier = dadi.IncidentIdentifier
				LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.DisciplineActionDiscipline dad ON dadi.DisciplineActionIdentifier = dad.DisciplineActionIdentifier

				INNER JOIN dbo.DimSchool dschool ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),di.SchoolId)   = dschool._sourceKey
				INNER JOIN dbo.DimTime dt ON di.IncidentDate = dt.SchoolDate
												AND dt.SchoolKey is not null   
												AND dschool.SchoolKey = dt.SchoolKey
				LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor d_dib ON dib.BehaviorDescriptorId   = d_dib.DescriptorId
				LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.IncidentLocationType d_dil ON di.IncidentLocationTypeId   = d_dil.IncidentLocationTypeId
				LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor d_dia ON dad.DisciplineDescriptorId   = d_dia.DescriptorId
				LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor d_dirt ON di.ReporterDescriptionDescriptorId   = d_dirt.DescriptorId
		WHERE dbo.Func_ETL_GetSchoolYear(di.IncidentDate) >= 2019 AND
		    (
			  	(di.LastModifiedDate > @LastLoadDate AND di.LastModifiedDate <= @NewLoadDate)
			)
													
			
		--loading legacy data if it has not been loaded.
		--load types are ignored as this data will only be loaded once.
		IF NOT EXISTS(SELECT 1 
		              FROM dbo.DimDisciplineIncident 
					  WHERE CHARINDEX('LegacyDW',_sourceKey,1) > 0)
			BEGIN
			   INSERT INTO Staging.DisciplineIncident
				   (_sourceKey
				    ,[SchoolKey]
				    ,[ShortNameOfInstitution]
				    ,[NameOfInstitution]
				    ,[SchoolYear]
				    ,[IncidentDate]
				    ,[IncidentTime]
				    ,[IncidentDescription]
				    ,[BehaviorDescriptor_CodeValue]
				    ,[BehaviorDescriptor_Description]
				    ,[LocationDescriptor_CodeValue]
				    ,[LocationDescriptor_Description]
				    ,[DisciplineDescriptor_CodeValue]
				    ,[DisciplineDescriptor_Description]
				    ,DisciplineDescriptor_ISS_Indicator
				    ,DisciplineDescriptor_OSS_Indicator
				    ,[ReporterDescriptor_CodeValue]
				    ,[ReporterDescriptor_Description]
			 	    
				    ,[IncidentReporterName]
				    ,[ReportedToLawEnforcement_Indicator]
				    ,[IncidentCost]

					,IncidentModifiedDate

				    ,[ValidFrom]
				    ,[ValidTo]
				    ,[IsCurrent])
			 SELECT DISTINCT 
					  CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),di.[CND_INCIDENT_ID])) AS [_sourceKey],    
					  dschool.SchoolKey,
					  dschool.ShortNameOfInstitution,
					  dschool.NameOfInstitution,
					  dbo.Func_ETL_GetSchoolYear(di.[CND_INCIDENT_DATE]) AS [SchoolYear],
					  di.CND_INCIDENT_DATE AS [IncidentDate],
					 -- TRY_CAST(di.CND_INCIDENT_TIME AS DATETIME2) ,
					  CONVERT(char(12),TRY_CAST(di.CND_INCIDENT_TIME AS DATETIME2), 108) IncidentTime,
					  --'00:00:00.0000000' AS ,
					  di.[CND_INCIDENT_DESCRIPTION] AS [IncidentDescription],
					  COALESCE(di.CND_INCIDENT_CODE,'N/A') as [BehaviorDescriptor_CodeValue],
					  COALESCE(di.CND_INCIDENT_CODE,'N/A') as [BehaviorDescriptor_Description],
	  
					  COALESCE(di.[CND_INCIDENT_LOCATION],'N/A') as [LocationDescriptor_CodeValue],
					  COALESCE(di.[CND_INCIDENT_LOCATION],'N/A') as [LocationDescriptor_Description],

					  COALESCE(di.[ACT_ACTION_CODE],'N/A') as [DisciplineDescriptor_CodeValue],
					  COALESCE(di.[ACT_ACTION_CODE],'N/A') as [DisciplineDescriptor_Description],
					  CASE WHEN  COALESCE(di.ACT_ACTION_CODE,'N/A') IN ('In-School Suspension)') THEN 1 ELSE 0 END,
					  CASE WHEN  COALESCE(di.ACT_ACTION_CODE,'N/A') IN ('Out of School Suspension') THEN 1 ELSE 0 END,
	  
					  'N/A' as ReporterDescriptor_CodeValue,
					  'N/A' as ReporterDescriptor_Description,

					  'N/A' AS [IncidentReporterName],
					  0 AS ReportedToLawEnforcement,
					  0 AS IncidentCost,

					  '07/01/2015' AS IncidentModifiedDate,
					  
					  '07/01/2015' AS ValidFrom,
					  GETDATE() AS ValidTo,
					  0 AS IsCurrent		
					  
				--select distinct *
				FROM  [Raw_LegacyDW].[DisciplineIncidents] di
					  INNER JOIN dbo.DimSchool dschool ON CONCAT_WS('|', 'Ed-Fi', Convert(NVARCHAR(MAX),di.[SKL_SCHOOL_ID]))   = dschool._sourceKey 
					  INNER JOIN dbo.DimTime dt ON di.CND_INCIDENT_DATE = dt.SchoolDate
														 AND dt.SchoolKey is not null   
														 AND dschool.SchoolKey = dt.SchoolKey	
				WHERE TRY_CAST(di.CND_INCIDENT_DATE AS DATETIME)  > '2015-09-01'
			END

			
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		
	END CATCH;
END;
GO

--Dim Assessment
--------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimAssessment_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY

			
		DECLARE @Assessment TABLE
		(   
			AssessmentCategoryDescriptor_CodeValue NVARCHAR(50) NOT NULL,    
			AssessmentCategoryDescriptor_Description NVARCHAR(1024) NOT NULL,    
			AssessmentFamilyTitle NVARCHAR(100) NULL,    	
			AdaptiveAssessment_Indicator bit NOT NULL, 
			AssessmentIdentifier NVARCHAR(60) NOT NULL,   
			ObjectiveAssessmentIdentificationCode NVARCHAR(60) NOT NULL,   
			AssessmentTitle NVARCHAR(500) NOT NULL,

			ReportingMethodDescriptor_CodeValue NVARCHAR(50) NOT NULL,   
			ReportingMethodDescriptor_Description NVARCHAR(1024) NOT NULL,   
	
			ResultDatatypeTypeDescriptor_CodeValue  NVARCHAR(50) NOT NULL,   
			ResultDatatypeTypeDescriptor_Description NVARCHAR(1024) NOT NULL,   


			AssessmentScore_Indicator  BIT NOT NULL,
			AssessmentPerformanceLevel_Indicator  BIT NOT NULL,

			ObjectiveAssessmentScore_Indicator  BIT NOT NULL,
			ObjectiveAssessmentPerformanceLevel_Indicator  BIT NOT NULL,

			AssessmentModifiedDate [datetime] NOT NULL,

			ValidFrom DATETIME NOT NULL, 
			ValidTo DATETIME NOT NULL, 
			IsCurrent BIT NOT NULL,
			IsLegacy BIT NOT NULL

		);



		--Assessmnent
		INSERT INTO @Assessment
		(
			AssessmentCategoryDescriptor_CodeValue,
			AssessmentCategoryDescriptor_Description,
			AssessmentFamilyTitle,
			AdaptiveAssessment_Indicator,
			AssessmentIdentifier,
			ObjectiveAssessmentIdentificationCode,
			AssessmentTitle,

			ReportingMethodDescriptor_CodeValue,   
			ReportingMethodDescriptor_Description,   
	
			ResultDatatypeTypeDescriptor_CodeValue,   
			ResultDatatypeTypeDescriptor_Description,   


			AssessmentScore_Indicator,
			AssessmentPerformanceLevel_Indicator,

			ObjectiveAssessmentScore_Indicator,
			ObjectiveAssessmentPerformanceLevel_Indicator,

			AssessmentModifiedDate,
			ValidFrom, 
			ValidTo, 
			IsCurrent,
			IsLegacy
		)

		SELECT DISTINCT 
			   a_d.CodeValue AS [AssessmentCategoryDescriptor_CodeValue],
			   a_d.[Description] AS [AssessmentCategoryDescriptor_Description],
			   a.AssessmentFamilyTitle AS [AssessmentFamilyTitle],
			   ISNULL(a.AdaptiveAssessment,0) AS [AdaptiveAssessment_Indicator], 
			   a.AssessmentIdentifier,
			   'N/A' AS ObjectiveAssessmentIdentificationCode,
			   a.AssessmentTitle,	   

			   a_s_armt.CodeValue AS ReportingMethodDescriptor_CodeValue,
			   a_s_armt.[Description] AS ReportingMethodDescriptor_Description,
			   a_s_rdtt.CodeValue AS ResultDatatypeTypeDescriptor_CodeValue,
			   a_s_rdtt.[Description] AS ResultDatatypeTypeDescriptor_Description,

			   1 AS AssessmentScore_Indicator,
			   0 AS AssessmentPerformanceLevel_Indicator,
  
			   0 AS ObjectiveAssessmentScore_Indicator,
			   0 AS ObjectiveAssessmentPerformanceLevel_Indicator,
			   
			   CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(a.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS AssessmentModifiedDate,
			   --Making sure the first time, the ValidFrom is set to beginning of time 
			   CASE WHEN @LastLoadDate <> '07/01/2015' THEN
				           (SELECT MAX(t) FROM
                             (VALUES
                               (a.LastModifiedDate)                             
                             ) AS [MaxLastModifiedDate](t)
                           )
					ELSE 
					      '07/01/2015' -- setting the validFrom to beggining of time during thre first load. 
			   END AS ValidFrom,
			   '12/31/9999' AS ValidTo,
			   1 AS IsCurrent,
			   0 AS IsLegacy
		FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Assessment a 
			 INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor a_d ON a.AssessmentCategoryDescriptorId = a_d.DescriptorId
			 LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.AssessmentScore a_s ON a.AssessmentIdentifier = a_s.AssessmentIdentifier 
			 LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.AssessmentReportingMethodType a_s_armt ON a_s.AssessmentReportingMethodTypeId = a_s_armt.AssessmentReportingMethodTypeId
			 LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.ResultDatatypeType a_s_rdtt ON a_s.ResultDatatypeTypeId = a_s_rdtt.ResultDatatypeTypeId
		WHERE CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1 
		      AND (a.LastModifiedDate > @LastLoadDate AND a.LastModifiedDate <= @NewLoadDate)
		Union
		SELECT DISTINCT 
			   a_d.CodeValue AS [AssessmentCategoryDescriptor_CodeValue],
			   a_d.[Description] AS [AssessmentCategoryDescriptor_Description],
			   a.AssessmentFamilyTitle AS [AssessmentFamilyTitle],
			   ISNULL(a.AdaptiveAssessment,0) AS [AdaptiveAssessment_Indicator], 
			   a.AssessmentIdentifier,
				'N/A' AS ObjectiveAssessmentIdentificationCode,
			   a.AssessmentTitle,	   
	   
			   a_pl_armt.CodeValue AS ReportingMethodDescriptor_CodeValue,
			   a_pl_armt.[Description] AS ReportingMethodDescriptor_Description,
			   a_pl_rdtt.CodeValue AS ResultDatatypeTypeDescriptor_CodeValue,
			   a_pl_rdtt.[Description] AS ResultDatatypeTypeDescriptor_Description,

			   0 AS AssessmentScore_Indicator,
			   1 AS AssessmentPerformanceLevel_Indicator,
  
			   0 AS ObjectiveAssessmentScore_Indicator,
			   0 AS ObjectiveAssessmentPerformanceLevel_Indicator,
			   
			   CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(a.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS AssessmentModifiedDate,
			   ---Making sure the first time, the ValidFrom is set to beginning of time 
			   CASE WHEN @LastLoadDate <> '07/01/2015' THEN
				           (SELECT MAX(t) FROM
                             (VALUES
                               (a.LastModifiedDate)                             
                             ) AS [MaxLastModifiedDate](t)
                           )
					ELSE 
					      '07/01/2015' -- setting the validFrom to beggining of time during thre first load. 
			   END AS ValidFrom,
			   '12/31/9999' AS ValidTo,
			   1 AS IsCurrent,
			   0 AS IsLegacy
		FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Assessment a 
			 INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor a_d ON a.AssessmentCategoryDescriptorId = a_d.DescriptorId
	 
	 
			 LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.AssessmentPerformanceLevel a_pl ON a.AssessmentIdentifier = a_pl.AssessmentIdentifier 
			 LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor a_pl_d ON a_pl.PerformanceLevelDescriptorId = a_pl_d.DescriptorId
			 LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.AssessmentReportingMethodType a_pl_armt ON a_pl.AssessmentReportingMethodTypeId = a_pl_armt.AssessmentReportingMethodTypeId
			 LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.ResultDatatypeType a_pl_rdtt ON a_pl.ResultDatatypeTypeId = a_pl_rdtt.ResultDatatypeTypeId
		WHERE CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1 
		      AND (a.LastModifiedDate > @LastLoadDate AND a.LastModifiedDate <= @NewLoadDate)
		--ORDER BY a.AssessmentIdentifier, ObjectiveAssessmentIdentificationCode, ReportingMethodDescriptor_CodeValue

		
			
							
			
		--loading legacy data if it has not been loaded.
		--load types are ignored as this data will only be loaded once.
		IF NOT EXISTS(SELECT 1 
		              FROM dbo.DimAssessment 
					  WHERE CHARINDEX('LegacyDW',_sourceKey,1) > 0)
			BEGIN
			   ;WITH UnpivotedScores AS 
				(
					SELECT testid,schyear,testtime,studentno,adminyear,grade, scoretype, scorevalue, CASE WHEN a.scoretype IN ('Proficiency level','Proficiency level 2') THEN 1 ELSE 0 END AS isperflevel
					--INTO [Raw_LegacyDW].[MCASAssessmentScores]
					FROM (  
							 --ensuring all score columns have the same data type to avoid conflicts with unpivot
							 SELECT testid,schyear,testtime,studentno,adminyear,grade,teststatus ,
								  CAST(rawscore AS NVARCHAR(MAX)) AS [Raw score],
								  CAST(scaledscore AS NVARCHAR(MAX)) AS [Scale score],
								  CAST(perflevel AS NVARCHAR(MAX)) AS  [Proficiency level],
								  CAST(sgp AS NVARCHAR(MAX)) AS [Percentile rank],
								  CAST(cpi AS NVARCHAR(MAX)) AS [Composite Performance Index],
								  CAST(perf2 AS NVARCHAR(MAX)) AS [Proficiency level 2]
     
							FROM [BPSGranary02].[RAEDatabase].[dbo].[mcasitems] 
							WHERE schyear >= 2015 ) scores
					UNPIVOT
					(
					   scorevalue
					   FOR scoretype IN ([Raw score],[Scale score],[Proficiency level],[Percentile rank],[Composite Performance Index],[Proficiency level 2])
					) AS a
				)

			   INSERT INTO @Assessment
				(
					AssessmentCategoryDescriptor_CodeValue,
					AssessmentCategoryDescriptor_Description,
					AssessmentFamilyTitle,
					AdaptiveAssessment_Indicator,
					AssessmentIdentifier,
					ObjectiveAssessmentIdentificationCode,
					AssessmentTitle,

					ReportingMethodDescriptor_CodeValue,   
					ReportingMethodDescriptor_Description,   
	
					ResultDatatypeTypeDescriptor_CodeValue,   
					ResultDatatypeTypeDescriptor_Description,   


					AssessmentScore_Indicator,
					AssessmentPerformanceLevel_Indicator,

					ObjectiveAssessmentScore_Indicator,
					ObjectiveAssessmentPerformanceLevel_Indicator,

					AssessmentModifiedDate,
					ValidFrom, 
					ValidTo, 
					IsCurrent,
			        IsLegacy
				)

				SELECT DISTINCT 
					   'State assessment' AS [AssessmentCategoryDescriptor_CodeValue],
					   'State assessment' AS [AssessmentCategoryDescriptor_Description],
					   NULL AS [AssessmentFamilyTitle],
					   0 AS [AdaptiveAssessment_Indicator], 
					   testid AS AssessmentIdentifier,
					   'N/A' AS ObjectiveAssessmentIdentificationCode,
					   testid AS AssessmentTitle,	   

					   scoretype AS ReportingMethodDescriptor_CodeValue,
					   scoretype AS ReportingMethodDescriptor_Description,
					   CASE WHEN isperflevel = 1 THEN 'Level' ELSE 'Integer' end   AS ResultDatatypeTypeDescriptor_CodeValue,
					   CASE WHEN isperflevel = 1 THEN 'Level' ELSE 'Integer' end AS ResultDatatypeTypeDescriptor_Description,
	   
					   CASE WHEN isperflevel = 1 THEN 0 ELSE 1 end AS AssessmentScore_Indicator,
					   isperflevel AS AssessmentPerformanceLevel_Indicator,
  
					   0 AS ObjectiveAssessmentScore_Indicator,
					   0 AS ObjectiveAssessmentPerformanceLevel_Indicator,

					   '07/01/2015' AS AssessmentModifiedDate,
					   '07/01/2015' AS ValidFrom,
					   GETDATE() AS ValidTo,
					   0 AS IsCurrent,
					   1 AS IsLegacy
				FROM UnpivotedScores
			END

		TRUNCATE TABLE Staging.Assessment
		INSERT INTO Staging.Assessment
				   ([_sourceKey]
				   ,[AssessmentCategoryDescriptor_CodeValue]
				   ,[AssessmentCategoryDescriptor_Description]
				   ,[AssessmentFamilyTitle]
				   ,[AdaptiveAssessment_Indicator]
				   ,[AssessmentIdentifier]
				   ,[AssessmentTitle]

				   ,[ReportingMethodDescriptor_CodeValue]
				   ,[ReportingMethodDescriptor_Description]

				   ,[ResultDatatypeTypeDescriptor_CodeValue]
				   ,[ResultDatatypeTypeDescriptor_Description]

				   ,[AssessmentScore_Indicator]
				   ,[AssessmentPerformanceLevel_Indicator]

				   ,[ObjectiveAssessmentScore_Indicator]
				   ,[ObjectiveAssessmentPerformanceLevel_Indicator]
				   
				   ,AssessmentModifiedDate

				   ,[ValidFrom]
				   ,[ValidTo]
				   ,[IsCurrent])
        --declare @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate()
		SELECT DISTINCT 
				CONCAT_WS('|',CASE WHEN IsLegacy = 0 THEN 'Ed-Fi' ELSE 'LegacyDW' END , Convert(NVARCHAR(MAX),AssessmentIdentifier)  + '|' + Convert(NVARCHAR(MAX),ObjectiveAssessmentIdentificationCode) + '|' + Convert(NVARCHAR(MAX),ReportingMethodDescriptor_CodeValue)) AS [_sourceKey]
				
				,[AssessmentCategoryDescriptor_CodeValue]
			    ,[AssessmentCategoryDescriptor_Description]
			    ,[AssessmentFamilyTitle]
			    ,[AdaptiveAssessment_Indicator]
			    ,[AssessmentIdentifier]
			    ,[AssessmentTitle]
			    
			    ,[ReportingMethodDescriptor_CodeValue]
			    ,[ReportingMethodDescriptor_Description]
			    
			    ,[ResultDatatypeTypeDescriptor_CodeValue]
			    ,[ResultDatatypeTypeDescriptor_Description]
			    
			    ,[AssessmentScore_Indicator]
			    ,[AssessmentPerformanceLevel_Indicator]
			    
			    ,[ObjectiveAssessmentScore_Indicator]
			    ,[ObjectiveAssessmentPerformanceLevel_Indicator]
	  		    ,AssessmentModifiedDate
			    ,ValidFrom
			    ,ValidTo
			    ,IsCurrent
		FROM @Assessment;			    

				
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		-- Test whether the transaction is uncommittable.
		
	END CATCH;
END;
GO

--Dim Course
--------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_DimCourse_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY

		
		TRUNCATE TABLE Staging.Course
		INSERT INTO Staging.Course
		(
		    _sourceKey,
		    CourseCode,
		    CourseTitle,
		    CourseDescription,
		    CourseLevelCharacteristicTypeDescriptor_CodeValue,
		    CourseLevelCharacteristicTypeDescriptor_Description,
		    AcademicSubjectDescriptor_CodeValue,
		    AcademicSubjectDescriptor_Description,
		    HighSchoolCourseRequirement_Indicator,
		    MinimumAvailableCredits,
		    MaximumAvailableCredits,
		    GPAApplicabilityType_CodeValue,
		    GPAApplicabilityType_Description,
		    SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue,
		    SecondaryCourseLevelCharacteristicTypeDescriptor_Description,
		    CourseModifiedDate,
		    ValidFrom,
		    ValidTo,
		    IsCurrent
		)
		
        --declare @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate()
		SELECT DISTINCT 
			   CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),c.CourseCode)) AS [_sourceKey],
			   c.CourseCode,
			   c.CourseTitle,
			   c.CourseDescription,
			   COALESCE(clct.CodeValue,'N/A') AS [CourseLevelCharacteristicTypeDescriptor_CodeValue],
			   COALESCE(clct.[Description],'N/A') AS [CourseLevelCharacteristicTypeDescriptor_Descriptor],

			   COALESCE(ast.CodeValue,'N/A') AS [AcademicSubjectDescriptor_CodeValue],
			   COALESCE(ast.[Description],'N/A') AS [AcademicSubjectDescriptor_Descriptor],
			   COALESCE(c.HighSchoolCourseRequirement,0) AS [HighSchoolCourseRequirement_Indicator],

			   c.MinimumAvailableCredits,
			   c.MaximumAvailableCredits,
			   COALESCE(cgat.CodeValue,'N/A')  AS GPAApplicabilityType_CodeValue,
			   COALESCE(cgat.[Description],'N/A') AS GPAApplicabilityType_Description,
	   
			   'N/A' AS [SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue],
			   'N/A' AS [SecondaryCourseLevelCharacteristicTypeDescriptor_Description],
			   CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(c.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS CourseModifiedDate,

				--Making sure the first time, the ValidFrom is set to beginning of time 
				CASE WHEN @LastLoadDate <> '07/01/2015' THEN
				           (SELECT MAX(t) FROM
                             (VALUES
                               (c.LastModifiedDate)                                               
                             ) AS [MaxLastModifiedDate](t)
                           )
					ELSE 
					      '07/01/2015' -- setting the validFrom to beggining of time during thre first load. 
				END AS ValidFrom,
			   '12/31/9999' as ValidTo,
				1 AS IsCurrent
		--select *
		FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Course c --WHERE c.CourseCode = '094'
			 LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.CourseLevelCharacteristic clc ON c.CourseCode = clc.CourseCode
			 LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.CourseLevelCharacteristicType clct ON clc.CourseLevelCharacteristicTypeId = clct.CourseLevelCharacteristicTypeId
			 LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.AcademicSubjectType ast ON c.AcademicSubjectDescriptorId = ast.AcademicSubjectTypeId
			 LEFT JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.CourseGPAApplicabilityType cgat ON c.CourseGPAApplicabilityTypeId = cgat.CourseGPAApplicabilityTypeId
		WHERE EXISTS (SELECT 1 
					  FROM  [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.CourseOffering co 
					  WHERE c.CourseCode = co.CourseCode
						AND co.SchoolYear IN (2019,2020)) AND
			 (c.LastModifiedDate > @LastLoadDate AND c.LastModifiedDate <= @NewLoadDate)
			
							
		
		--[v34_v34_EdFi_BPS_Production_Ods]
		--loading legacy data if it has not been loaded.
		--load types are ignored as this data will only be loaded once.
		IF NOT EXISTS(SELECT 1 
		              FROM dbo.DimCourse 
					  WHERE CHARINDEX('LegacyDW',_sourceKey,1) > 0)
			BEGIN
			   INSERT INTO Staging.Course
				   (_sourceKey,
				    CourseCode,
					CourseTitle,
					CourseDescription,
					CourseLevelCharacteristicTypeDescriptor_CodeValue,
					CourseLevelCharacteristicTypeDescriptor_Description,
					AcademicSubjectDescriptor_CodeValue,
					AcademicSubjectDescriptor_Description,
					HighSchoolCourseRequirement_Indicator,
					MinimumAvailableCredits,
					MaximumAvailableCredits,
					GPAApplicabilityType_CodeValue,
					GPAApplicabilityType_Description,
					SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue,
					SecondaryCourseLevelCharacteristicTypeDescriptor_Description,
					CourseModifiedDate,
					ValidFrom,
					ValidTo,
					IsCurrent)
				 SELECT DISTINCT 
					   CONCAT('LegacyDW|',c.CourseNumber,'-',CASE WHEN s.SectionID = '' THEN 'N/A' ELSE s.SectionID END) AS [_sourceKey],
					   CONCAT_WS('-',c.CourseNumber,CASE WHEN s.SectionID = '' THEN 'N/A' ELSE s.SectionID END) AS [CourseCode],
					   c.TitleFull,
					   c.TitleFull,
					   COALESCE(c.LevelCode,'N/A') AS [CourseLevelCharacteristicTypeDescriptor_CodeValue],
					   COALESCE(c.LevelCode,'N/A') AS [CourseLevelCharacteristicTypeDescriptor_Description],

					   COALESCE(c.Department,'N/A') AS [AcademicSubjectDescriptor_CodeValue],
					   COALESCE(c.Department,'N/A') AS [AcademicSubjectDescriptor_Description],
					   0 AS [HighSchoolCourseRequirement_Indicator],

					   NULL AS MinimumAvailableCredits,
					   NULL AS MaximumAvailableCredits,
					   CASE WHEN COALESCE(c.InGPA,'N') = 'Y' THEN 'Applicable' ELSE 'Not Applicable' END  AS GPAApplicabilityType_CodeValue,
					   CASE WHEN COALESCE(c.InGPA,'N') = 'Y' THEN 'Applicable' ELSE 'Not Applicable' END AS GPAApplicabilityType_Description,
					   COALESCE(c.MassCore,'N/A') AS [SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue],
					   COALESCE(c.MassCore,'N/A') AS [SecondaryCourseLevelCharacteristicTypeDescriptor_Description],
					   '07/01/2015' AS CourseModifiedDate,
					   '07/01/2015' AS ValidFrom,
					    GETDATE() as ValidTo,
						0 AS IsCurrent
				--select *
				FROM [BPSGranary02].[RAEDatabase].[dbo].[CourseCatalog_aspen] c  --   WHERE CourseNumber = '094' AND SchoolYear IN ('2017-2018','2016-2017','2015-2016')
					 INNER JOIN [BPSGranary02].[RAEDatabase].[dbo].[StudentCourseGrade_aspenNewFormat] s ON c.CourseNumber = s.CourseNumber
																										AND c.SchoolYear = s.SchoolYear
				WHERE c.SchoolYear IN ('2017-2018','2016-2017','2015-2016');
			

			END

		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

		
	END CATCH;
END;
GO

--Fact StudentAttendanceByDay
----------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_FactStudentAttendanceByDay_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
	
		--DECLARE @LastLoadDate datetime= '07/01/2015' DECLARE @NewLoadDate datetime = GETDATE()
		TRUNCATE TABLE Staging.StudentAttendanceByDay	
		CREATE TABLE #StudentsToBeProcessed (StudentUSI INT, 
		                                     EventDate DATE ,
											 LastModifiedDate DATETIME )
		  
		CREATE TABLE #AttedanceEventRankedByReason (StudentUSI INT, 
		                                            SchoolId INT, 
													SchoolYear SMALLINT, 
													EventDate DATE, 
													LastModifiedDate DATETIME,
													AttendanceEventCategoryDescriptorId INT,
													AttendanceEventReason NVARCHAR(max) , 
													RowId INT  )
	    CREATE TABLE #DistinctAttedanceEvents (StudentUSI INT, 
		                                       SchoolId INT, 
											   SchoolYear SMALLINT, 
											   EventDate DATE, 
											   LastModifiedDate DATETIME,
											   AttendanceEventCategoryDescriptorId INT,
											   AttendanceEventReason NVARCHAR(max))
	
		CREATE NONCLUSTERED INDEX [#AttedanceEventRankedByReason_MainCovering]
		ON [dbo].[#AttedanceEventRankedByReason] ([StudentUSI],[SchoolId],[EventDate],[RowId])
		INCLUDE ([AttendanceEventCategoryDescriptorId],[AttendanceEventReason])


		INSERT INTO #DistinctAttedanceEvents
		(
		    StudentUSI,
		    SchoolId,
		    SchoolYear,
		    EventDate,
			LastModifiedDate,
		    AttendanceEventCategoryDescriptorId,
		    AttendanceEventReason
		)
		SELECT   DISTINCT 
					StudentUSI, 
					SchoolId, 
					SchoolYear, 
					EventDate,
					LastModifiedDate,
					AttendanceEventCategoryDescriptorId,					
					LTRIM(RTRIM(COALESCE(AttendanceEventReason,''))) AS AttendanceEventReason 
		FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentSchoolAttendanceEvent
		WHERE SchoolYear >= 2019
			AND (LastModifiedDate > @LastLoadDate  AND LastModifiedDate <= @NewLoadDate)
		

		INSERT INTO #AttedanceEventRankedByReason
		(
			StudentUSI,
			SchoolId,
			SchoolYear,
			EventDate,
			LastModifiedDate,
			AttendanceEventCategoryDescriptorId,
			AttendanceEventReason,
			RowId
		)
		SELECT DISTINCT  
		            StudentUSI, 
					SchoolId, 
					SchoolYear, 
					EventDate,
					LastModifiedDate,
					AttendanceEventCategoryDescriptorId,
					AttendanceEventReason , 
					ROW_NUMBER() OVER (PARTITION BY StudentUSI, 
													SchoolId, 
													SchoolYear, 
													EventDate,
													AttendanceEventCategoryDescriptorId
										ORDER BY AttendanceEventReason DESC) AS RowId 
			FROM #DistinctAttedanceEvents

			
		IF (@LastLoadDate <> '07/01/2015')
			BEGIN
				INSERT INTO #StudentsToBeProcessed (StudentUSI, EventDate, LastModifiedDate)
				SELECT DISTINCT StudentUSI, EventDate, LastModifiedDate
				FROM #DistinctAttedanceEvents
			END
	    ELSE --this first time all students will be processed
			BEGIN
				INSERT INTO #StudentsToBeProcessed (StudentUSI, EventDate, LastModifiedDate)
				SELECT DISTINCT StudentUSI, NULL AS EventDate, NULL AS LastModifiedDate --we don't care about event changes the first this runs. 
				FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation
				WHERE SchoolYear >= 2019
			END;
		
		
		
		INSERT INTO Staging.StudentAttendanceByDay
		(
		    _sourceKey,
		    StudentKey,
		    TimeKey,
		    SchoolKey,
		    AttendanceEventCategoryKey,
		    AttendanceEventReason,
		    ModifiedDate,
		    _sourceStudentKey,
		    _sourceTimeKey,
		    _sourceSchoolKey,
		    _sourceAttendanceEventCategoryKey
		)	
		SELECT DISTINCT         
				  CONCAT_WS('|',Convert(NVARCHAR(MAX),ssa.StudentUSI),CONVERT(CHAR(10), cdce.Date, 101)) AS _sourceKey,
				  NULL AS StudentKey,
				  NULL AS TimeKey,	  
				  NULL AS SchoolKey,  
				  NULL AS AttendanceEventCategoryKey,				  
				  ISNULL(ssae.AttendanceEventReason,'') AS AttendanceEventReason,
				  --stbp.LastModifiedDate only makes sense when identifying deltas, the first time we just follow the calendar date
				  cdce.Date AS ModifiedDate,
				  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),ssa.StudentUSI)) AS _sourceStudentKey,
		          cdce.Date AS _sourceTimeKey,		          
				  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),ssa.SchoolId))  AS _sourceSchoolKey,
		          CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),ssae.AttendanceEventCategoryDescriptorId))  AS _sourceAttendanceEventCategoryKey
				  				  
			--select *  
			FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa 
				INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.CalendarDate cda on ssa.SchoolId = cda.SchoolId 														   
				INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.CalendarDateCalendarEvent cdce on cda.Date=cdce.Date 
																					 and cda.SchoolId=cdce.SchoolId
				INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor d_cdce on cdce.CalendarEventDescriptorId = d_cdce.DescriptorId
																	  and d_cdce.CodeValue='Instructional day' -- ONLY Instructional days
	            INNER JOIN  #StudentsToBeProcessed stbp ON ssa.StudentUSI = stbp.StudentUSI
				                                      AND (stbp.EventDate IS NULL OR 
													       cdce.Date = stbp.EventDate)
				LEFT JOIN #AttedanceEventRankedByReason ssae on ssa.StudentUSI = ssae.StudentUSI
															   AND ssa.SchoolId = ssae.SchoolId 
															   AND cda.Date = ssae.EventDate
															   AND ssae.RowId= 1			
			WHERE  cdce.Date >= ssa.EntryDate 
			   AND cdce.Date <= GETDATE()
			   AND (
					 (ssa.ExitWithdrawDate is null) 
					  OR
					 (ssa.ExitWithdrawDate is not null and cdce.Date<=ssa.ExitWithdrawDate) 
				   )
				AND ssa.SchoolYear >= 2019
				
			DROP TABLE #StudentsToBeProcessed, #AttedanceEventRankedByReason, #DistinctAttedanceEvents;
			
			
		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		
	END CATCH;
END;
GO

--Fact StudentDiscipline
----------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_FactStudentDiscipline_PopulateStaging] 
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
    
		--DECLARE @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate();
		TRUNCATE TABLE Staging.StudentDiscipline
			
		INSERT INTO Staging.StudentDiscipline
		(
		    _sourceKey,
		    StudentKey,
		    TimeKey,
		    SchoolKey,
		    DisciplineIncidentKey,
		    ModifiedDate,
		    _sourceStudentKey,
		    _sourceTimeKey,
		    _sourceSchoolKey,
		    _sourceDisciplineIncidentKey
		)
		
		SELECT DISTINCT 
		       CONCAT_WS('|',Convert(NVARCHAR(MAX),sdia.StudentUSI),di.IncidentIdentifier) AS _sourceKey,
			   NULL AS StudentKey,
			   NULL AS TimeKey,	  
			   NULL AS SchoolKey,  
			   NULL AS DisciplineIncidentKey,  
			   di.IncidentDate AS ModifiedDate,
			   CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),sdia.StudentUSI)) AS _sourceStudentKey,
		       di.IncidentDate AS _sourceTimeKey,		          
			   CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),di.SchoolId))  AS _sourceSchoolKey,
		       CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),di.IncidentIdentifier))  AS  _sourceDisciplineIncidentKey

		FROM  [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.DisciplineIncident di       
			  INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentDisciplineIncidentAssociation sdia ON di.IncidentIdentifier = sdia.IncidentIdentifier
		WHERE di.IncidentDate >= '07/01/2018'	  
		 AND  (
		       (di.LastModifiedDate > @LastLoadDate  AND di.LastModifiedDate <= @NewLoadDate)
			     OR
		       (sdia.LastModifiedDate > @LastLoadDate  AND sdia.LastModifiedDate <= @NewLoadDate)
			  )
		 
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		
	END CATCH;
END;
GO

--Fact StudentAssessmentScore
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_FactStudentAssessmentScore_PopulateStaging]
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY
    
		--DECLARE @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate();
		--select * from Staging.StudentAssessmentScore
		TRUNCATE TABLE Staging.StudentAssessmentScore
			
		INSERT INTO Staging.StudentAssessmentScore
		(
		    _sourceKey,
		    StudentKey,
		    TimeKey,
		    AssessmentKey,
		    ScoreResult,
		    IntegerScoreResult,
		    DecimalScoreResult,
		    LiteralScoreResult,
		    ModifiedDate,
		    _sourceStudentKey,
		    _sourceTimeKey,
		    _sourceAssessmentKey
		)
		
		
		SELECT   DISTINCT 
			      CONCAT_WS('|',CONVERT(NVARCHAR(MAX),s.StudentUSI),sa.StudentAssessmentIdentifier) AS _sourceKey,				  
				  NULL AS StudentKey,
				  NULL AS TimeKey,	  
				  NULL AS AssessmentKey,
				  sas.Result AS [SoreResult],
				  CASE when ascr_rdtt.CodeValue in ('Integer') AND TRY_CAST(sas.Result AS INTEGER) IS NOT NULL AND sas.Result <> '-' THEN sas.Result ELSE NULL END AS IntegerScoreResult,
				  CASE when ascr_rdtt.CodeValue in ('Decimal','Percentage','Percentile')  AND TRY_CAST(sas.Result AS FLOAT)  IS NOT NULL THEN sas.Result ELSE NULL END AS DecimalScoreResult,
				  CASE when ascr_rdtt.CodeValue not in ('Integer','Decimal','Percentage','Percentile') THEN sas.Result ELSE NULL END AS LiteralScoreResult,
				  CONVERT(DATE ,sa.AdministrationDate) AS ModifiedDate ,
				  CONCAT_WS('|','Ed-Fi',CONVERT(NVARCHAR(MAX),s.StudentUSI)) AS  _sourceStudentKey,
				  CONVERT(DATE ,sa.AdministrationDate) AS  _sourceTimeKey,
				  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),sa.AssessmentIdentifier),'N/A',Convert(NVARCHAR(MAX),armt.CodeValue)) AS  _sourceAssessmentKey
			--select top 1 'Ed-Fi|' + Convert(NVARCHAR(MAX),sa.AssessmentIdentifier)  + '|N/A|' + Convert(NVARCHAR(MAX),armt.CodeValue), sa.AdministrationDate,*  
			FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].[edfi].Student s 
      
				--student assessment
				inner join [EDFISQL01].[v34_EdFi_BPS_Production_Ods].[edfi].StudentAssessment sa on sa.StudentUSI = s.StudentUSI
      
				--student assessment score results
				inner join [EDFISQL01].[v34_EdFi_BPS_Production_Ods].[edfi].StudentAssessmentScoreResult sas on sa.StudentAssessmentIdentifier = sas.StudentAssessmentIdentifier
																and sa.AssessmentIdentifier = sas.AssessmentIdentifier

				--assessment 
				inner join [EDFISQL01].[v34_EdFi_BPS_Production_Ods].[edfi].Assessment a on sa.AssessmentIdentifier = a.AssessmentIdentifier 

				inner join [EDFISQL01].[v34_EdFi_BPS_Production_Ods].[edfi].AssessmentScore ascr on sas.AssessmentIdentifier = ascr.AssessmentIdentifier 
													and sas.[AssessmentReportingMethodTypeId] = ascr.[AssessmentReportingMethodTypeId]
				inner join [EDFISQL01].[v34_EdFi_BPS_Production_Ods].[edfi].[AssessmentReportingMethodType] armt on ascr.[AssessmentReportingMethodTypeId] = armt.[AssessmentReportingMethodTypeId]

				INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.ResultDatatypeType ascr_rdtt ON ascr.ResultDatatypeTypeId = ascr_rdtt.ResultDatatypeTypeId	
				
			WHERE CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1 
				 AND sa.AdministrationDate >= '07/01/2018'		
				 AND  (
					   (sa.LastModifiedDate > @LastLoadDate  AND sa.LastModifiedDate <= @NewLoadDate)			     
					  )

		UNION ALL
		SELECT   DISTINCT 
			      CONCAT_WS('|',CONVERT(NVARCHAR(MAX),s.StudentUSI),sa.StudentAssessmentIdentifier),
				  NULL AS StudentKey,
				  NULL AS TimeKey,	  
				  NULL AS AssessmentKey,
				  apl_ld.CodeValue AS [SoreResult],
				  NULL AS IntegerScoreResult,
				  NULL AS DecimalScoreResult,
				  apl_ld.CodeValue AS LiteralScoreResult,	  
				  CONVERT(DATE ,sa.AdministrationDate) AS ModifiedDate ,
				  CONCAT_WS('|','Ed-Fi',CONVERT(NVARCHAR(MAX),s.StudentUSI)) AS  _sourceStudentKey,
				  CONVERT(DATE ,sa.AdministrationDate) AS  _sourceTimeKey,
				  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),sa.AssessmentIdentifier),'N/A',Convert(NVARCHAR(MAX),apl_sd.CodeValue)) AS  _sourceAssessmentKey
			--select top 100 *  
			FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].[edfi].Student s 
      
				--student assessment
				inner join [EDFISQL01].[v34_EdFi_BPS_Production_Ods].[edfi].StudentAssessment sa on sa.StudentUSI = s.StudentUSI 
	
				inner  join [EDFISQL01].[v34_EdFi_BPS_Production_Ods].[edfi].StudentAssessmentPerformanceLevel sapl on sa.StudentAssessmentIdentifier = sapl.StudentAssessmentIdentifier
																		 and sa.AssessmentIdentifier = sapl.AssessmentIdentifier
															 --    and apl.PerformanceLevelDescriptorId = sapl.PerformanceLevelDescriptorId
    
				--assessment 
				inner join [EDFISQL01].[v34_EdFi_BPS_Production_Ods].[edfi].Assessment a on sa.AssessmentIdentifier = a.AssessmentIdentifier 

				inner join [EDFISQL01].[v34_EdFi_BPS_Production_Ods].[edfi].[AssessmentPerformanceLevel] apl on sa.AssessmentIdentifier = apl.AssessmentIdentifier 
																 and sapl.[AssessmentReportingMethodTypeId] = apl.[AssessmentReportingMethodTypeId]
																 and sapl.PerformanceLevelDescriptorId = apl.PerformanceLevelDescriptorId
    
				inner join [EDFISQL01].[v34_EdFi_BPS_Production_Ods].[edfi].[AssessmentReportingMethodType] apl_sd on apl.[AssessmentReportingMethodTypeId] = apl_sd.[AssessmentReportingMethodTypeId] 
				inner join [EDFISQL01].[v34_EdFi_BPS_Production_Ods].[edfi].Descriptor apl_ld on apl.PerformanceLevelDescriptorId = apl_ld.DescriptorId 

			WHERE CHARINDEX('MCAS',a.AssessmentIdentifier,1) = 1           
				 AND sa.AdministrationDate >= '07/18/2018'
				 AND  (
					   (sa.LastModifiedDate > @LastLoadDate  AND sa.LastModifiedDate <= @NewLoadDate)			     
					  )
		 
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		
	END CATCH;
END;
GO

--Fact StudentCourseTranscript
----------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE [dbo].[Proc_ETL_FactStudentCourseTranscript_PopulateStaging] 
@LastLoadDate datetime,
@NewLoadDate datetime
AS
BEGIN
    --added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	--current session wont be the deadlock victim if it is involved in a deadlock with other sessions with the deadlock priority set to LOW
	SET DEADLOCK_PRIORITY HIGH;
	
	--When SET XACT_ABORT is ON, if a Transact-SQL statement raises a run-time error, the entire transaction is terminated and rolled back.
	SET XACT_ABORT ON;

	--This will allow for dirty reads. By default SQL Server uses "READ COMMITED" 
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;



	BEGIN TRY

		--DECLARE @LastLoadDate datetime = '07/01/2015' declare @NewLoadDate datetime = getdate();
		--select * from Staging.StudentCourseTranscript
		TRUNCATE TABLE Staging.StudentCourseTranscript	
		INSERT INTO Staging.StudentCourseTranscript
		(
		    _sourceKey,
		    StudentKey,
		    TimeKey,
		    CourseKey,
		    SchoolKey,
		    EarnedCredits,
		    PossibleCredits,
		    FinalLetterGradeEarned,
		    FinalNumericGradeEarned,
		    ModifiedDate,
		    _sourceStudentKey,
		    _sourceSchoolYear,
			_sourceTerm,
		    _sourceCourseKey,
		    _sourceSchoolKey
		)
		
		SELECT DISTINCT
			   CONCAT_WS('|',Convert(NVARCHAR(MAX),ct.StudentUSI),Convert(NVARCHAR(MAX),ct.SchoolYear),Convert(NVARCHAR(MAX),ct.SchoolId),Convert(NVARCHAR(MAX),ct.CourseCode),td.CodeValue) AS _sourceKey,
			   NULL AS StudentKey,
			   NULL AS TimeKey,	  
			   NULL AS CourseKey,
			   NULL AS SchoolKey,  
			   ct.EarnedCredits,
			   ct.AttemptedCredits,
			   ct.FinalLetterGradeEarned,
			   ct.FinalNumericGradeEarned,			   
			   ct.LastModifiedDate AS ModifiedDate,
			   CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),ct.StudentUSI)) AS _sourceStudentKey,
		       ct.SchoolYear AS _sourceSchoolYear,		          
			   td.CodeValue AS _sourceTerm,		          
			   CONCAT_WS('|','Ed-Fi',ct.CourseCode)  AS _sourceCourseKey,
			   CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),ct.SchoolId))  AS _sourceSchoolKey
		--select *  
		FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.CourseTranscript ct
			INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor td ON ct.TermDescriptorId = td.DescriptorId
		WHERE  			
			  ct.SchoolYear >= 2019			
			 AND  (
					   (ct.LastModifiedDate > @LastLoadDate  AND ct.LastModifiedDate <= @NewLoadDate)			     
				  )
		
	END TRY
	BEGIN CATCH
		
		--constructing exception details
		DECLARE
		   @errorMessage nvarchar( MAX ) = ERROR_MESSAGE( );		
     
		DECLARE
		   @errorDetails nvarchar( MAX ) = CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);

		PRINT @errorDetails;
		THROW 51000, @errorDetails, 1;

		
		PRINT CONCAT('An error had ocurred executing SP:',OBJECT_NAME(@@PROCID),'. Error details: ', @errorMessage);
		
		-- Test XACT_STATE:
		-- If  1, the transaction is committable.
		-- If -1, the transaction is uncommittable and should be rolled back.
		-- XACT_STATE = 0 means that there is no transaction and a commit or rollback operation would generate an error.

	END CATCH;
END; 
GO



