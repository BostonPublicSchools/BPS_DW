DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM BPS_DW.[dbo].[Lineage] WHERE TableName= 'dbo.DimTime')
BEGIN
    INSERT INTO BPS_DW.[dbo].[Lineage]
	(
	 [TableName], 
	 [StartTime], 
	 [EndTime], 
	 [LoadType], 
	 [Status]
	)
	VALUES
	('dbo.DimTime', 
	 GETDATE(), 
	 NULL, 
	 'F'
	 , -- full 
	 ''
	); -- Processing
	SET @lineageKey = SCOPE_IDENTITY() ;
END;
ELSE
BEGIN
     SELECT @lineageKey = LineageKey
	 FROM BPS_DW.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.DimTime'
END 

--generating those values that do not vary by school
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




DECLARE @startDate DATE = '20150701';
DECLARE @endDate DATE = '20200630';

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
           SchoolYear = BPS_DW.dbo.Func_GetSchoolYear(TheDate),
           SchoolYearDescription = CONVERT(NVARCHAR(MAX), BPS_DW.dbo.Func_GetSchoolYear(TheDate) - 1) + ' - '
                                   + CONVERT(NVARCHAR(MAX), BPS_DW.dbo.Func_GetSchoolYear(TheDate)),
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
           FederalHolidayName = BPS_DW.[dbo].[Func_GetHolidayFromDate](TheDate), -- Memorial Day, 4th of July
           FederalHoliday_Indicator = (CASE
											WHEN BPS_DW.[dbo].[Func_GetHolidayFromDate](TheDate) = 'Non-Holiday' THEN
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

;WITH EdFiSchools AS
(
	SELECT cd.Date as SchoolDate, 
		   'Ed-Fi|' + Convert(NVARCHAR(MAX),s.SchoolId) AS [_sourceKey],
		   --ses.SessionName,
		   td.CodeValue TermDescriptorCodeValue,
		   td.Description TermDescriptorDescription,       
		   cet.CodeValue CalendarEventTypeCodeValue,
		   cet.Description CalendarEventTypeDescription, 
	       ROW_NUMBER() OVER (PARTITION BY ses.SchoolYear, s.SchoolId ORDER BY DATEADD(YEAR,9,cd.Date)) AS DayOfSchoolYear
	FROM [EdFi_BPS_Staging_Ods].edfi.School s
		INNER JOIN [EdFi_BPS_Staging_Ods].edfi.EducationOrganization edOrg  ON s.SchoolId = edOrg.EducationOrganizationId
		INNER JOIN [EdFi_BPS_Staging_Ods].edfi.CalendarDate cd ON s.SchoolId = cd.SchoolId
		INNER JOIN [EdFi_BPS_Staging_Ods].edfi.CalendarDateCalendarEvent cdce ON cd.SchoolId = cdce.SchoolId
																							AND cd.Date = cdce.Date
		INNER JOIN [EdFi_BPS_Staging_Ods].edfi.CalendarEventDescriptor ced  ON cdce.CalendarEventDescriptorId = ced.CalendarEventDescriptorId
		INNER JOIN [EdFi_BPS_Staging_Ods].edfi.Descriptor cedv  ON ced.CalendarEventDescriptorId = cedv.DescriptorId
		INNER JOIN [EdFi_BPS_Staging_Ods].edfi.CalendarEventType cet ON ced.CalendarEventTypeId = cet.CalendarEventTypeId
		INNER JOIN [EdFi_BPS_Staging_Ods].edfi.Session ses ON s.SchoolId = ses.SchoolId
																		 AND cd.Date BETWEEN ses.BeginDate AND ses.EndDate
		INNER JOIN [EdFi_BPS_Staging_Ods].edfi.Descriptor td ON ses.TermDescriptorId = td.DescriptorId
	   -- ORDER BY [_sourceKey], ses.SchoolYear, SchoolDate
)



/*
SELECT * FROM [EdFi_BPS_Staging_Ods].edfi.CalendarDate
SELECT * FROM [EdFi_BPS_Staging_Ods].edfi.Session
SELECT * FROM [EdFi_BPS_Staging_Ods].edfi.TermType
SELECT * FROM [EdFi_BPS_Staging_Ods].edfi.SessionGradingPeriod
SELECT * FROM [EdFi_BPS_Staging_Ods].edfi.GradingPeriod
SELECT * FROM [EdFi_BPS_Staging_Ods].edfi.Descriptor where namespace = 'http://ed-fi.org/Descriptor/TermDescriptor.xml'
*/


INSERT INTO BPS_DW.[dbo].[DimTime]
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
           
		   ,[SchoolKey]
		   ,DayOfSchoolYear
           ,SchoolCalendarEventType_CodeValue
           ,SchoolCalendarEventType_Description
           ,SchoolTermDescriptor_CodeValue
           ,SchoolTermDescriptor_Description
		   
           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey])
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
	  ,ds.SchoolKey
	  ,es.DayOfSchoolYear
	  ,es.CalendarEventTypeCodeValue
	  ,es.CalendarEventTypeDescription
	  ,es.TermDescriptorCodeValue
	  ,es.TermDescriptorDescription	  
	   ,GETDATE() AS ValidFrom
	   ,CASE WHEN ds._sourceKey IS NOT NULL THEN 
		      CASE WHEN ds.IsCurrent = 1 THEN '12/31/9999' 
			    ELSE GETDATE() 
			  END
		 ELSE
		     '12/31/9999' 
		 END  AS ValidTo
	    ,CASE WHEN ds._sourceKey IS NOT NULL THEN ds.IsCurrent ELSE  1 end AS IsCurrent
	    ,@lineageKey AS [LineageKey]
FROM @NonSchoolTime nst
     LEFT JOIN EdFiSchools es ON nst.SchoolDate = es.SchoolDate
	 left JOIN BPS_DW.dbo.DimSchool ds ON es._sourceKey = ds._sourceKey
WHERE NOT EXISTS(SELECT 1 
					FROM BPS_DW.[dbo].[DimTime] dt 
					WHERE nst.[SchoolDate] = dt.[SchoolDate]
					  AND (
					          (ds.SchoolKey IS NULL AND dt.SchoolKey IS NULL) 
						    OR  
					          (ds.SchoolKey = dt.SchoolKey) 
						  )
				  )
	 --ORDER BY es.SchoolDate
 
--select * from BPS_DW.[dbo].[DimTime]


 --updatng the lineage table
UPDATE BPS_DW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;

