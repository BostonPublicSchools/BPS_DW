

				   SELECT * FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School 
				   SELECT * FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization 
				   SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor WHERE DescriptorId = 433


DECLARE @minFrom INT = 12
DECLARE @maxFrom INT = 13000

DECLARE @minTo INT = 1
DECLARE @maxTo INT = 32

DECLARE @valueToBeScaled INT =13000

SELECT (((@maxTo-@minTo)*(@valueToBeScaled-@minFrom))/(@maxFrom-@minFrom)) + @minTo

SELECT * FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Program ORDER BY ProgramName

DROP PROCEDURE IF EXISTS  [dbo].[Proc_ETL_DimStudent_PopulateStaging];

SELECT * FROM dbo.DimStudent WHERE  EntrySchoolYear = 2022  IsCurrent = 1  AND ValidTo = '12/31/9999' -- StudentUniqueId = '384047' 

SELECT * FROM dbo.DimStudent WHERE StudentUniqueId = '400441' ORDER BY ValidFrom DESC; --ValidTo < ValidFrom StudentUniqueId = '384047' ORDER BY EntryDate DESC

--DESC   IsCurrent = 1  AND ValidTo = '2021-03-06 07:58:06.400' -- StudentUniqueId = '384047' 

SELECT * FROM dbo.DimCourse WHERE CourseCode ='039'

SELECT CourseCode, COUNT(*) AS Total 
FROM dbo.DimCourse 
WHERE IsCurrent = 1 
GROUP BY CourseCode
HAVING COUNT(*) > 1
ORDER BY CourseKey  AND CourseCode = '032'WHERE  SchoolKey IN (54,1544)
SELECT * FROM dbo.DimSchool WHERE  SchoolKey IN (54,1544)
SELECT * FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization WHERE 

SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School WHERE SchoolId = 1265
SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization WHERE EducationOrganizationId = 1265

SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School WHERE Short
SELECT distinct edorg.LastModifiedDate, ost.LastModifiedDate , sct.LastModifiedDate, tIt.LastModifiedDate 
		FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School s
		     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization edorg on s.SchoolId = edorg.EducationOrganizationId
		     INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.OperationalStatusType ost ON edorg.OperationalStatusTypeId = ost.OperationalStatusTypeId
		     LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolCategory sc on s.SchoolId = sc.SchoolId
		     LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolCategoryType sct on sc.SchoolCategoryTypeId = sct.SchoolCategoryTypeId
		     LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.TitleIPartASchoolDesignationType tIt on s.TitleIPartASchoolDesignationTypeId = tIt.TitleIPartASchoolDesignationTypeId
		     LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganizationIdentificationCode eoic ON edorg.EducationOrganizationId = eoic.EducationOrganizationId 
		     																			   AND eoic.EducationOrganizationIdentificationSystemDescriptorId = 433 --state code
		     LEFT JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganizationIdentificationCode eoic_sch ON edorg.EducationOrganizationId = eoic_sch.EducationOrganizationId 
																					   AND eoic_sch.EducationOrganizationIdentificationSystemDescriptorId = 428 --district code
		WHERE edorg.NameOfInstitution = 'Fenway High School'


	
	 --CASE when ssa.ExitWithdrawDate is null then '12/31/9999'  else ssa.ExitWithdrawDate END  AS ValidTo,
	--Case when ssa.ExitWithdrawDate is NULL AND EXISTS(SELECT 1 FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolYearType syt WHERE syt.CurrentSchoolYear = 1 AND syt.SchoolYear = ssa.SchoolYear) then 1 else 0 end AS IsCurrent
			   	
select s.LastModifiedDate, ssa.LastModifiedDate , ssa.EntryDate, ssa.ExitWithdrawDate

FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s
	INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa ON s.StudentUSI = ssa.StudentUSI
		WHERE s.StudentUniqueId = 384047
SELECT * FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolYearType WHERE CurrentSchoolYear = 1

/*
I want to know the individual incidents for the school per year so that I can group 
by the school or by year by OSS and ISS, by race. 
*/

SELECT DISTINCT 
       [StudentKey]
      ,[TimeKey]
      ,[SchoolKey]
      ,[DisciplineIncidentKey]
      ,[StudentId]
      ,[StudentStateId]
      ,[FirstName]
      ,[LastName]
      ,[RaceCode]
      ,[GradeLevel]
      ,[DistrictSchoolCode]
      ,[UmbrellaSchoolCode]
      ,[SchoolName]
      ,[IncidentDate]
      ,[IncidentSchoolYear]
      ,[SchoolTerm]
      ,[IncidentIdentifier]
      ,[IncidentTime]
      ,[IncidentDescription]
      ,[IncidentType]
      ,[IncidentLocation]
      ,[IncidentAction]
      ,[IncidentReporter]
      ,[IsISS]
      ,[IsOSS]
FROM EdFiDW.[dbo].[View_StudentDiscipline]
WHERE UmbrellaSchoolCode = '1040' -- Brighton High School
  AND IncidentSchoolYear = 2020
ORDER BY [StudentId], IncidentDate


SELECT 
      DistrictSchoolCode,
	  SchoolName,
	  IncidentSchoolYear,
	  RaceCode,
      COUNT(DISTINCT [IncidentIdentifier]) AS TotalIncidents
FROM EdFiDW.dbo.View_StudentDiscipline
GROUP BY DistrictSchoolCode,
	     SchoolName,
	     IncidentSchoolYear,
		 RaceCode
ORDER BY SchoolName, IncidentSchoolYear, TotalIncidents


SELECT 
      DistrictSchoolCode,
	  SchoolName,
	  IncidentSchoolYear,
	  IsOSS,
      COUNT(DISTINCT [IncidentIdentifier]) AS TotalIncidents
FROM EdFiDW.dbo.View_StudentDiscipline
GROUP BY DistrictSchoolCode,
	     SchoolName,
	     IncidentSchoolYear,
		 IsOSS
ORDER BY SchoolName, IncidentSchoolYear, TotalIncidents



SELECT 
      DistrictSchoolCode,
	  SchoolName,
	  IncidentSchoolYear,
	  IsISS,
      COUNT(DISTINCT [IncidentIdentifier]) AS TotalIncidents
FROM EdFiDW.dbo.View_StudentDiscipline
GROUP BY DistrictSchoolCode,
	     SchoolName,
	     IncidentSchoolYear,
		 IsISS
ORDER BY SchoolName, IncidentSchoolYear, TotalIncidents



/*
I want to be able to see ADA for each student so that I can group 
by the school or by year by OSS and ISS, by race. 
*/

SELECT *
FROM EdFiDW.dbo.View_StudentAttendance_ADA
WHERE UmbrellaSchoolCode = '1040' -- Brighton High School
  AND SchoolYear = 2020
ORDER BY StudentId, SchoolYear


SELECT SchoolName,
       SchoolYear,	   
       AVG(ADA) AvgADA
FROM EdFiDW.dbo.View_StudentAttendance_ADA
GROUP BY SchoolName,
         SchoolYear
ORDER BY SchoolName, SchoolYear;

        --schools
        UPDATE dbo.DimSchool
		SET IsLatest = 0;


		;WITH LatestEntry AS
        (
			SELECT DISTINCT 
				   d._sourceKey, 
				   d.SchoolKey AS TheKey, 
				   d.ValidFrom, 
				   d.ValidTo,
				   d.IsLatest,
				   ROW_NUMBER() OVER (PARTITION BY d._sourceKey ORDER BY d.ValidFrom Desc, d.ValidTo DESC) AS RowRankId
			FROM dbo.DimSchool d 
		)

		UPDATE d
		SET d.IsLatest = 1
		FROM dbo.DimSchool d
		WHERE EXISTS (SELECT 1 
			              FROM LatestEntry le
						  WHERE d.SchoolKey = le.TheKey 
						    AND le.RowRankId = 1);

		--time
        UPDATE dbo.DimTime
		SET IsLatest = 0;

		--time
		;WITH LatestEntry AS
        (
			SELECT DISTINCT 
				   d.SchoolDate AS _sourceKey, 
				   d.TimeKey AS TheKey, 
				   d.ValidFrom, 
				   d.ValidTo,
				   d.IsLatest,
				   ROW_NUMBER() OVER (PARTITION BY d.SchoolDate  ORDER BY d.ValidFrom Desc, d.ValidTo DESC) AS RowRankId
			FROM dbo.DimTime d  
		)

		UPDATE d
		SET d.IsLatest = 1
		FROM dbo.DimTime d
		WHERE EXISTS (SELECT 1 
			              FROM LatestEntry le
						  WHERE d.TimeKey = le.TheKey 
						    AND le.RowRankId = 1);

							
							SELECT * FROM dbo.DimTime WHERE SchoolDate = '2021-07-21'

		--staff
		WITH MyIds AS
		(
		  SELECT  CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),s.StaffUSI)) AS [_sourceKeyOld],
		          CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),s.StaffUniqueId)) AS [_sourceKeyNew]
		  FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Staff s
		)
		UPDATE s
		SET s._sourceKey = mi._sourceKeyNew
		FROM dbo.DimStaff s
		INNER JOIN MyIds mi ON s._sourceKey = mi._sourceKeyOld
		
		SELECT * FROM dbo.DimStaff WHERE _sourceKey = 'Ed-Fi|000829' ORDER BY  _sourceKey , ValidFrom

		SELECT * FROM dbo.DimStaff WHERE _sourceKey = 'Ed-Fi|000829' ORDER BY  _sourceKey , ValidFrom

		declare @LastLoadDate datetime = '2021-07-26 03:00:30.000' declare @NewLoadDate datetime = getdate()
		SELECT  DISTINCT 
			    CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),s.StaffUniqueId)) AS [_sourceKey]
				,sem.ElectronicMailAddress AS [PrimaryElectronicMailAddress]
				,emt.CodeValue AS [PrimaryElectronicMailTypeDescriptor_CodeValue]
				,emt.Description AS [PrimaryElectronicMailTypeDescriptor_Description]
				,s.StaffUniqueId
				,s.PersonalTitlePrefix
				,s.FirstName
				,s.MiddleName
				,LEFT(LTRIM(s.MiddleName),1) AS MiddleInitial	    
				,s.LastSurname
				,dbo.Func_ETL_GetFullName(s.FirstName,s.MiddleName,s.LastSurname) AS FullName
				,s.GenerationCodeSuffix
				,s.MaidenName        
				,s.BirthDate
				,DATEDIFF(YEAR, s.BirthDate, GetDate()) AS [StaffAge]
				,CASE 
					WHEN sex.CodeValue  = 'Male' THEN 'M'
					WHEN sex.CodeValue  = 'Female' THEN 'F'
					ELSE 'NS' -- not selected
				END AS SexType_Code
				,COALESCE(sex.CodeValue,'Not Selected') AS SexType_Description
				,CASE WHEN COALESCE(sex.CodeValue,'Not Selected')  = 'Male' THEN 1 ELSE 0 END AS SexType_Male_Indicator
				,CASE WHEN COALESCE(sex.CodeValue,'Not Selected')  = 'Female' THEN 1 ELSE 0 END AS SexType_Female_Indicator
				,CASE WHEN COALESCE(sex.CodeValue,'Not Selected')  = 'Not Selected' THEN 1 ELSE 0 END AS SexType_NotSelected_Indicator
				,COALESCE(d_le.CodeValue,'N/A') as [HighestLevelOfEducationDescriptorDescriptor_CodeValue]
				,COALESCE(d_le.Description,'N/A') as [HighestLevelOfEducationDescriptorDescriptor_Description]
				,s.YearsOfPriorProfessionalExperience
				,s.YearsOfPriorTeachingExperience
				,s.HighlyQualifiedTeacher
				,COALESCE(d_sc.CodeValue,'N/A') as StaffClassificationDescriptor_CodeValue
				,COALESCE(d_sc.Description,'N/A') as StaffClassificationDescriptor_Description
				,CASE WHEN @LastLoadDate <> '07/01/2015' THEN COALESCE(s.LastModifiedDate,'07/01/2015') ELSE '07/01/2015' END AS StaffMainInfoModifiedDate
				--Making sure the first time, the ValidFrom is set to beginning of time 
				,CASE WHEN @LastLoadDate <> '07/01/2015' THEN
				           (SELECT MAX(t) FROM
                             (VALUES
                               (s.LastModifiedDate)
                             ) AS [MaxLastModifiedDate](t)
                           )
					ELSE 
					      seoaa.BeginDate 
				END AS ValidFrom
				,case when seoaa.EndDate IS null then  '12/31/9999' else seoaa.EndDate  END AS ValidTo
				,case when seoaa.EndDate IS NULL OR seoaa.EndDate > GETDATE() THEN  1 else 0 end AS IsCurrent 
		--SELECT distinct s.*, seoaa.*
		FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Staff s 
			  INNER JOIN  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffEducationOrganizationAssignmentAssociation seoaa ON s.StaffUSI = seoaa.StaffUSI
			  --sex	 
			  left JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SexType sex ON s.SexTypeId = sex.SexTypeId
			  left join [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.OldEthnicityType oet on s.OldEthnicityTypeId = oet.OldEthnicityTypeId
			  left join [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CitizenshipStatusType cst on s.CitizenshipStatusTypeId = cst.CitizenshipStatusTypeId
			  left join [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d_le on s.HighestCompletedLevelOfEducationDescriptorId = d_le.DescriptorId
			  LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StaffElectronicMail sem ON s.StaffUSI = sem.StaffUSI
			 															  AND sem.PrimaryEmailAddressIndicator = 1
			  LEFT JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.ElectronicMailType emt ON sem.ElectronicMailTypeId = emt.ElectronicMailTypeId
			  left join [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor d_sc on seoaa.StaffClassificationDescriptorId = d_sc.DescriptorId
			 
		WHERE 
	    s.StaffUniqueId = '000829'		AND  (s.LastModifiedDate > @LastLoadDate AND s.LastModifiedDate <= @NewLoadDate)

		SELECT * FROM dbo.DimStaff WHERE _sourceKey = 'Ed-Fi|000829' ORDER BY  _sourceKey , ValidFrom
		SELECT * FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Staff WHERE StaffUniqueId = '000829'	
		
		--IsLatest - Staff
        UPDATE dbo.DimStaff
		SET IsLatest = 0;


		;WITH LatestEntry AS
        (
			SELECT DISTINCT 
				   d._sourceKey, 
				   d.StaffKey AS TheKey, 
				   d.ValidFrom, 
				   d.ValidTo,
				   d.IsLatest,
				   ROW_NUMBER() OVER (PARTITION BY d._sourceKey ORDER BY d.ValidFrom Desc, d.ValidTo DESC) AS RowRankId
			FROM dbo.DimStaff d 
		)

		UPDATE d
		SET d.IsLatest = 1
		FROM dbo.DimStaff d
		WHERE EXISTS (SELECT 1 
			              FROM LatestEntry le
						  WHERE d.StaffKey = le.TheKey 
						    AND le.RowRankId = 1);

		--Students
        UPDATE dbo.DimStudent
		SET IsLatest = 0;


		;WITH LatestEntry AS
        (
			SELECT DISTINCT 
				   d._sourceKey, 
				   d.StudentKey AS TheKey, 
				   d.ValidFrom, 
				   d.ValidTo,
				   d.IsLatest,
				   ROW_NUMBER() OVER (PARTITION BY d._sourceKey ORDER BY d.ValidFrom Desc, d.ValidTo DESC) AS RowRankId
			FROM dbo.DimStudent d 
		)

		UPDATE d
		SET d.IsLatest = 1
		FROM dbo.DimStudent d
		WHERE EXISTS (SELECT 1 
			              FROM LatestEntry le
						  WHERE d.StudentKey = le.TheKey 
						    AND le.RowRankId = 1);


	

		--IDs - Students
		WITH MyIds AS
		(
		  SELECT  CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),s.StudentUSI)) AS [_sourceKeyOld],
		          CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),s.StudentUniqueId)) AS [_sourceKeyNew]
		  FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s
		)
		UPDATE s
		SET s._sourceKey = mi._sourceKeyNew
		FROM dbo.DimStudent s
		INNER JOIN MyIds mi ON s._sourceKey = mi._sourceKeyOld

		--IsCurrent  - Students 
		UPDATE dbo.DimStudent
		SET IsCurrent = 0;
		
		WITH ActiveStudents AS 
		(SELECT CONCAT_WS('|','Ed-Fi', Convert(NVARCHAR(MAX),s.StudentUniqueId)) AS _sourceKey			   	
		--select ssa.SchoolYear, ssa.ExitWithdrawDate , case  when ssa.ExitWithdrawDate is NULL and EXISTS(SELECT 1 FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolYearType syt WHERE syt.CurrentSchoolYear = 1 AND syt.SchoolYear = ssa.SchoolYear)   then 1 else 0 end 
		FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student s
			INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation ssa ON s.StudentUSI = ssa.StudentUSI
		WHERE (ssa.ExitWithdrawDate is NULL OR ssa.ExitWithdrawDate >= GETDATE()) 
			             AND 
						 EXISTS(SELECT 1
						        FROM  [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SchoolYearType syt 
								WHERE syt.CurrentSchoolYear = 1 
								  AND syt.SchoolYear = ssa.SchoolYear)
	  )

	  UPDATE ds
	  SET ds.IsCurrent = 1
	  FROM  dbo.DimStudent ds
	  WHERE EXISTS (SELECT 1 FROM ActiveStudents ast WHERE ds._sourceKey = ast._sourceKey)




	  UPDATE st
SET st.EntrySchoolYear =   COALESCE(
									   (
										SELECT TOP 1 dt.SchoolYear
										FROM dbo.DimTime dt
										WHERE st.EntryDate = dt.SchoolDate
										   AND st.SchoolKey = dt.SchoolKey
										),
								   dbo.Func_ETL_GetSchoolYear((st.EntryDate))
								 ),
							  
	st.ExitWithdrawSchoolYear =   COALESCE(
	                                (
									SELECT TOP 1 dt.SchoolYear
									FROM dbo.DimTime dt
									WHERE st.ExitWithdrawDate = dt.SchoolDate
									   AND st.SchoolKey = dt.SchoolKey
									),
									dbo.Func_ETL_GetSchoolYear((st.ExitWithdrawDate))
								)
FROM  dbo.DimStudent st
     
      
select *
from dbo.View_StudentRoster
where StudentId = '202437'
and '2021-08-01'
      between ValidFrom and ValidTo

SELECT * FROM dbo.DimStudent WHERE StudentKey IN (44147,44148,44149)
SELECT * FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Student  WHERE StudentUniqueId = '202437'
SELECT * FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation  WHERE StudentUSI = 43004

SELECT * FROM dbo.DimSchool WHERE SchoolKey IN (8,64)
	  
SELECT * FROM  [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.CalendarDate WHERE Date = '2021-07-06' AND SchoolId = 1050



DECLARE @startDate DATE = '2021-09-24 01:00:01.000';
DECLARE @endDate DATE = '2021-09-25 01:00:01.000';
--SELECT   DATEDIFF(DAY, @startDate, @endDate)
--generating time fields that don't vary by school;
WITH daySeqs (daySeqNumber)
AS (SELECT 0
	UNION ALL
	SELECT daySeqNumber + 1
	FROM daySeqs
	WHERE daySeqNumber < DATEDIFF(DAY, @startDate, DATEADD(DAY,-1,@endDate))),
		theDates (theDate)
AS (SELECT DATEADD(DAY, daySeqNumber,@startDate)
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
			LastDayOfMonth = DATEADD(DAY, -1, DATEADD(MONTH, 1, TheFirstOfMonth)),
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

SELECT * FROM dbo.DimTime WHERE SchoolDate = '2021-09-23'


SELECT EventDate
     , COUNT(1)
	-- SELECT *
FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentSchoolAttendanceEvent 
      
   join [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.School
        on School.SchoolId = StudentSchoolAttendanceEvent.SchoolId
    join [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.EducationOrganization
       on EducationOrganization.EducationOrganizationId = School.SchoolId
    ---join [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.EducationOrganizationNetworkAssociation
       -- on EducationOrganizationNetworkAssociation.MemberEducationOrganizationId = EducationOrganization.EducationOrganizationId
where EventDate = '2021-09-10'
group by EventDate
order by EventDate desc;

SELECT * FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentSchoolAttendanceEvent

SELECT * FROM [dbo].[View_StudentAttendanceByDay] WHERE AttedanceDate = '2021-09-10'

SELECT   DISTINCT 
			ssae.StudentUSI, 
			s.StudentUniqueId,
			ssae.SchoolId, 
			ssae.SchoolYear, 
			ssae.EventDate,
			ssae.LastModifiedDate,
			d_ssae.CodeValue AS AttendanceEventCategoryDescriptor_CodeValue,					
			LTRIM(RTRIM(COALESCE(ssae.AttendanceEventReason,''))) AS AttendanceEventReason 
FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.StudentSchoolAttendanceEvent ssae
		INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Student s ON ssae.StudentUSI = s.StudentUSI
		INNER JOIN [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.Descriptor d_ssae ON ssae.AttendanceEventCategoryDescriptorId = d_ssae.DescriptorId

WHERE SchoolYear >= 2019
	AND ssae.EventDate = '2021-09-10'
	AND not EXISTS (SELECT 1
					FROM dbo.DimSchool ds
					WHERE  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),ssae.SchoolId)) = ds._sourceKey
						   AND ssae.EventDate >= ds.[ValidFrom]
											AND ssae.EventDate < ds.[ValidTo]) 

		SELECT * FROM dbo.DimSchool WHERE DistrictSchoolCode IN ('1440')
		SELECT * FROM [EDFISQL01].[v34_EdFi_BPS_Production_Ods].edfi.School WHERE SchoolId = '1440'
			
						
SELECT * 
FROM [dbo].FactStudentAttendanceByDay fsabd
		INNER JOIN dbo.DimTime dt ON fsabd.TimeKey = dt.TimeKey
WHERE dt.SchoolDate = '2021-09-10' AND fsabd.AttendanceEventCategoryKey <> 4

SELECT * FROM [dbo].[View_StudentAttendanceByDay] WHERE AttedanceDate = '2021-09-10'

SELECT * 
FROM Staging.StudentAttendanceByDay


			
			

UPDATE s 
SET s.StudentKey =	(SELECT TOP (1) ds.StudentKey
						FROM dbo.DimStudent ds
						WHERE s._sourceStudentKey = ds._sourceKey									
						AND s.[ModifiedDate] >= ds.[ValidFrom]
						AND s.[ModifiedDate] < ds.[ValidTo]
					ORDER BY ds.[ValidFrom] DESC)
			
FROM Staging.StudentAttendanceByDay s;
	
SELECT COUNT(*) FROM Staging.StudentAttendanceByDay WHERE StudentKey IS NULL 


UPDATE s 
SET s.TimeKey =       (SELECT TOP (1) dt.TimeKey
							   FROM dbo.DimTime dt
									INNER JOIN dbo.DimSchool ds ON dt.SchoolKey = ds.SchoolKey
							   WHERE s._sourceSchoolKey = ds._sourceKey
							     AND s._sourceTimeKey = dt.SchoolDate
							   ORDER BY dt.SchoolDate)
							
			
FROM Staging.StudentAttendanceByDay s;
	
SELECT COUNT(*) FROM Staging.StudentAttendanceByDay WHERE TimeKey IS NULL 


UPDATE s 
SET s.SchoolKey =    (SELECT TOP (1) ds.SchoolKey
							  FROM dbo.DimSchool ds
							  WHERE s._sourceSchoolKey = ds._sourceKey									
									AND s.[ModifiedDate] >= ds.[ValidFrom]
									AND s.[ModifiedDate] < ds.[ValidTo]
							  ORDER BY ds.[ValidFrom] DESC)
							
			
FROM Staging.StudentAttendanceByDay s;
	
SELECT * FROM Staging.StudentAttendanceByDay WHERE SchoolKey IS NULL 
SELECT * FROM dbo.DimSchool ds WHERE ds._sourceKey = 'Ed-Fi|8034'
UPDATE dbo.DimSchool SET ValidTo = '12/31/9999' WHERE IsCurrent = 1

DECLARE @lineageKey INT = 15

DROP INDEX IF EXISTS CSI_FactStudentAttendanceByDay ON dbo.FactStudentAttendanceByDay;

		--deleting changed records
		DELETE prod
		FROM [dbo].FactStudentAttendanceByDay AS prod
		WHERE EXISTS (SELECT 1 
		              FROM [Staging].StudentAttendanceByDay stage
					  WHERE prod._sourceKey = stage._sourceKey)
					  
	    
		
		INSERT INTO dbo.FactStudentAttendanceByDay
		(
		    _sourceKey,
		    StudentKey,
		    TimeKey,
		    SchoolKey,
		    AttendanceEventCategoryKey,
		    AttendanceEventReason,
		    LineageKey
		)
		SELECT DISTINCT 
		    _sourceKey,
		    StudentKey,
		    TimeKey,
		    SchoolKey,
		    AttendanceEventCategoryKey,
		    AttendanceEventReason,
			@LineageKey		
		FROM Staging.StudentAttendanceByDay

			
		--loading from legacy dw just once
		IF (NOT EXISTS(SELECT 1  
		               FROM dbo.FactStudentAttendanceByDay 
		               WHERE _sourceKey = 'LegacyDW'))
			  BEGIN
			     INSERT INTO EdFiDW.[dbo].[FactStudentAttendanceByDay]
					   (_sourceKey,
					    [StudentKey]
					   ,[TimeKey]
					   ,[SchoolKey]
					   ,[AttendanceEventCategoryKey]
					   ,[AttendanceEventReason]
					   ,[LineageKey])
				SELECT 
				      'LegacyDW' AS _sourceKey,
					  ds.StudentKey,
					  dt.TimeKey,	  
					  dschool.SchoolKey,      
					  daec.AttendanceEventCategoryKey,
					  'N/A' AS  AttendanceEventReason,
					  @LineageKey AS [LineageKey]
				--select top 100  a.*
				FROM [BPSGranary02].[BPSDW].[dbo].[Attendance] a	
					--joining DW tables
					INNER JOIN EdFiDW.dbo.DimStudent ds  ON CONCAT_WS('|', 'LegacyDW', Convert(NVARCHAR(MAX),a.[StudentNo]))   = ds._sourceKey
													   AND a.[Date] BETWEEN ds.ValidFrom AND ds.ValidTo
					INNER JOIN EdFiDW.dbo.DimSchool dschool ON CONCAT_WS('|', 'Ed-Fi', Convert(NVARCHAR(MAX),a.Sch))   = dschool._sourceKey -- all schools except one (inactive) are Ed-Fi
													   AND a.[Date] BETWEEN dschool.ValidFrom AND dschool.ValidTo
					INNER JOIN EdFiDW.dbo.DimTime dt ON a.[Date] = dt.SchoolDate
													and dt.SchoolKey is not null   
													and dschool.SchoolKey = dt.SchoolKey
					INNER JOIN EdFiDW.[dbo].DimAttendanceEventCategory daec ON CASE 
																					WHEN a.AttendanceCodeDesc IN ('Absent') THEN 'Unexcused Absence'
																					WHEN a.AttendanceCodeDesc IN ('Absent, Bus Strike','Bus / Transportation','Excused Absent','In School, Suspended','Suspended') THEN 'Excused Absence'
																					WHEN a.AttendanceCodeDesc IN ('Early Dismissal','Dismissed')  THEN 'Early departure'
																					WHEN a.AttendanceCodeDesc = 'No Contact'  THEN 'No Contact'
																					WHEN CHARINDEX('Tardy',a.AttendanceCodeDesc,1) > 0 THEN 'Tardy'
																					ELSE 'In Attendance' 	                                                                   
																				END = daec.AttendanceEventCategoryDescriptor_CodeValue
 
				WHERE  a.[Date] BETWEEN '2015-07-01' AND '2018-06-30'				
				   AND a.sch between '1000' and '4700'
			  END

		--re-creating the columnstore index
		CREATE COLUMNSTORE INDEX CSI_FactStudentAttendanceByDay
			  ON dbo.FactStudentAttendanceByDay
			  ([StudentKey]
			  ,[TimeKey]
			  ,[SchoolKey]
			  ,[AttendanceEventCategoryKey]
			  ,[AttendanceEventReason]
			  ,[LineageKey])

		--Deriving
		--dropping the columnstore index
		DROP INDEX IF EXISTS CSI_Derived_StudentAttendanceByDay ON Derived.StudentAttendanceByDay;

		--ByDay
		delete d_sabd
		FROM  [Derived].[StudentAttendanceByDay] d_sabd
		WHERE EXISTS(SELECT 1 
		             FROM Staging.StudentAttendanceByDay s_sabd
					 WHERE d_sabd.StudentKey = s_sabd.StudentKey
					    AND d_sabd.[TimeKey] = s_sabd.[TimeKey])

		INSERT INTO [Derived].[StudentAttendanceByDay]
					([StudentKey]
					,[TimeKey]
					,[SchoolKey]
					,AttendanceEventCategoryKey
					,[EarlyDeparture]
					,[ExcusedAbsence]
					,[UnexcusedAbsence]
					,[NoContact]
					,[InAttendance]
					,[Tardy])

		SELECT 
				StudentKey, 
				TimeKey, 
				SchoolKey,
				AttendanceEventCategoryKey,
				--pivoted from row values	  
				CASE WHEN [Early departure] IS NULL THEN 0 ELSE 1 END AS EarlyDeparture,
				CASE WHEN [Excused Absence] IS NULL THEN 0 ELSE 1 END AS [ExcusedAbsence],
				CASE WHEN [Unexcused Absence] IS NULL THEN 0 ELSE 1 END AS [UnexcusedAbsence],
				CASE WHEN [No Contact] IS NULL THEN 0 ELSE 1 END AS [NoContact],
				CASE WHEN [In Attendance] IS NULL THEN 0 ELSE 1 END AS [InAttendance],
				CASE WHEN [Tardy] IS NULL THEN 0 ELSE 1 END AS [Tardy]	     
	   
		FROM (
				SELECT fsabd.StudentKey,
						fsabd.TimeKey,
						fsabd.SchoolKey,
						fsabd.AttendanceEventCategoryKey,
						dact.AttendanceEventCategoryDescriptor_CodeValue AS AttendanceType	       	 			 			   
				FROM dbo.[FactStudentAttendanceByDay] fsabd 
				        INNER JOIN Staging.StudentAttendanceByDay s_sabd ON fsabd.StudentKey = s_sabd.StudentKey
					                                                    AND fsabd.[TimeKey] = s_sabd.[TimeKey]
						INNER JOIN dbo.DimStudent ds ON fsabd.StudentKey = ds.StudentKey
						INNER JOIN dbo.DimAttendanceEventCategory dact ON fsabd.AttendanceEventCategoryKey = dact.AttendanceEventCategoryKey		
				WHERE 1=1 
				--AND ds.StudentUniqueId = 341888
				--AND dt.SchoolDate = '2018-10-26'

		
			) AS SourceTable 
		PIVOT 
			(
				MAX(AttendanceType)
				FOR AttendanceType IN ([Early departure],
										[Excused Absence],
										[Unexcused Absence],
										[No Contact],
										[In Attendance],
										[Tardy]
								)
			) AS PivotTable;
			
		CREATE COLUMNSTORE INDEX CSI_Derived_StudentAttendanceByDay
			ON Derived.StudentAttendanceByDay
			([StudentKey]
			,[TimeKey]
			,[SchoolKey]
			,[EarlyDeparture]
			,[ExcusedAbsence]
			,[UnexcusedAbsence]
			,[NoContact]
			,[InAttendance]
			,[Tardy])
			
		--ADA
		
		DELETE d_sabd
		FROM  [Derived].[StudentAttendanceADA] d_sabd
		WHERE EXISTS(SELECT 1 
		             FROM Staging.StudentAttendanceByDay s_sabd
					      INNER JOIN dbo.DimTime dt ON s_sabd.TimeKey = dt.TimeKey
						  INNER JOIN dbo.DimStudent st ON s_sabd.StudentKey = st.StudentKey
					 WHERE d_sabd.StudentId = st.StudentUniqueId
					   AND d_sabd.[SchoolYear] = dt.SchoolYear)
					   
	    --handling students who change their names
		;WITH UniqueStudents AS 
		(
		  SELECT    DISTINCT
					v_sabd.StudentId, 
					v_sabd.FirstName, 
					v_sabd.LastName,
					v_sabd.AttedanceDate,
					ROW_NUMBER() OVER (PARTITION BY v_sabd.StudentId ORDER BY v_sabd.AttedanceDate DESC) AS RankId
		  FROM dbo.View_StudentAttendanceByDay v_sabd 
		  WHERE EXISTS(SELECT 1 
					   FROM Staging.StudentAttendanceByDay s_sabd
							  INNER JOIN dbo.DimTime dt ON s_sabd.TimeKey = dt.TimeKey
							  INNER JOIN dbo.DimStudent st ON s_sabd.StudentKey = st.StudentKey
					   WHERE v_sabd.StudentId = st.StudentUniqueId
						   AND v_sabd.[SchoolYear] = dt.SchoolYear) 
		  
		)
		INSERT INTO [Derived].[StudentAttendanceADA]([StudentId]																
													,[FirstName]
													,[LastName]
													,[DistrictSchoolCode]
													,[UmbrellaSchoolCode]
													,[SchoolName]
													,[SchoolYear]
													,[NumberOfDaysPresent]
													,[NumberOfDaysAbsent]
													,[NumberOfDaysAbsentUnexcused]
													,[NumberOfDaysMembership]
													,[ADA])

		SELECT     DISTINCT
					v_sabd.StudentId, 
					us.FirstName, 
					us.LastName, 
					v_sabd.[DistrictSchoolCode],
					v_sabd.[UmbrellaSchoolCode],	   
					v_sabd.SchoolName, 	   
					v_sabd.SchoolYear,	   
					COUNT(DISTINCT (CASE WHEN v_sabd.InAttendance =1 THEN v_sabd.AttedanceDate ELSE NULL END))   AS NumberOfDaysPresent,
					COUNT(DISTINCT (CASE WHEN v_sabd.InAttendance =0 THEN v_sabd.AttedanceDate ELSE NULL END))  AS NumberOfDaysAbsent,
					COUNT(DISTINCT (CASE WHEN v_sabd.[UnexcusedAbsence] =1 THEN v_sabd.AttedanceDate ELSE NULL END))    AS NumberOfDaysAbsentUnexcused,
					COUNT(DISTINCT v_sabd.AttedanceDate)   AS NumberOfDaysMembership,
					COUNT(DISTINCT (CASE WHEN v_sabd.InAttendance =1 THEN v_sabd.AttedanceDate ELSE NULL END)) / CONVERT(Float,COUNT(DISTINCT v_sabd.AttedanceDate)) * 100 AS ADA			
		FROM dbo.View_StudentAttendanceByDay v_sabd	
		     INNER JOIN UniqueStudents us ON v_sabd.StudentId = us.StudentId AND us.RankId = 1 --only the latest name		
		GROUP BY    v_sabd.StudentId, 					
					us.FirstName, 
					us.LastName, 
					v_sabd.[DistrictSchoolCode],
					v_sabd.[UmbrellaSchoolCode],	   
					v_sabd.SchoolName, 	   
					v_sabd.SchoolYear

					
		

        -- updating the EndTime to now and status to Success		
		UPDATE dbo.ETL_Lineage
			SET 
				EndTime = SYSDATETIME(),
				Status = 'S' -- success
		WHERE [LineageKey] = @LineageKey;
	
	
		-- Update the LoadDates table with the most current load date
		UPDATE [dbo].[ETL_IncrementalLoads]
		SET [LoadDate] = GETDATE()
		WHERE [TableName] = N'dbo.FactStudentAttendanceByDay';