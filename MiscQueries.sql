

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