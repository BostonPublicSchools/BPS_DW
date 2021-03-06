DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM EdFiDW.[dbo].[Lineage] WHERE TableName= 'dbo.FactStudentCourseTranscript')
BEGIN
    INSERT INTO EdFiDW.[dbo].[Lineage]
	(
	 [TableName], 
	 [StartTime], 
	 [EndTime], 
	 [LoadType], 
	 [Status]
	)
	VALUES
	('dbo.FactStudentCourseTranscript', 
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
	 FROM EdFiDW.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.FactStudentCourseTranscript'
END 

DELETE from EdFiDW.[dbo].FactStudentCourseTranscript

;WITH SchoolTermsFirstDates AS 
(
  SELECT DISTINCT SchoolKey, 
                  SchoolTermDescriptor_CodeValue AS Term,
				  SchoolYear,
                  MIN(SchoolDate) OVER (PARTITION BY SchoolKey, SchoolTermDescriptor_CodeValue, SchoolYear) AS MinTermDate
  FROM EdFiDW.dbo.DimTime 
  WHERE SchoolKey IS NOT NULL

)

INSERT INTO EdFiDW.[dbo].[FactStudentCourseTranscript]
           ([StudentKey]
           ,[TimeKey]
           ,[CourseKey]
		   ,[SchoolKey]
           ,EarnedCredits
           ,PossibleCredits 
           ,FinalLetterGradeEarned
		   ,FinalNumericGradeEarned
           ,[LineageKey])
		    
SELECT distinct
      ds.StudentKey,
      dt.TimeKey,
	  dcourse.CourseKey,	  
	  dschool.SchoolKey,      
	  ct.EarnedCredits,
	  ct.AttemptedCredits,
	  ct.FinalLetterGradeEarned,
	  ct.FinalNumericGradeEarned,
	  @lineageKey AS [LineageKey]
--select *  
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseTranscript ct
	INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor td ON ct.TermDescriptorId = td.DescriptorId
	
	--joining DW tables
	INNER JOIN EdFiDW.dbo.DimStudent ds  ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ct.StudentUSI)   = ds._sourceKey
	INNER JOIN EdFiDW.dbo.DimSchool dschool ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ct.SchoolId)   = dschool._sourceKey
	INNER JOIN EdFiDW.dbo.DimCourse dcourse ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ct.CourseCode)   = dcourse._sourceKey
	
	INNER JOIN EdFiDW.dbo.DimTime dt on dt.SchoolKey is not null   
									and dschool.SchoolKey = dt.SchoolKey
                                    and td.CodeValue = dt.SchoolTermDescriptor_CodeValue
									AND ct.SchoolYear = dt.SchoolYear

WHERE  dt.SchoolDate BETWEEN ds.ValidFrom  AND ds.ValidTo
   AND dt.SchoolDate BETWEEN dschool.ValidFrom AND dschool.ValidTo
   AND dt.SchoolDate BETWEEN dcourse.ValidFrom AND dcourse.ValidTo
   AND dt.SchoolDate <= GETDATE()   
   --AND dt.SchoolTermDescriptor_CodeValue = 'Other' --year long only
   AND ct.SchoolYear IN (2019, 2020)
   AND EXISTS (SELECT 1
               FROM SchoolTermsFirstDates std 
			   WHERE dschool.SchoolKey = std.SchoolKey
			     AND dt.SchoolTermDescriptor_CodeValue = std.Term
				 AND dt.SchoolYear = std.SchoolYear
				 AND dt.SchoolDate = std.MinTermDate)

--order by s.StudentUSI;

--select * from EdFiDW.[dbo].[FactStudentCourseTranscript]

--updatng the lineage table
UPDATE EdFiDW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;

/*
SELECT
  	TABLE_NAME
FROM
  	INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME LIKE '%GPA%'

SELECT
  	t.TABLE_NAME
FROM
  	INFORMATION_SCHEMA.TABLES t
	INNER JOIN INFORMATION_SCHEMA.COLUMNS  c ON t.TABLE_NAME = c.TABLE_NAME 
WHERE c.COLUMN_NAME LIKE '%credit%'


SELECT * FROM dbo.DimSchool WHERE IsCurrent = 1
select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.School WHERE SchoolId = 2360
select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EducationOrganization WHERE EducationOrganizationId = 2360

select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseTranscript
select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentGradebookEntry

select DISTINCT *
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.CourseTranscript ct
	 INNER JOIN [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor td ON ct.TermDescriptorId = td.DescriptorId
*/

