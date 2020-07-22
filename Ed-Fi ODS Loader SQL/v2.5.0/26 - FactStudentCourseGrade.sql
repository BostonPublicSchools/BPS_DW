DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM LongitudinalPOC.[dbo].[Lineage] WHERE TableName= 'dbo.FactStudentCourseTranscript')
BEGIN
    INSERT INTO LongitudinalPOC.[dbo].[Lineage]
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
	 FROM LongitudinalPOC.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.FactStudentCourseTranscript'
END 

truncate table LongitudinalPOC.[dbo].FactStudentCourseTranscript

/*
CREATE NONCLUSTERED INDEX [IX_DimeTimne_School-SchoolKey_Including-TimeKey]
ON [dbo].[DimTime] ([SchoolDate],[SchoolKey])
INCLUDE ([TimeKey])
GO

CREATE NONCLUSTERED INDEX [IX_DimStudent_sourceKey_Including-StudentKey]
ON [dbo].[DimStudent] ([_sourceKey])
INCLUDE ([StudentKey])
GO

CREATE NONCLUSTERED INDEX [IX_DimSection_sourceKey_Including-SectionKey]
ON [dbo].[DimSection] ([_sourceKey])
INCLUDE ([SectionKey])
GO

CREATE NONCLUSTERED INDEX [IX_DimStaff_sourceKey_Including-StaffKey]
ON [dbo].[DimStaff] ([_sourceKey])
INCLUDE ([StaffKey])
GO

*/


INSERT INTO LongitudinalPOC.[dbo].[FactStudentCourseTranscript]
           ([StudentKey]
           ,[TimeKey]
           ,[CourseKey]
		   ,[SchoolKey]
           ,EarnedCredits
           ,PossibleCredits 
           ,FinalLetterGradeEarned
		   ,FinalNumericGradeEarned
           ,[LineageKey])
		    
SELECT DISTINCT 
      ds.StudentKey,
      dt.TimeKey,
	  dcourse.CourseKey,	  
	  dschool.SchoolKey,      
	  scg.CreditsEarned AS EarnedCredits,
	  scg.CreditsPossible AS PossibleCredits,
	  CASE WHEN scg.CreditsEarned = 0 THEN 'NC'
	  ELSE
		  CASE WHEN TRY_CAST(scg.FinalMark AS DECIMAL) IS NULL THEN scg.FinalMark 
			ELSE NULL 
	      END 
	  END AS FinalLetterGradeEarned,
	  CASE WHEN TRY_CAST(scg.FinalMark AS DECIMAL) IS NOT NULL THEN scg.FinalMark ELSE NULL END AS FinalNumericGradeEarned,
	  @lineageKey AS [LineageKey]
--select *  
FROM [RAEDatabase].[dbo].[StudentCourseGrade_aspenNewFormat] scg
    
	--joining DW tables
	INNER JOIN LongitudinalPOC.dbo.DimStudent ds  ON CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),scg.StudentNo))  = ds._sourceKey
	INNER JOIN LongitudinalPOC.dbo.DimSchool dschool ON CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),scg.SchoolID))   = dschool._sourceKey
	INNER JOIN LongitudinalPOC.dbo.DimCourse dcourse ON CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),scg.CourseNumber))  = dcourse._sourceKey
	
	INNER JOIN LongitudinalPOC.dbo.DimTime dt on dt.SchoolKey is not null   
									and dschool.SchoolKey = dt.SchoolKey
                                    and dt.SchoolTermDescriptor_CodeValue = 'Other' -- year
									AND RIGHT(RTRIM(scg.SchoolYear),4) = dt.SchoolYear

WHERE  dt.SchoolDate BETWEEN ds.ValidFrom  AND ds.ValidTo
   --AND dt.SchoolDate <= GETDATE()   
   AND scg.SchoolYear IN ('2015-2016','2016-2017', '2017-2018')	
      

   /*
  SELECT DISTINCT SchoolTermDescriptor_CodeValue FROM LongitudinalPOC.dbo.DimTime
  SELECT schyear, StudentNo, sch,SchName_23,Semester,CourseAcademicLevel,CourseNo,CourseTitle,Section,FinalMark,Subject,DESECoreCourse,CreditsEarned,CreditsPossible
  FROM [BPSDW].[dbo].[StudentCourseGrade]
  where ISNUMERIC(FinalMark) =1
  SELECT TOP 100 CourseTitle, CreditsEarned, CreditsPossible, FinalMark , *
  FROM [BPSDW].[dbo].[StudentCourseGrade]
  where schyear between '2015' and '2017'
  AND StudentNo= '307609'
  ORDER BY schyear, CourseTitle
  */
--order by s.StudentUSI;

--select * from LongitudinalPOC.[dbo].[FactStudentCourseTranscript]

--updatng the lineage table
UPDATE LongitudinalPOC.[dbo].[Lineage]
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

*/



