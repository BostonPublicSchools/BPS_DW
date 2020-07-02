DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM BPS_DW.[dbo].[Lineage] WHERE TableName= 'dbo.FactStudentCourseTranscript')
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
	 FROM BPS_DW.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.FactStudentCourseTranscript'
END 

truncate table BPS_DW.[dbo].FactStudentCourseTranscript

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


INSERT INTO BPS_DW.[dbo].[FactStudentCourseTranscript]
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
	  ct.EarnedCredits,
	  ct.AttemptedCredits, -- under review
	  ct.FinalLetterGradeEarned,
	  ct.FinalNumericGradeEarned,
	  @lineageKey AS [LineageKey]
--select *  
FROM [EdFi_BPS_Staging_Ods].edfi.Student s    
    INNER JOIN [EdFi_BPS_Staging_Ods].edfi.CourseTranscript ct ON s.StudentUSI = ct.StudentUSI
	INNER JOIN [EdFi_BPS_Staging_Ods].edfi.Descriptor td ON ct.TermDescriptorId = td.DescriptorId
	
	--joining DW tables
	INNER JOIN BPS_DW.dbo.DimStudent ds  ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),s.StudentUSI)   = ds._sourceKey
	INNER JOIN BPS_DW.dbo.DimSchool dschool ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ct.SchoolId)   = dschool._sourceKey
	INNER JOIN BPS_DW.dbo.DimCourse dcourse ON 'Ed-Fi|' + Convert(NVARCHAR(MAX),ct.CourseCode)   = dcourse._sourceKey
	
	INNER JOIN BPS_DW.dbo.DimTime dt on dt.SchoolKey is not null   
									and dschool.SchoolKey = dt.SchoolKey
                                    and td.CodeValue = dt.SchoolTermDescriptor_CodeValue
									AND ct.SchoolYear = dt.SchoolYear

WHERE  dt.SchoolDate BETWEEN ds.ValidFrom  AND ds.ValidTo
   --AND dt.SchoolDate <= GETDATE()   
   AND ct.SchoolYear IN (2019, 2020)	

--order by s.StudentUSI;

--select * from BPS_DW.[dbo].[FactStudentCourseTranscript]

--updatng the lineage table
UPDATE BPS_DW.[dbo].[Lineage]
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
select * from [EdFi_BPS_Staging_Ods].edfi.School WHERE SchoolId = 2360
select * from [EdFi_BPS_Staging_Ods].edfi.EducationOrganization WHERE EducationOrganizationId = 2360

select * from [EdFi_BPS_Staging_Ods].edfi.CourseTranscript
select * from [EdFi_BPS_Staging_Ods].edfi.StudentGradebookEntry

*/



