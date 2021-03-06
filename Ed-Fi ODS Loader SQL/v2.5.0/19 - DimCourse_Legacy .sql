DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM EdFiDW.[dbo].[Lineage] WHERE TableName= 'dbo.DimCourse')
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
	('dbo.DimCourse', 
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
	 WHERE TableName= 'dbo.DimCourse'
END 

		
INSERT INTO [EdFiDW].[dbo].[DimCourse]
           ([_sourceKey]
           ,[CourseCode]
           ,[CourseTitle]
           ,[CourseDescription]
           ,[CourseLevelCharacteristicTypeDescriptor_CodeValue]
           ,[CourseLevelCharacteristicTypeDescriptor_Description]
           ,[AcademicSubjectDescriptor_CodeValue]
           ,[AcademicSubjectDescriptor_Description]
           ,[HighSchoolCourseRequirement_Indicator]
           ,[MinimumAvailableCredits]
           ,[MaximumAvailableCredits]
           ,[GPAApplicabilityType_CodeValue]
           ,[GPAApplicabilityType_Description]   
		   ,[SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue]
		   ,[SecondaryCourseLevelCharacteristicTypeDescriptor_Description]
		   ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey])


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
	   '07/01/2015' AS ValidFrom,
	   '12/31/9999' as ValidTo,
	    1 AS IsCurrent,
	   @lineageKey AS [LineageKey]
--select *
FROM [BPSGranary02].[RAEDatabase].[dbo].[CourseCatalog_aspen] c     
     INNER JOIN [BPSGranary02].[RAEDatabase].[dbo].[StudentCourseGrade_aspenNewFormat] s ON c.CourseNumber = s.CourseNumber
																						AND c.SchoolYear = s.SchoolYear
WHERE c.SchoolYear IN ('2017-2018','2016-2017','2015-2016')
--AND NOT EXISTS (SELECT 1 FROM [EdFiDW].[dbo].[DimCourse] dc WHERE c.CourseNumber = dc.coursecode)


--updatng the lineage table
UPDATE EdFiDW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;




