DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM LongitudinalPOC.[dbo].[Lineage] WHERE TableName= 'dbo.DimCourse')
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
	 FROM LongitudinalPOC.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.DimCourse'
END 

		
INSERT INTO [LongitudinalPOC].[dbo].[DimCourse]
           ([_sourceKey]
           ,[CourseCode]
           ,[CourseTitle]
           ,[CourseDescription]
           ,[CourseLevelCharacteristicTypeDescriptor_CodeValue]
           ,[CourseLevelCharacteristicTypeDescriptor_Descriptor]
           ,[AcademicSubjectDescriptor_CodeValue]
           ,[AcademicSubjectDescriptor_Descriptor]
           ,[HighSchoolCourseRequirement_Indicator]
           ,[MinimumAvailableCredits]
           ,[MaximumAvailableCredits]
           ,[GPAApplicabilityType_CodeValue]
           ,[GPAApplicabilityType_Description]           
		   ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey])

    
SELECT DISTINCT 
        CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),c.CourseNumber)) AS [_sourceKey],
       c.CourseNumber,
	   c.TitleFull,
	   c.TitleFull,
	   COALESCE(c.LevelCode,'N/A') AS [CourseLevelCharacteristicTypeDescriptor_CodeValue],
	   COALESCE(c.LevelCode,'N/A') AS [CourseLevelCharacteristicTypeDescriptor_Descriptor],

       COALESCE(c.Department,'N/A') AS [AcademicSubjectDescriptor_CodeValue],
	   COALESCE(c.Department,'N/A') AS [AcademicSubjectDescriptor_Descriptor],
	   0 AS [HighSchoolCourseRequirement_Indicator],

	   NULL AS MinimumAvailableCredits,
	   NULL AS MaximumAvailableCredits,
	   CASE WHEN COALESCE(c.InGPA,'N') = 'Y' THEN 'Applicable' ELSE 'Not Applicable' END  AS GPAApplicabilityType_CodeValue,
	   CASE WHEN COALESCE(c.InGPA,'N') = 'Y' THEN 'Applicable' ELSE 'Not Applicable' END AS GPAApplicabilityType_Description,
	   COALESCE(c.MassCore,'N/A') AS [SecondaryCourseLevelCharacteristicTypeDescriptor_CodeValue],
	   COALESCE(c.MassCore,'N/A') AS [SecondaryCourseLevelCharacteristicTypeDescriptor_Description],
	   GETDATE() AS ValidFrom,
	   '12/31/9999' as ValidTo,
	    1 AS IsCurrent
	   --@lineageKey AS [LineageKey]
--select *
FROM [RAEDatabase].[dbo].[CourseCatalog_aspen] c     
WHERE c.SchoolYear IN ('2017-2018','2016-2017','2015-2016')


--SELECT SchoolYear, CourseNumber, TitleFull,LevelCode, InGPA,Department,MassCore  FROM [RAEDatabase].[dbo].[CourseCatalog_aspen]  
--SELECT DISTINCT LevelCode, MassCore FROM [RAEDatabase].[dbo].[CourseCatalog_aspen]
--select distinct GPAApplicabilityType_CodeValue from LongitudinalPOC.[dbo].[DimCourse]
--select distinct [CourseLevelCharacteristicTypeDescriptor_CodeValue] from LongitudinalPOC.[dbo].[DimCourse]

--updatng the lineage table
UPDATE LongitudinalPOC.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;


SELECT * FROM 





