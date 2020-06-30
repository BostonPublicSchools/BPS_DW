DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM BPS_DW.[dbo].[Lineage] WHERE TableName= 'dbo.DimCourse')
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
	 FROM BPS_DW.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.DimCourse'
END 

		
INSERT INTO [BPS_DW].[dbo].[DimCourse]
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
       'Ed-Fi|' + Convert(NVARCHAR(MAX),c.CourseCode) AS [_sourceKey],
       c.CourseCode,
	   c.CourseTitle,
	   c.CourseDescription,
	   ISNULL(clct.CodeValue,'N/A') AS [CourseLevelCharacteristicTypeDescriptor_CodeValue],
	   ISNULL(clct.[Description],'N/A') AS [CourseLevelCharacteristicTypeDescriptor_Descriptor],

       ISNULL(ast.CodeValue,'N/A') AS [AcademicSubjectDescriptor_CodeValue],
	   ISNULL(ast.[Description],'N/A') AS [AcademicSubjectDescriptor_Descriptor],
	   ISNULL(c.HighSchoolCourseRequirement,0) AS [HighSchoolCourseRequirement_Indicator],

	   c.MinimumAvailableCredits,
	   c.MaximumAvailableCredits,
	   ISNULL(cgat.CodeValue,'N/A')  AS GPAApplicabilityType_CodeValue,
	   ISNULL(cgat.[Description],'N/A') AS GPAApplicabilityType_Description,

	   GETDATE() AS ValidFrom,
	   '12/31/9999' as ValidTo,
	    1 AS IsCurrent,
	   @lineageKey AS [LineageKey]
--select *
FROM EdFi_BPS_Staging_Ods.edfi.Course c
     LEFT JOIN EdFi_BPS_Staging_Ods.edfi.CourseLevelCharacteristic clc ON c.CourseCode = clc.CourseCode
	 LEFT JOIN EdFi_BPS_Staging_Ods.edfi.CourseLevelCharacteristicType clct ON clc.CourseLevelCharacteristicTypeId = clct.CourseLevelCharacteristicTypeId
	 LEFT JOIN EdFi_BPS_Staging_Ods.edfi.AcademicSubjectType ast ON c.AcademicSubjectDescriptorId = ast.AcademicSubjectTypeId
	 LEFT JOIN EdFi_BPS_Staging_Ods.edfi.CourseGPAApplicabilityType cgat ON c.CourseGPAApplicabilityTypeId = cgat.CourseGPAApplicabilityTypeId
WHERE EXISTS (SELECT 1 
              FROM  EdFi_BPS_Staging_Ods.edfi.CourseOffering co 
			  WHERE c.CourseCode = co.CourseCode
                AND co.SchoolYear IN (2019,2020))

--select * from BPS_DW.[dbo].[DimCourse]

--updatng the lineage table
UPDATE BPS_DW.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;






